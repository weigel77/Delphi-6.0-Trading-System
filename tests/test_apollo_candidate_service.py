from __future__ import annotations

import unittest

from config import AppConfig
from services.apollo_candidate_service import ApolloCandidateService


class ApolloCandidateServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ApolloCandidateService()
        self.option_chain = {
            "success": True,
            "expiration_date": "2026-04-06",
            "underlying_price": 6600.0,
            "puts": [
                self._contract(6600, 29.5, 30.5, -0.50, oi=1000, volume=500),
                self._contract(6520, 1.4, 1.6, -0.18, oi=480, volume=60),
                self._contract(6515, 0.4, 0.5, -0.12, oi=430, volume=50),
                self._contract(6510, 1.9, 2.1, -0.16, oi=500, volume=80),
                self._contract(6505, 0.7, 0.8, -0.12, oi=450, volume=60),
                self._contract(6500, 2.2, 2.4, -0.15, oi=520, volume=90),
                self._contract(6495, 0.8, 0.9, -0.11, oi=470, volume=70),
                self._contract(6480, 1.8, 2.0, -0.12, oi=550, volume=85),
                self._contract(6475, 0.6, 0.7, -0.09, oi=430, volume=55),
                self._contract(6470, 1.6, 1.8, -0.10, oi=560, volume=75),
                self._contract(6465, 0.5, 0.6, -0.08, oi=420, volume=50),
                self._contract(6460, 0.2, 0.3, -0.06, oi=390, volume=45),
                self._contract(6455, 1.4, 1.5, -0.07, oi=410, volume=48),
                self._contract(6450, 0.4, 0.5, -0.05, oi=405, volume=44),
            ],
            "calls": [
                self._contract(6600, 29.5, 30.5, 0.50, oi=1000, volume=500, put_call="CALL"),
            ],
        }

    def test_returns_exactly_three_mode_outputs(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        self.assertEqual(result["count_label"], "3 Modes")
        self.assertEqual(len(result["candidates"]), 3)
        self.assertEqual(
            [item["mode_label"] for item in result["candidates"]],
            [
                "Standard (Apollo Core)",
                "Aggressive (Yield Mode)",
                "Fortress (Max Safety)",
            ],
        )

    def test_modes_choose_meaningfully_different_candidates(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        modes = {item["mode_key"]: item for item in result["candidates"]}
        self.assertEqual(modes["standard"]["short_strike"], 6480.0)
        self.assertEqual(modes["aggressive"]["short_strike"], 6480.0)
        self.assertEqual(modes["fortress"]["short_strike"], 6480.0)
        self.assertGreater(modes["aggressive"]["premium_received_dollars"], modes["standard"]["premium_received_dollars"])
        self.assertLessEqual(modes["fortress"]["short_strike"], modes["standard"]["short_strike"])

    def test_credit_efficiency_is_populated_for_all_profiles(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        for item in result["candidates"]:
            self.assertIsNotNone(item["credit_efficiency_pct"])
            self.assertGreater(item["total_premium"], 0)
            self.assertGreater(item["max_loss"], 0)

    def test_standard_mode_uses_em_baseline_and_standard_size_logic(self) -> None:
        reduced_chain = {
            **self.option_chain,
            "puts": [item for item in self.option_chain["puts"] if item["strike"] >= 6475],
            "calls": [
                self._contract(6600, 35.5, 36.5, 0.50, oi=1000, volume=500, put_call="CALL"),
            ],
        }
        result = self.service.build_trade_candidates(
            option_chain=reduced_chain,
            structure={"grade": "Poor", "trend_classification": "Bearish", "damage_classification": "High"},
            macro={"grade": "Minor"},
        )

        standard = next(item for item in result["candidates"] if item["mode_key"] == "standard")
        self.assertEqual(standard["required_distance_rule_used"], "1.80x EM")
        self.assertEqual(standard["pass_type"], "standard_strict")
        self.assertFalse(standard["fallback_used"])
        self.assertEqual(standard["recommended_contract_size"], 5)
        self.assertIn("reduces size to 5", standard["recommended_contract_size_reason"])

    def test_aggressive_mode_uses_spx_percent_rule_and_can_scale_to_ten(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Neutral", "trend_classification": "Balanced", "damage_classification": "Moderate"},
            macro={"grade": "None"},
        )

        aggressive = next(item for item in result["candidates"] if item["mode_key"] == "aggressive")
        self.assertEqual(aggressive["required_distance_rule_used"], "1.50x EM")
        self.assertEqual(aggressive["pass_type"], "aggressive_strict")
        self.assertFalse(aggressive["fallback_used"])
        self.assertEqual(aggressive["recommended_contract_size"], 10)
        self.assertIn("scales to 10 contracts", aggressive["recommended_contract_size_reason"])

    def test_fortress_mode_anchors_above_standard_em_and_uses_baseline_contracts(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Neutral", "trend_classification": "Balanced", "damage_classification": "Moderate"},
            macro={"grade": "None"},
        )

        standard = next(item for item in result["candidates"] if item["mode_key"] == "standard")
        fortress = next(item for item in result["candidates"] if item["mode_key"] == "fortress")
        self.assertEqual(fortress["pass_type"], "fortress")
        self.assertEqual(fortress["pass_type_label"], "Fortress")
        self.assertFalse(fortress["fallback_used"])
        self.assertEqual(fortress["selection_variant"], "base")
        self.assertEqual(fortress["recommended_contract_size"], 10)
        self.assertGreaterEqual(fortress["target_em_multiple"], standard["em_multiple"])
        self.assertLessEqual(fortress["short_strike"], standard["short_strike"])
        self.assertEqual(fortress["base_candidate"]["contracts"], 10)
        self.assertIsNone(fortress["optimized_candidate"])

    def test_fortress_em_applies_poor_structure_markup_relative_to_standard(self) -> None:
        reduced_chain = {
            **self.option_chain,
            "puts": [
                self._contract(6600, 29.5, 30.5, -0.50, oi=1000, volume=500),
                self._contract(6465, 1.0, 1.1, -0.08, oi=420, volume=50),
                self._contract(6460, 0.2, 0.3, -0.06, oi=390, volume=45),
            ],
            "calls": [
                self._contract(6600, 35.5, 36.5, 0.50, oi=1000, volume=500, put_call="CALL"),
            ],
        }

        result = self.service.build_trade_candidates(
            option_chain=reduced_chain,
            structure={"grade": "Poor", "trend_classification": "Bearish", "damage_classification": "High"},
            macro={"grade": "Minor"},
        )

        fortress = next(item for item in result["candidates"] if item["mode_key"] == "fortress")
        self.assertEqual(fortress["pass_type"], "fortress")
        self.assertEqual(fortress["pass_type_label"], "Fortress")
        self.assertFalse(fortress["fallback_used"])
        self.assertEqual(fortress["fallback_rule_name"], "")
        self.assertTrue(fortress["available"])
        self.assertAlmostEqual(fortress["target_em_multiple"], 2.23, places=2)
        self.assertEqual(fortress["selection_variant"], "base")
        self.assertFalse(fortress["baseline_meets_fortress_em"])

    def test_fortress_always_produces_a_candidate_even_when_no_optimized_trade_qualifies(self) -> None:
        reduced_chain = {
            **self.option_chain,
            "puts": [
                self._contract(6600, 29.5, 30.5, -0.50, oi=1000, volume=500),
                self._contract(6465, 0.5, 0.6, -0.08, oi=420, volume=50),
                self._contract(6460, 0.2, 0.3, -0.06, oi=390, volume=45),
            ],
            "calls": [
                self._contract(6600, 35.5, 36.5, 0.50, oi=1000, volume=500, put_call="CALL"),
            ],
        }

        result = self.service.build_trade_candidates(
            option_chain=reduced_chain,
            structure={"grade": "Poor", "trend_classification": "Bearish", "damage_classification": "High"},
            macro={"grade": "Minor"},
        )

        fortress = next(item for item in result["candidates"] if item["mode_key"] == "fortress")
        self.assertTrue(fortress["available"])
        self.assertEqual(fortress["pass_type"], "fortress")
        self.assertEqual(fortress["no_trade_message"], "")
        self.assertEqual(fortress["selection_variant"], "base")
        self.assertEqual(fortress["recommended_contract_size"], 10)
        self.assertEqual(fortress["fallback_rule_name"], "")

    def test_all_apollo_profiles_share_the_same_efficiency_threshold(self) -> None:
        self.assertEqual(self.service.AGGRESSIVE_MIN_RISK_EFFICIENCY, ApolloCandidateService.SHARED_MIN_RISK_EFFICIENCY)
        self.assertEqual(self.service.STANDARD_MIN_RISK_EFFICIENCY, ApolloCandidateService.SHARED_MIN_RISK_EFFICIENCY)
        fortress_mode = next(
            item
            for item in self.service._build_mode_configs(6600.0, 60.0, "Good", "None")
            if item["key"] == "fortress"
        )
        self.assertIsNone(fortress_mode["strict_policy"]["min_risk_efficiency"])

    def test_fortress_reports_risk_efficiency_audit_fields(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        fortress = next(item for item in result["candidates"] if item["mode_key"] == "fortress")
        self.assertAlmostEqual(fortress["premium_per_contract"], fortress["credit"] * 100, places=2)
        self.assertAlmostEqual(
            fortress["total_premium"],
            fortress["premium_per_contract"] * fortress["recommended_contract_size"],
            places=2,
        )
        self.assertAlmostEqual(
            fortress["max_theoretical_risk"],
            fortress["max_loss"],
            places=2,
        )
        self.assertAlmostEqual(
            fortress["risk_efficiency"],
            fortress["total_premium"] / fortress["max_theoretical_risk"],
            places=4,
        )
        self.assertAlmostEqual(fortress["credit_efficiency_pct"], fortress["risk_efficiency"] * 100, places=2)
        self.assertAlmostEqual(fortress["target_em"], 2.2, places=2)

    def test_candidate_generation_uses_final_adjusted_structure_not_base_structure(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={
                "grade": "Neutral",
                "final_grade": "Neutral",
                "base_grade": "Poor",
                "rsi_modifier_applied": True,
                "rsi_modifier_label": "Oversold Boost",
            },
            macro={"grade": "None"},
        )

        aggressive = next(item for item in result["candidates"] if item["mode_key"] == "aggressive")

        self.assertEqual(aggressive["recommended_contract_size"], 10)
        self.assertEqual(result["diagnostics"]["structure_grade_used"], "Neutral")
        self.assertEqual(result["diagnostics"]["base_structure_grade"], "Poor")
        self.assertEqual(result["diagnostics"]["rsi_modifier_label"], "Oversold Boost")

    def test_candidate_dollar_values_are_total_position_values(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        standard = next(item for item in result["candidates"] if item["mode_key"] == "standard")
        self.assertAlmostEqual(standard["credit"], 1.65, places=2)
        self.assertAlmostEqual(standard["premium_received_dollars"], 1320.0, places=2)
        self.assertAlmostEqual(standard["max_loss"], 14680.0, places=2)

    def test_risk_cap_resizes_trade_when_theoretical_loss_is_too_large(self) -> None:
        service = ApolloCandidateService(AppConfig(apollo_account_value=15000.0))
        result = service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        standard = next(item for item in result["candidates"] if item["mode_key"] == "standard")
        self.assertTrue(standard["risk_cap_adjusted"])
        self.assertEqual(standard["original_contract_size"], 8)
        self.assertEqual(standard["adjusted_contract_size"], 6)
        self.assertLessEqual(standard["max_loss"], 2250.0)
        self.assertEqual(standard["risk_cap_status"], "Adjusted for risk cap")

    def test_trade_stays_available_at_one_contract_when_minimum_size_exceeds_cap(self) -> None:
        service = ApolloCandidateService(AppConfig(apollo_account_value=1000.0))
        result = service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        standard = next(item for item in result["candidates"] if item["mode_key"] == "standard")
        aggressive = next(item for item in result["candidates"] if item["mode_key"] == "aggressive")
        fortress = next(item for item in result["candidates"] if item["mode_key"] == "fortress")

        self.assertTrue(standard["available"])
        self.assertEqual(standard["adjusted_contract_size"], 1)
        self.assertEqual(standard["risk_cap_status"], "Minimum size exceeds configured risk cap")
        self.assertIn("Reduced to 1 contract", standard["recommended_contract_size_reason"])
        self.assertTrue(aggressive["available"])
        self.assertEqual(aggressive["adjusted_contract_size"], 1)
        self.assertEqual(aggressive["risk_cap_status"], "Minimum size exceeds configured risk cap")
        self.assertTrue(fortress["available"])
        self.assertEqual(fortress["risk_cap_status"], "Within $15,000 max loss cap")
        self.assertEqual(fortress["adjusted_contract_size"], fortress["recommended_contract_size"])
        self.assertEqual(fortress["pricing_basis"], "Short = Bid, Long = Ask")

    def test_fortress_prefers_higher_total_premium_when_credit_efficiency_is_equal(self) -> None:
        lower_size = {"credit_efficiency_pct": 10.0, "total_premium": 1000.0, "max_loss": 9000.0}
        higher_size = {"credit_efficiency_pct": 10.0, "total_premium": 1500.0, "max_loss": 13500.0}
        self.assertGreater(
            self.service._fortress_selection_priority_key(higher_size),
            self.service._fortress_selection_priority_key(lower_size),
        )

    def test_exit_ladder_is_attached_to_all_mode_outputs(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        for item in result["candidates"]:
            self.assertEqual(len(item["exit_plan"]), 4)
            self.assertIn("within 30 points", item["exit_plan"][0])
            self.assertIn("two consecutive 5-minute candles", item["exit_plan"][2])
            self.assertTrue(item["exit_plan_applied"])

    def test_realistic_max_loss_is_present_and_uses_sixty_percent_of_max_loss(self) -> None:
        result = self.service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        for item in result["candidates"]:
            self.assertIn("realistic_max_loss", item)
            self.assertIsNotNone(item["realistic_max_loss"])
            self.assertAlmostEqual(item["realistic_max_loss"], item["max_loss"] * 0.60, places=2)

    def test_invalid_candidate_still_reports_realistic_max_loss_at_sixty_percent(self) -> None:
        service = ApolloCandidateService(AppConfig(apollo_account_value=1000.0))
        result = service.build_trade_candidates(
            option_chain=self.option_chain,
            structure={"grade": "Good", "trend_classification": "Bullish", "damage_classification": "Low"},
            macro={"grade": "None"},
        )

        fortress = next(item for item in result["candidates"] if item["mode_key"] == "fortress")
        self.assertTrue(fortress["available"])
        self.assertAlmostEqual(fortress["realistic_max_loss"], fortress["max_loss"] * 0.60, places=2)

    @staticmethod
    def _contract(
        strike: float,
        bid: float,
        ask: float,
        delta: float,
        *,
        oi: int,
        volume: int,
        put_call: str = "PUT",
    ) -> dict:
        return {
            "strike": strike,
            "bid": bid,
            "ask": ask,
            "last": (bid + ask) / 2,
            "mark": (bid + ask) / 2,
            "delta": delta,
            "open_interest": oi,
            "total_volume": volume,
            "put_call": put_call,
            "symbol": f"SPX-{put_call}-{strike}",
        }


if __name__ == "__main__":
    unittest.main()
