from __future__ import annotations

import unittest

from services.em_policy_engine import build_em_policy_payload


class EmPolicyEngineTests(unittest.TestCase):
    def test_policy_payload_groups_qualified_and_excluded_trades(self) -> None:
        payload = build_em_policy_payload(
            [
                {
                    "system": "Apollo",
                    "status": "Closed",
                    "result": "Win",
                    "actual_em_multiple": 1.72,
                    "expected_move_used": 25.0,
                    "expected_move_source": "same_day_atm_straddle",
                    "em_multiple_floor": 1.8,
                    "percent_floor": 1.2,
                    "boundary_rule_used": "max(1.2% SPX, 1.8x expected move)",
                    "actual_distance_to_short": 43.0,
                    "fallback_used": "no",
                    "fallback_rule_name": "",
                    "structure_grade": "Good",
                    "macro_grade": "None",
                    "vix_entry": 20.0,
                    "realized_pnl": 180.0,
                },
                {
                    "system": "Apollo",
                    "status": "Closed",
                    "result": "Loss",
                    "actual_em_multiple": 1.95,
                    "expected_move_used": 20.0,
                    "expected_move_source": "same_day_atm_straddle",
                    "em_multiple_floor": 1.6,
                    "percent_floor": 1.2,
                    "boundary_rule_used": "max(1.2% SPX, 1.6x expected move)",
                    "actual_distance_to_short": 39.0,
                    "fallback_used": "yes",
                    "fallback_rule_name": "standard_1.6x_em_fallback",
                    "structure_grade": "Neutral",
                    "macro_grade": "Minor",
                    "vix_entry": 21.0,
                    "realized_pnl": -90.0,
                },
                {
                    "system": "Kairos",
                    "status": "Closed",
                    "result": "Black Swan",
                    "actual_em_multiple": 2.12,
                    "expected_move_used": 26.5,
                    "expected_move_source": "same_day_atm_straddle",
                    "em_multiple_floor": 2.0,
                    "percent_floor": 1.0,
                    "boundary_rule_used": "max(1.0% SPX, 2.0x expected move)",
                    "actual_distance_to_short": 56.18,
                    "fallback_used": "no",
                    "fallback_rule_name": "",
                    "structure_grade": "Poor",
                    "macro_grade": "Major",
                    "vix_entry": 24.0,
                    "realized_pnl": -350.0,
                },
                {
                    "system": "Kairos",
                    "status": "Open",
                    "result": "",
                    "actual_em_multiple": None,
                    "expected_move_used": None,
                },
            ]
        )

        self.assertEqual(payload["qualified_trade_count"], 3)
        self.assertEqual(payload["excluded_trade_count"], 1)
        self.assertEqual(payload["excluded_reasons"]["open trade"], 1)
        self.assertEqual(payload["apollo"]["qualified_trade_count"], 2)
        self.assertEqual(payload["kairos"]["qualified_trade_count"], 1)
        self.assertEqual(payload["apollo"]["vix_guidance"]["18-22"]["sample_size"], 2)
        self.assertEqual(payload["apollo"]["fallback_effect"]["fallback_trade_count"], 1)
        self.assertEqual(payload["kairos"]["overall_recommended_em_range"], "2.0-2.2")
        self.assertEqual(payload["kairos"]["overall_confidence"], "low")


if __name__ == "__main__":
    unittest.main()