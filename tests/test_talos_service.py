import tempfile
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from config import AppConfig
from services.talos_service import TalosService
from services.trade_store import TradeStore


class _StubMarketDataService:
    def get_latest_snapshot(self, ticker, query_type="latest"):
        return {"Latest Value": 5300.0, "As Of": "Now"}


class _StubOptionsChainService:
    pass


class _StubOpenTradeManager:
    def __init__(self, trade_store: TradeStore) -> None:
        self.trade_store = trade_store
        self.current_pl_by_trade_id: dict[int, float] = {}
        self.record_overrides_by_trade_id: dict[int, dict] = {}

    def evaluate_open_trades(self, send_alerts: bool = False):
        records = []
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status not in {"open", "reduced"}:
                continue
            trade_id = int(trade.get("id") or 0)
            records.append(
                {
                    "trade_id": trade_id,
                    "trade_number": int(trade.get("trade_number") or 0),
                    "system_name": trade.get("system_name") or "Apollo",
                    "trade_mode": "talos",
                    "status": "Healthy",
                    "status_key": "healthy",
                    "action_recommendation": "Watch",
                    "contracts_to_close": 0,
                    "contracts": int(trade.get("remaining_contracts") or trade.get("contracts") or 1),
                    "current_spread_mark": None,
                    "current_pl": self.current_pl_by_trade_id.get(trade_id, 0.0),
                    "unrealized_pnl_display": "$0.00",
                    "percent_credit_captured": 0.0,
                    "talos_candidate_score": 72.0,
                    "expiration": trade.get("expiration_date") or trade.get("expiration") or "",
                    "profile_label": trade.get("candidate_profile") or "Standard",
                    "notes_entry": trade.get("notes_entry") or "",
                    "exit_plan_summary": "Next: watch the short strike.",
                    "exit_plan_gates": [],
                }
            )
            if trade_id in self.record_overrides_by_trade_id:
                records[-1].update(self.record_overrides_by_trade_id[trade_id])
        return {"records": records, "header_market_snapshots": {}}


class TalosServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "talos.db"
        self.state_path = Path(self.temp_dir.name) / "talos_state.json"
        self.trade_store = TradeStore(self.database_path)
        self.trade_store.initialize()
        self.open_trade_manager = _StubOpenTradeManager(self.trade_store)
        self.chicago = ZoneInfo("America/Chicago")
        self.now = datetime(2026, 4, 20, 10, 0, tzinfo=self.chicago)
        self.service = TalosService(
            trade_store=self.trade_store,
            market_data_service=_StubMarketDataService(),
            options_chain_service=_StubOptionsChainService(),
            open_trade_manager=self.open_trade_manager,
            config=AppConfig(),
            state_path=self.state_path,
            now_provider=lambda: self.now,
        )
        self.service.initialize()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _trade_payload(
        self,
        system_name: str,
        profile: str = "Standard",
        *,
        short_strike: float = 5250.0,
        long_strike: float = 5240.0,
        contracts: int = 1,
        total_premium: float = 180.0,
        max_theoretical_risk: float = 820.0,
    ) -> dict:
        stamp = self.now.isoformat(timespec="minutes")
        return {
            "trade_mode": "talos",
            "system_name": system_name,
            "journal_name": "Apollo Main" if system_name == "Apollo" else "Horme",
            "candidate_profile": profile,
            "system_version": "7.0",
            "status": "open",
            "trade_date": stamp.split("T", 1)[0],
            "entry_datetime": stamp,
            "expiration_date": stamp.split("T", 1)[0],
            "underlying_symbol": "SPX",
            "spx_at_entry": 5300.0,
            "vix_at_entry": 18.5,
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": 35.0,
            "expected_move_used": 35.0,
            "option_type": "Put Credit Spread",
            "short_strike": short_strike,
            "long_strike": long_strike,
            "spread_width": 10.0,
            "contracts": contracts,
            "actual_entry_credit": 1.8,
            "net_credit_per_contract": 1.8,
            "distance_to_short": 50.0,
            "actual_distance_to_short": 50.0,
            "actual_em_multiple": 1.4,
            "short_delta": 0.11,
            "total_premium": total_premium,
            "max_theoretical_risk": max_theoretical_risk,
            "notes_entry": f"{system_name} test entry",
        }

    def _candidate(self, system_name: str, score: float) -> dict:
        return {
            "candidate_score": score,
            "decision_note": f"{system_name} weighted score {score:.1f}.",
            "selection_basis": "Deterministic Talos weighted score.",
            "score_breakdown": {"candidate_score": score, "standard_preference_component": 10.0 if system_name == "Apollo" else 0.0},
            "score_breakdown_display": f"Weighted score {score:.1f}.",
            "pricing_basis_label": "Conservative executable entry credit",
            "pricing_math_display": "short bid 3.20 - long ask 0.40 = 2.80",
            "entry_gate_label": f"Talos accepted {system_name}.",
            "trade_payload": self._trade_payload(system_name),
        }

    def _apollo_candidate(
        self,
        score: float,
        *,
        profile: str = "Standard",
        short_strike: float = 5250.0,
        long_strike: float = 5240.0,
        contracts: int = 1,
        total_premium: float = 180.0,
        max_theoretical_risk: float = 820.0,
    ) -> dict:
        candidate = self._candidate("Apollo", score)
        candidate["trade_payload"] = self._trade_payload("Apollo", profile=profile)
        candidate["trade_payload"]["short_strike"] = short_strike
        candidate["trade_payload"]["long_strike"] = long_strike
        candidate["trade_payload"]["candidate_profile"] = profile
        candidate["trade_payload"]["contracts"] = contracts
        candidate["trade_payload"]["total_premium"] = total_premium
        candidate["trade_payload"]["max_theoretical_risk"] = max_theoretical_risk
        candidate["trade_payload"]["premium_per_contract"] = round(total_premium / max(contracts, 1), 2)
        candidate["trade_payload"]["actual_entry_credit"] = round((total_premium / max(contracts, 1)) / 100.0, 4)
        candidate["trade_payload"]["net_credit_per_contract"] = round((total_premium / max(contracts, 1)) / 100.0, 4)
        candidate["profile_label"] = profile
        candidate["signature"] = self.service._apollo_payload_signature(candidate["trade_payload"])
        candidate["score_breakdown"] = {
            "candidate_score": score,
            "standard_preference_component": 10.0 if profile == "Standard" else 3.0 if profile == "Fortress" else 1.0,
        }
        return candidate

    def _apollo_plan(self, candidates: list[dict], *, account_balance: float = 135000.0, open_records: list[dict] | None = None) -> dict:
        return self.service._select_apollo_portfolio(candidates, account_balance=account_balance, open_records=open_records or [])

    def test_run_cycle_allows_three_apollo_and_one_kairos_total(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._build_apollo_portfolio_plan = lambda account_balance, open_records, learning_state=None: self._apollo_plan(
            [
                self._apollo_candidate(78.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=220.0),
                self._apollo_candidate(76.0, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=210.0),
                self._apollo_candidate(75.0, profile="Aggressive", short_strike=5220.0, long_strike=5210.0, total_premium=205.0),
            ],
            account_balance=account_balance,
            open_records=open_records,
        )
        self.service._build_kairos_candidate = lambda account_balance: self._candidate("Kairos", 72.0)

        self.service.run_cycle(trigger_reason="manual")
        self.service.run_cycle(trigger_reason="manual")
        self.service.run_cycle(trigger_reason="manual")

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        counts = Counter(str(trade.get("system_name") or "") for trade in open_trades)
        self.assertEqual(len(open_trades), 4)
        self.assertEqual(counts["Apollo"], 3)
        self.assertEqual(counts["Kairos"], 1)

    def test_run_cycle_rearms_next_scan_and_reenters_after_capture_close(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._monitor_running = True
        closed_trade_id = self.trade_store.create_trade(self._trade_payload("Apollo", total_premium=180.0))
        self.open_trade_manager.record_overrides_by_trade_id[closed_trade_id] = {
            "current_spread_mark": 0.33,
            "current_total_close_cost": 33.0,
            "percent_credit_captured": 81.67,
            "percent_credit_captured_display": "81.7%",
            "total_premium": 180.0,
        }
        self.service._build_apollo_portfolio_plan = lambda account_balance, open_records, learning_state=None: self._apollo_plan(
            [
                self._apollo_candidate(
                    79.0,
                    profile="Fortress",
                    short_strike=5235.0,
                    long_strike=5225.0,
                    total_premium=220.0,
                )
            ],
            account_balance=account_balance,
            open_records=open_records,
        )
        self.service._build_kairos_candidate = lambda account_balance: None

        self.service.run_cycle(trigger_reason="manual")

        state = self.service._load_state()
        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        closed_trade = self.trade_store.get_trade(closed_trade_id)
        self.assertEqual(str(closed_trade.get("derived_status_raw") or closed_trade.get("status") or "").strip().lower(), "closed")
        self.assertEqual(len(open_trades), 1)
        self.assertEqual(state["last_cycle_status"], "completed")
        self.assertGreater(state["next_scan_at"], state["last_cycle_at"])
        self.assertTrue(any(item.get("category") == "runtime" and "resumed scanning after the close" in item.get("detail", "") for item in state["activity_log"]))

    def test_run_cycle_logs_post_close_follow_through_reason_when_no_new_entry_qualifies(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._monitor_running = True
        closed_trade_id = self.trade_store.create_trade(self._trade_payload("Apollo", total_premium=180.0))
        self.open_trade_manager.record_overrides_by_trade_id[closed_trade_id] = {
            "current_spread_mark": 0.33,
            "current_total_close_cost": 33.0,
            "percent_credit_captured": 81.67,
            "percent_credit_captured_display": "81.7%",
            "total_premium": 180.0,
        }
        self.service._build_apollo_portfolio_plan = lambda account_balance, open_records, learning_state=None: self._apollo_plan([], account_balance=account_balance, open_records=open_records)
        self.service._build_kairos_candidate = lambda account_balance: None

        self.service.run_cycle(trigger_reason="manual")

        state = self.service._load_state()
        self.assertGreater(state["next_scan_at"], state["last_cycle_at"])
        self.assertTrue(any(item.get("category") == "runtime" and "scheduled the next scan" in item.get("detail", "") for item in state["activity_log"]))
        self.assertTrue(any(item.get("category") == "runtime" and "Kairos: No qualified Kairos Talos candidate was available." in item.get("detail", "") for item in state["activity_log"]))

    def test_balance_components_keep_settled_anchor_separate_from_unrealized(self) -> None:
        self.service._save_state(self.service._default_state(starting_balance=135000.0))
        original_list_trades = self.trade_store.list_trades
        self.trade_store.list_trades = lambda trade_mode: ([{"gross_pnl": 200.0, "derived_status_raw": "closed"}] if trade_mode == "talos" else original_list_trades(trade_mode))

        balances = self.service._compute_balance_components([{"current_pl": 525.0}])

        self.assertEqual(balances["settled_balance"], 135200.0)
        self.assertEqual(balances["equity_balance"], 135200.0)
        self.assertEqual(balances["current_account_value"], 135725.0)
        self.assertEqual(balances["unrealized_pnl"], 525.0)

    def test_reset_archives_runtime_and_applies_new_starting_balance(self) -> None:
        self.trade_store.create_trade(self._trade_payload("Apollo"))
        state = self.service._default_state(starting_balance=135000.0)
        state["activity_log"] = [{"timestamp": self.now.isoformat(), "title": "opened"}]
        self.service._save_state(state)

        result = self.service.reset_state(new_starting_balance=150000.0)

        self.assertEqual(result["deleted_trade_count"], 1)
        self.assertTrue(Path(result["archive_path"]).exists())
        self.assertEqual(self.service._load_state()["starting_balance"], 150000.0)
        self.assertEqual(len(self.trade_store.list_trades("talos")), 0)

    def test_background_schedule_stops_repeating_after_final_post_close_pass(self) -> None:
        final_window = datetime(2026, 4, 20, 15, 0, 30, tzinfo=self.chicago)
        next_run = self.service._resolve_next_background_run_at(final_window)
        self.assertEqual(next_run, datetime(2026, 4, 20, 15, 1, tzinfo=self.chicago))

        after_final_window = datetime(2026, 4, 20, 15, 1, 1, tzinfo=self.chicago)
        next_day_run = self.service._resolve_next_background_run_at(after_final_window)
        self.assertEqual(next_day_run, datetime(2026, 4, 21, 8, 30, tzinfo=self.chicago))

    def test_get_dashboard_payload_reconciles_duplicate_same_system_trades(self) -> None:
        older_id = self.trade_store.create_trade(self._trade_payload("Apollo", profile="Aggressive"))
        newer_payload = self._trade_payload("Apollo", profile="Aggressive")
        newer_payload["entry_datetime"] = datetime(2026, 4, 20, 10, 5, tzinfo=self.chicago).isoformat(timespec="minutes")
        newer_id = self.trade_store.create_trade(newer_payload)
        self.open_trade_manager.current_pl_by_trade_id[older_id] = 120.0
        self.open_trade_manager.current_pl_by_trade_id[newer_id] = 140.0

        payload = self.service.get_dashboard_payload()

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        self.assertEqual(len(open_trades), 2)
        self.assertEqual(payload["open_trade_count"], 2)
        self.assertFalse(any("did not auto-close" in item.get("reason", "") for item in payload["skip_log"]))

    def test_append_skip_merges_identical_repeated_entries(self) -> None:
        state = self.service._default_state()

        self.service._append_skip(state, {"timestamp": self.now.isoformat(), "system": "Talos", "reason": "Market window is closed."})
        self.service._append_skip(state, {"timestamp": self.now.isoformat(), "system": "Talos", "reason": "Market window is closed."})

        self.assertEqual(len(state["skip_log"]), 1)
        self.assertEqual(state["skip_log"][0]["repeat_count"], 2)

    def test_apollo_entry_pricing_uses_executable_credit(self) -> None:
        pricing = self.service._resolve_apollo_entry_pricing(
            {
                "credit": 3.05,
                "executable_credit": 2.77,
                "short_put": {"bid": 3.12},
                "long_put": {"ask": 0.35},
            }
        )

        self.assertEqual(pricing["actual_entry_credit"], 2.77)
        self.assertIn("short bid 3.12 - long ask 0.35 = 2.77", pricing["pricing_math_display"])

    def test_kairos_entry_pricing_uses_conservative_credit(self) -> None:
        pricing = self.service._resolve_kairos_entry_pricing(
            {
                "credit_estimate_dollars": 305.0,
                "conservative_credit_dollars": 277.0,
                "short_leg_bid": 3.12,
                "long_leg_ask": 0.35,
            }
        )

        self.assertEqual(pricing["actual_entry_credit"], 2.77)
        self.assertIn("short bid 3.12 - long ask 0.35 = 2.77", pricing["pricing_math_display"])

    def test_learning_state_uses_all_trade_sources_and_shifts_weights_gradually(self) -> None:
        state = self.service._default_state()
        original_list_trades = self.trade_store.list_trades

        trade_map = {
            "real": [
                {
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "vix_at_entry": 21.0,
                    "gross_pnl": -650.0,
                    "win_loss_result": "Black Swan",
                    "actual_entry_credit": 1.1,
                    "spread_width": 10.0,
                    "max_theoretical_risk": 890.0,
                    "actual_em_multiple": 1.6,
                    "actual_distance_to_short": 48.0,
                    "structure_grade": "Good",
                    "macro_grade": "None",
                    "total_premium": 110.0,
                }
            ],
            "simulated": [
                {
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "vix_at_entry": 20.0,
                    "gross_pnl": -140.0,
                    "win_loss_result": "Loss",
                    "actual_entry_credit": 1.0,
                    "spread_width": 10.0,
                    "max_theoretical_risk": 900.0,
                    "actual_em_multiple": 1.5,
                    "actual_distance_to_short": 45.0,
                    "structure_grade": "Good",
                    "macro_grade": "None",
                    "total_premium": 100.0,
                }
            ],
            "talos": [
                {
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "vix_at_entry": 22.0,
                    "gross_pnl": -720.0,
                    "win_loss_result": "Black Swan",
                    "actual_entry_credit": 1.05,
                    "spread_width": 10.0,
                    "max_theoretical_risk": 895.0,
                    "actual_em_multiple": 1.55,
                    "actual_distance_to_short": 46.0,
                    "structure_grade": "Good",
                    "macro_grade": "None",
                    "total_premium": 105.0,
                }
            ],
        }
        self.trade_store.list_trades = lambda trade_mode: trade_map.get(trade_mode, original_list_trades(trade_mode))

        self.service._refresh_learning_state(state)

        learning_state = state["learning_state"]
        profile = learning_state["profiles"]["Apollo_HIGH_VIX"]
        source_types = {item["source_type"] for item in learning_state["records"]}
        self.assertEqual(source_types, {"real", "simulated", "talos"})
        self.assertTrue(profile["adaptive_enabled"])
        self.assertAlmostEqual(profile["weighted_trade_count"], 3.2)
        self.assertEqual(profile["weights"]["safety"], 34.0)
        self.assertEqual(profile["weights"]["loss_penalty"], 22.0)

    def test_learning_state_falls_back_to_default_weights_when_data_is_insufficient(self) -> None:
        state = self.service._default_state()

        self.service._refresh_learning_state(state)

        payload = self.service._build_learning_state_payload(state["learning_state"])
        self.assertFalse(payload["adaptive_enabled"])
        self.assertEqual(payload["current_weights"]["safety"], 32.0)
        self.assertEqual(payload["current_weights"]["loss_penalty"], 20.0)

    def test_talos_kairos_credit_floor_override_accepts_prime_candidate(self) -> None:
        class _StubKairosService:
            def __init__(self, *args, **kwargs):
                return None

            def initialize_live_kairos_on_page_load(self):
                return {
                    "current_timing_status": "Eligible",
                    "current_structure_status": "Bullish Confirmation",
                    "best_trade_override": {
                        "status": "stand-aside",
                        "candidate": {
                            "mode_key": "prime",
                            "mode_label": "Prime",
                            "mode_descriptor": "Window Open",
                            "actual_em_multiple": 1.7,
                            "daily_move_multiple": 1.7,
                            "actual_distance_to_short": 42.0,
                            "distance_points": 42.0,
                            "estimated_short_delta": 0.11,
                            "expected_move_used": 25.0,
                            "credit_estimate_dollars": 85.0,
                            "conservative_credit_dollars": 82.0,
                            "spread_width": 5.0,
                            "max_loss_dollars": 418.0,
                            "recommended_contracts": 1,
                            "short_strike": 5200,
                            "long_strike": 5195,
                            "rejection_reasons": ["Current live candidate is below the minimum credit threshold."],
                        },
                    },
                }

        with patch("services.talos_service.KairosService", _StubKairosService):
            candidate = self.service._build_kairos_candidate(135000.0)

        self.assertIsNotNone(candidate)
        self.assertTrue(candidate["credit_floor_override"])
        self.assertIn("Talos credit-floor override", candidate["decision_note"])

    def test_talos_kairos_accepts_subprime_improving_candidate_in_simulation(self) -> None:
        class _StubKairosService:
            def __init__(self, *args, **kwargs):
                return None

            def initialize_live_kairos_on_page_load(self):
                return {
                    "current_timing_status": "Eligible",
                    "current_structure_status": "Developing",
                    "best_trade_override": {
                        "status": "ready",
                        "candidate": {
                            "mode_key": "subprime-improving",
                            "mode_label": "Subprime Improving",
                            "mode_descriptor": "Subprime Improving",
                            "actual_em_multiple": 1.6,
                            "daily_move_multiple": 1.6,
                            "actual_distance_to_short": 41.0,
                            "distance_points": 41.0,
                            "estimated_short_delta": 0.12,
                            "expected_move_used": 24.0,
                            "credit_estimate_dollars": 110.0,
                            "conservative_credit_dollars": 105.0,
                            "spread_width": 5.0,
                            "max_loss_dollars": 395.0,
                            "recommended_contracts": 1,
                            "short_strike": 5205,
                            "long_strike": 5200,
                            "rejection_reasons": [],
                        },
                    },
                }

        with patch("services.talos_service.KairosService", _StubKairosService):
            candidate = self.service._build_kairos_candidate(135000.0)

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["trade_payload"]["candidate_profile"], "Subprime Improving")
        self.assertIn("Subprime Improving", candidate["decision_note"])
        self.assertIn("Subprime Improving", candidate["entry_gate_label"])

    def test_talos_kairos_still_accepts_window_open_candidate(self) -> None:
        class _StubKairosService:
            def __init__(self, *args, **kwargs):
                return None

            def initialize_live_kairos_on_page_load(self):
                return {
                    "current_timing_status": "Eligible",
                    "current_structure_status": "Bullish Confirmation",
                    "best_trade_override": {
                        "status": "ready",
                        "candidate": {
                            "mode_key": "window-open",
                            "mode_label": "Window Open",
                            "mode_descriptor": "Window Open",
                            "actual_em_multiple": 1.7,
                            "daily_move_multiple": 1.7,
                            "actual_distance_to_short": 42.0,
                            "distance_points": 42.0,
                            "estimated_short_delta": 0.11,
                            "expected_move_used": 25.0,
                            "credit_estimate_dollars": 115.0,
                            "conservative_credit_dollars": 110.0,
                            "spread_width": 5.0,
                            "max_loss_dollars": 390.0,
                            "recommended_contracts": 1,
                            "short_strike": 5200,
                            "long_strike": 5195,
                            "rejection_reasons": [],
                        },
                    },
                }

        with patch("services.talos_service.KairosService", _StubKairosService):
            candidate = self.service._build_kairos_candidate(135000.0)

        self.assertIsNotNone(candidate)
        self.assertIn("Window Open", candidate["decision_note"])

    def test_talos_rejects_candidates_below_minimum_entry_score(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._build_apollo_candidate = lambda account_balance: self._candidate("Apollo", 69.0)
        self.service._build_kairos_candidate = lambda account_balance: None

        payload = self.service.run_cycle(trigger_reason="manual")

        self.assertEqual(payload["open_trade_count"], 0)
        self.assertEqual(len(self.trade_store.list_trades("talos")), 0)
        self.assertTrue(any("score was 69.0" in item.get("reason", "") for item in payload["skip_log"]))

    def test_score_band_distinguishes_weak_and_reject(self) -> None:
        weak_band = self.service._score_band_for_value(64.0)
        reject_band = self.service._score_band_for_value(58.0)

        self.assertEqual(weak_band["label"], "Weak")
        self.assertEqual(weak_band["key"], "weak")
        self.assertEqual(reject_band["label"], "Reject")
        self.assertEqual(reject_band["key"], "reject")

    def test_current_commitment_payload_uses_projected_black_swan_loss_against_account_value(self) -> None:
        commitment = self.service._build_current_commitment_payload(
            [
                {"max_loss": 820.0},
                {"remaining_risk": 410.0},
            ],
            {"current_account_value": 135250.0},
        )

        self.assertEqual(commitment["basis"], "projected_black_swan_loss")
        self.assertEqual(commitment["value"], 1230.0)
        self.assertEqual(commitment["value_display"], "$1,230.00")
        self.assertAlmostEqual(commitment["ratio"], round(1230.0 / 135250.0, 4))

    def test_apollo_entry_blocked_outside_final_three_hours(self) -> None:
        self.service._build_apollo_candidate = lambda account_balance: self._candidate("Apollo", 72.0)
        self.service._build_kairos_candidate = lambda account_balance: None

        payload = self.service.run_cycle(trigger_reason="manual")

        self.assertEqual(payload["open_trade_count"], 0)
        self.assertTrue(any("Apollo can only open during the final 3 hours" in item.get("reason", "") for item in payload["skip_log"]))

    def test_talos_allows_apollo_entry_inside_final_three_hours(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._build_apollo_candidate = lambda account_balance: self._candidate("Apollo", 72.0)
        self.service._build_kairos_candidate = lambda account_balance: None

        payload = self.service.run_cycle(trigger_reason="manual")

        self.assertEqual(payload["open_trade_count"], 1)

    def test_select_apollo_portfolio_respects_black_swan_loss_cap(self) -> None:
        plan = self._apollo_plan(
            [
                self._apollo_candidate(80.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, max_theoretical_risk=6000.0, total_premium=250.0),
                self._apollo_candidate(79.0, profile="Fortress", short_strike=5230.0, long_strike=5220.0, max_theoretical_risk=6000.0, total_premium=250.0),
                self._apollo_candidate(78.0, profile="Aggressive", short_strike=5210.0, long_strike=5200.0, max_theoretical_risk=3500.0, total_premium=180.0),
            ],
            account_balance=100000.0,
        )

        self.assertGreaterEqual(len(plan["selected_candidates"]), 1)
        self.assertLessEqual(plan["projected_black_swan_loss_used"], 10000.0)

    def test_select_apollo_portfolio_can_choose_below_target_mixed_combo_when_it_scores_best(self) -> None:
        original_profile_history = self.service._build_apollo_profile_history_summary
        original_combination_history = self.service._build_apollo_combination_history_summary
        self.service._build_apollo_profile_history_summary = lambda: {
            "Standard": {"weighted_trade_count": 9.0, "win_rate": 0.72, "expectancy": 76.0, "average_win": 220.0, "average_loss": -84.0, "black_swan_rate": 0.03, "net_pnl": 710.0, "credit_efficiency": 0.23, "outcome_mix_quality": 0.80},
            "Fortress": {"weighted_trade_count": 7.0, "win_rate": 0.69, "expectancy": 63.0, "average_win": 205.0, "average_loss": -82.0, "black_swan_rate": 0.03, "net_pnl": 540.0, "credit_efficiency": 0.20, "outcome_mix_quality": 0.76},
            "Aggressive": {"weighted_trade_count": 4.0, "win_rate": 0.49, "expectancy": 31.0, "average_win": 248.0, "average_loss": -152.0, "black_swan_rate": 0.10, "net_pnl": 110.0, "credit_efficiency": 0.22, "outcome_mix_quality": 0.43},
        }
        self.service._build_apollo_combination_history_summary = lambda profile_history=None: {
            "Standard": {"mix_quality": 0.75, "trade_count": 9.0},
            "Fortress": {"mix_quality": 0.70, "trade_count": 7.0},
            "Aggressive": {"mix_quality": 0.41, "trade_count": 4.0},
            "Fortress+Standard": {"mix_quality": 0.93, "trade_count": 16.0},
            "Aggressive+Standard": {"mix_quality": 0.38, "trade_count": 13.0},
            "Aggressive+Fortress": {"mix_quality": 0.36, "trade_count": 11.0},
            "Aggressive+Fortress+Standard": {"mix_quality": 0.44, "trade_count": 20.0},
        }

        try:
            plan = self._apollo_plan(
                [
                    self._apollo_candidate(79.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=430.0, max_theoretical_risk=1200.0),
                    self._apollo_candidate(77.5, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=320.0, max_theoretical_risk=1100.0),
                    self._apollo_candidate(71.0, profile="Aggressive", short_strike=5215.0, long_strike=5205.0, total_premium=210.0, max_theoretical_risk=2200.0),
                ],
                account_balance=100000.0,
            )
        finally:
            self.service._build_apollo_profile_history_summary = original_profile_history
            self.service._build_apollo_combination_history_summary = original_combination_history

        self.assertEqual(plan["combination_shape"], "mixed-profile")
        self.assertGreaterEqual(len(plan["selected_candidates"]), 2)
        self.assertLess(plan["selected_total_premium"], plan["premium_target"])
        self.assertIn("Standard x1", plan["selected_profile_mix"])
        self.assertIn("Fortress x1", plan["selected_profile_mix"])
        self.assertIn("scoring objective rather than a gate", plan["selection_note"])

    def test_select_apollo_portfolio_can_move_toward_target_with_best_valid_next_addition(self) -> None:
        self.trade_store.create_trade(self._trade_payload("Apollo", profile="Standard", total_premium=250.0, max_theoretical_risk=820.0))
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]
        plan = self._apollo_plan(
            [
                self._apollo_candidate(78.0, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=330.0, max_theoretical_risk=1200.0),
                self._apollo_candidate(77.0, profile="Aggressive", short_strike=5220.0, long_strike=5210.0, total_premium=360.0, max_theoretical_risk=1200.0),
            ],
            account_balance=100000.0,
            open_records=open_records,
        )

        self.assertEqual(len(plan["selected_candidates"]), 1)
        self.assertGreater(plan["selected_total_premium"], plan["current_total_premium"])
        self.assertIn("Standard x1", plan["selected_profile_mix"])
        self.assertTrue(
            "Fortress x1" in plan["selected_profile_mix"] or "Aggressive x1" in plan["selected_profile_mix"]
        )
        self.assertEqual(plan["combination_shape"], "incremental-addition")

    def test_select_apollo_portfolio_prefers_best_incremental_addition_when_mixed_book_is_best(self) -> None:
        self.trade_store.create_trade(self._trade_payload("Apollo", profile="Standard", total_premium=250.0, max_theoretical_risk=820.0))
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]
        original_profile_history = self.service._build_apollo_profile_history_summary
        original_combination_history = self.service._build_apollo_combination_history_summary
        self.service._build_apollo_profile_history_summary = lambda: {
            "Standard": {"weighted_trade_count": 8.0, "win_rate": 0.68, "expectancy": 72.0, "average_win": 210.0, "average_loss": -95.0, "black_swan_rate": 0.05, "net_pnl": 620.0, "credit_efficiency": 0.21, "outcome_mix_quality": 0.73},
            "Fortress": {"weighted_trade_count": 7.0, "win_rate": 0.72, "expectancy": 61.0, "average_win": 190.0, "average_loss": -80.0, "black_swan_rate": 0.03, "net_pnl": 580.0, "credit_efficiency": 0.19, "outcome_mix_quality": 0.77},
            "Aggressive": {"weighted_trade_count": 5.0, "win_rate": 0.55, "expectancy": 48.0, "average_win": 260.0, "average_loss": -140.0, "black_swan_rate": 0.08, "net_pnl": 300.0, "credit_efficiency": 0.24, "outcome_mix_quality": 0.58},
        }
        self.service._build_apollo_combination_history_summary = lambda profile_history=None: {
            "Standard": {"mix_quality": 0.70, "trade_count": 8.0},
            "Fortress": {"mix_quality": 0.68, "trade_count": 7.0},
            "Aggressive": {"mix_quality": 0.50, "trade_count": 5.0},
            "Fortress+Standard": {"mix_quality": 0.86, "trade_count": 15.0},
            "Aggressive+Standard": {"mix_quality": 0.61, "trade_count": 13.0},
            "Aggressive+Fortress": {"mix_quality": 0.57, "trade_count": 12.0},
            "Aggressive+Fortress+Standard": {"mix_quality": 0.66, "trade_count": 20.0},
        }

        try:
            plan = self._apollo_plan(
                [
                    self._apollo_candidate(77.5, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=320.0, max_theoretical_risk=1100.0),
                    self._apollo_candidate(74.0, profile="Aggressive", short_strike=5215.0, long_strike=5205.0, total_premium=390.0, max_theoretical_risk=1800.0),
                ],
                account_balance=100000.0,
                open_records=open_records,
            )
        finally:
            self.service._build_apollo_profile_history_summary = original_profile_history
            self.service._build_apollo_combination_history_summary = original_combination_history

        self.assertEqual(plan["combination_shape"], "incremental-addition")
        self.assertIn("Standard x1", plan["selected_profile_mix"])
        self.assertIn("Fortress x1", plan["selected_profile_mix"])
        self.assertIn("Fortress", plan["next_addition_summary"])
        self.assertGreater(plan["selected_portfolio_score"], 0.0)

    def test_select_apollo_portfolio_can_still_choose_single_standard_profile_when_best(self) -> None:
        original_profile_history = self.service._build_apollo_profile_history_summary
        original_combination_history = self.service._build_apollo_combination_history_summary
        self.service._build_apollo_profile_history_summary = lambda: {
            "Standard": {"weighted_trade_count": 10.0, "win_rate": 0.74, "expectancy": 88.0, "average_win": 220.0, "average_loss": -82.0, "black_swan_rate": 0.02, "net_pnl": 780.0, "credit_efficiency": 0.24, "outcome_mix_quality": 0.82},
            "Fortress": {"weighted_trade_count": 6.0, "win_rate": 0.60, "expectancy": 40.0, "average_win": 170.0, "average_loss": -88.0, "black_swan_rate": 0.05, "net_pnl": 240.0, "credit_efficiency": 0.18, "outcome_mix_quality": 0.61},
            "Aggressive": {"weighted_trade_count": 4.0, "win_rate": 0.48, "expectancy": 28.0, "average_win": 250.0, "average_loss": -155.0, "black_swan_rate": 0.12, "net_pnl": 90.0, "credit_efficiency": 0.22, "outcome_mix_quality": 0.46},
        }
        self.service._build_apollo_combination_history_summary = lambda profile_history=None: {
            "Standard": {"mix_quality": 0.92, "trade_count": 10.0},
            "Fortress": {"mix_quality": 0.58, "trade_count": 6.0},
            "Aggressive": {"mix_quality": 0.40, "trade_count": 4.0},
            "Fortress+Standard": {"mix_quality": 0.28, "trade_count": 16.0},
            "Aggressive+Standard": {"mix_quality": 0.20, "trade_count": 14.0},
            "Aggressive+Fortress": {"mix_quality": 0.18, "trade_count": 10.0},
            "Aggressive+Fortress+Standard": {"mix_quality": 0.16, "trade_count": 20.0},
        }

        try:
            plan = self._apollo_plan(
                [
                    self._apollo_candidate(82.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=410.0, max_theoretical_risk=1100.0),
                    self._apollo_candidate(70.5, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=180.0, max_theoretical_risk=1900.0),
                    self._apollo_candidate(70.2, profile="Aggressive", short_strike=5215.0, long_strike=5205.0, total_premium=210.0, max_theoretical_risk=2600.0),
                ],
                account_balance=50000.0,
            )
        finally:
            self.service._build_apollo_profile_history_summary = original_profile_history
            self.service._build_apollo_combination_history_summary = original_combination_history

        self.assertEqual(plan["combination_shape"], "incremental-addition")
        self.assertEqual(plan["selected_profile_mix"], "Standard x1")

    def test_select_apollo_portfolio_scales_quantity_down_to_fit_black_swan_cap(self) -> None:
        plan = self._apollo_plan(
            [
                self._apollo_candidate(
                    82.0,
                    profile="Standard",
                    short_strike=5250.0,
                    long_strike=5240.0,
                    contracts=3,
                    total_premium=900.0,
                    max_theoretical_risk=12000.0,
                )
            ],
            account_balance=100000.0,
        )

        self.assertEqual(len(plan["selected_candidates"]), 1)
        self.assertTrue(plan["quantity_scaling_applied"])
        self.assertEqual(plan["default_contract_quantity_total"], 3)
        self.assertEqual(plan["selected_contract_quantity_total"], 2)
        self.assertLessEqual(plan["projected_black_swan_loss_used"], 10000.0)
        self.assertIn("quantity scaling qualified the portfolio", plan["scaling_summary"])

    def test_select_apollo_portfolio_rejects_candidate_that_cannot_fit_even_at_one_contract(self) -> None:
        plan = self._apollo_plan(
            [
                self._apollo_candidate(
                    82.0,
                    profile="Standard",
                    short_strike=5250.0,
                    long_strike=5240.0,
                    contracts=2,
                    total_premium=700.0,
                    max_theoretical_risk=24000.0,
                )
            ],
            account_balance=100000.0,
        )

        self.assertEqual(plan["selected_candidates"], [])
        self.assertIn("blocked after scaling qty 2->1", plan["selection_note"])
        self.assertIn("blocked after scaling qty 2->1", plan["combinations_evaluated_summary"])

    def test_evaluate_apollo_portfolio_persists_scaled_quantity_when_it_qualifies(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        plan = self._apollo_plan(
            [
                self._apollo_candidate(
                    82.0,
                    profile="Standard",
                    short_strike=5250.0,
                    long_strike=5240.0,
                    contracts=3,
                    total_premium=900.0,
                    max_theoretical_risk=12000.0,
                )
            ],
            account_balance=100000.0,
        )

        action_taken = self.service._evaluate_apollo_portfolio(state, plan, [])

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        self.assertTrue(action_taken)
        self.assertEqual(len(open_trades), 1)
        self.assertEqual(int(open_trades[0].get("contracts") or 0), 2)
        self.assertTrue(any("qty 3->2" in item.get("detail", "") for item in state["activity_log"]))

    def test_evaluate_apollo_portfolio_bootstraps_from_zero_open_positions(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        plan = self._apollo_plan(
            [
                self._apollo_candidate(81.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=410.0, max_theoretical_risk=1100.0),
                self._apollo_candidate(79.0, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=320.0, max_theoretical_risk=1100.0),
            ],
            account_balance=100000.0,
            open_records=[],
        )

        action_taken = self.service._evaluate_apollo_portfolio(state, plan, [])

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        self.assertTrue(action_taken)
        self.assertEqual(len(open_trades), 1)
        self.assertTrue(any(item.get("title") == "Apollo bootstrap entry" for item in state["activity_log"]))
        self.assertTrue(any("portfolio empty before entry yes" in item.get("detail", "") for item in state["activity_log"]))
        self.assertTrue(any("bootstrap trade yes" in item.get("detail", "") for item in state["activity_log"]))

    def test_evaluate_apollo_portfolio_ignores_non_talos_apollo_records_when_checking_live_cap(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        plan = self._apollo_plan(
            [
                self._apollo_candidate(82.0, profile="Aggressive", short_strike=5225.0, long_strike=5215.0, total_premium=2240.0, max_theoretical_risk=13011.34),
            ],
            account_balance=135000.0,
            open_records=[],
        )
        original_evaluate_open_trades = self.open_trade_manager.evaluate_open_trades
        self.open_trade_manager.evaluate_open_trades = lambda send_alerts=False: {
            "records": [
                {
                    "trade_id": 999,
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "profile_label": "Standard",
                    "contracts": 8,
                    "short_strike": 5250.0,
                    "long_strike": 5240.0,
                    "max_theoretical_risk": 15792.24,
                    "actual_entry_credit": 0.0,
                }
            ]
        }

        try:
            action_taken = self.service._evaluate_apollo_portfolio(state, plan, [])
        finally:
            self.open_trade_manager.evaluate_open_trades = original_evaluate_open_trades

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        self.assertTrue(action_taken)
        self.assertEqual(len(open_trades), 1)
        self.assertFalse(any("already at or above the hard cap" in item.get("reason", "") for item in state["skip_log"]))

    def test_run_cycle_builds_apollo_portfolio_from_zero_open_positions_over_successive_cycles(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._build_apollo_portfolio_plan = lambda account_balance, open_records, learning_state=None: self._apollo_plan(
            [
                self._apollo_candidate(81.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=410.0, max_theoretical_risk=1100.0),
                self._apollo_candidate(79.5, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=320.0, max_theoretical_risk=1100.0),
                self._apollo_candidate(77.0, profile="Aggressive", short_strike=5215.0, long_strike=5205.0, total_premium=260.0, max_theoretical_risk=1600.0),
            ],
            account_balance=account_balance,
            open_records=open_records,
        )
        self.service._build_kairos_candidate = lambda account_balance: None

        first_payload = self.service.run_cycle(trigger_reason="manual")
        self.now = datetime(2026, 4, 20, 13, 1, tzinfo=self.chicago)
        second_payload = self.service.run_cycle(trigger_reason="manual")

        self.assertEqual(first_payload["open_trade_count"], 1)
        self.assertEqual(second_payload["open_trade_count"], 2)
        state = self.service._load_state()
        self.assertTrue(any("bootstrap trade yes" in item.get("detail", "") for item in state["activity_log"]))
        self.assertTrue(any("more capacity remains yes" in item.get("detail", "") for item in state["activity_log"]))

    def test_get_dashboard_payload_formats_activity_log_timestamp_for_display(self) -> None:
        state = self.service._default_state()
        state["activity_log"] = [
            {
                "timestamp": datetime(2026, 4, 23, 13, 12, tzinfo=self.chicago).isoformat(),
                "category": "apollo-portfolio",
                "title": "Apollo portfolio review",
                "detail": "Selected option Standard x1 won because it was the highest-scoring legal Apollo option; portfolio empty before entry yes; bootstrap trade yes; remaining live Black Swan capacity $9,000.00; more capacity remains yes.",
            }
        ]
        self.service._save_state(state)

        payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["activity_log"][0]["timestamp"], "04-23-26 13:12 CST")

    def test_get_dashboard_payload_includes_credit_captured_display_for_open_talos_positions(self) -> None:
        trade_id = self.trade_store.create_trade(self._trade_payload("Apollo"))
        self.open_trade_manager.record_overrides_by_trade_id[trade_id] = {
            "current_spread_mark": 0.33,
            "percent_credit_captured": 81.67,
            "percent_credit_captured_display": "81.7%",
        }

        payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["open_records"][0]["percent_credit_captured_display"], "81.7%")

    def test_manage_open_trades_auto_liquidates_when_credit_capture_reaches_eighty_percent(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        trade_id = self.trade_store.create_trade(self._trade_payload("Apollo", total_premium=180.0))
        state = self.service._default_state()
        self.open_trade_manager.record_overrides_by_trade_id[trade_id] = {
            "current_spread_mark": 0.33,
            "current_total_close_cost": 33.0,
            "percent_credit_captured": 81.67,
            "percent_credit_captured_display": "81.7%",
            "total_premium": 180.0,
        }
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]

        action_taken = self.service._manage_open_trades(state, open_records)

        trade = self.trade_store.get_trade(trade_id)
        self.assertTrue(action_taken)
        self.assertEqual(str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower(), "closed")
        self.assertTrue(any(item.get("category") == "capture-close" for item in state["activity_log"]))
        self.assertTrue(any("81.7%" in item.get("detail", "") for item in state["activity_log"]))

    def test_manage_open_trades_blocks_capture_liquidation_during_close_restricted_window(self) -> None:
        self.now = datetime(2026, 4, 20, 8, 40, tzinfo=self.chicago)
        trade_id = self.trade_store.create_trade(self._trade_payload("Apollo", total_premium=180.0))
        state = self.service._default_state()
        self.open_trade_manager.record_overrides_by_trade_id[trade_id] = {
            "current_spread_mark": 0.33,
            "current_total_close_cost": 33.0,
            "percent_credit_captured": 81.67,
            "total_premium": 180.0,
        }
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]

        action_taken = self.service._manage_open_trades(state, open_records)

        trade = self.trade_store.get_trade(trade_id)
        self.assertFalse(action_taken)
        self.assertEqual(str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower(), "open")
        self.assertTrue(any("80% capture liquidation" in item.get("reason", "") for item in state["skip_log"]))

    def test_apollo_profile_history_summary_uses_all_trade_sources(self) -> None:
        original_builder = self.service._build_learning_records
        self.service._build_learning_records = lambda: [
            {"source_type": "real", "source_weight": 1.0, "system": "Apollo", "profile": "Standard", "segment": "Apollo_LOW_VIX", "vix_regime": "LOW_VIX", "em_multiple": 1.4, "distance_to_short": 50.0, "outcome": "win", "realized_pnl": 120.0, "max_drawdown": 80.0, "drawdown_severity": 0.10, "credit_efficiency": 0.18, "fit_context_available": True, "total_premium": 180.0},
            {"source_type": "simulated", "source_weight": 0.7, "system": "Apollo", "profile": "Fortress", "segment": "Apollo_LOW_VIX", "vix_regime": "LOW_VIX", "em_multiple": 1.6, "distance_to_short": 55.0, "outcome": "loss", "realized_pnl": -80.0, "max_drawdown": 120.0, "drawdown_severity": 0.16, "credit_efficiency": 0.16, "fit_context_available": True, "total_premium": 170.0},
            {"source_type": "talos", "source_weight": 1.5, "system": "Apollo", "profile": "Aggressive", "segment": "Apollo_LOW_VIX", "vix_regime": "LOW_VIX", "em_multiple": 1.8, "distance_to_short": 42.0, "outcome": "win", "realized_pnl": 60.0, "max_drawdown": 140.0, "drawdown_severity": 0.18, "credit_efficiency": 0.22, "fit_context_available": True, "total_premium": 210.0},
        ]

        try:
            summary = self.service._build_apollo_profile_history_summary()
        finally:
            self.service._build_learning_records = original_builder

        self.assertIn("Standard", summary)
        self.assertIn("Fortress", summary)
        self.assertIn("Aggressive", summary)
        self.assertGreater(summary["Standard"]["weighted_trade_count"], 0.0)
        self.assertGreater(summary["Fortress"]["weighted_trade_count"], 0.0)
        self.assertGreater(summary["Aggressive"]["weighted_trade_count"], 0.0)

    def test_evaluate_apollo_portfolio_adds_only_one_additional_position_per_cycle(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        self.trade_store.create_trade(self._trade_payload("Apollo", profile="Standard"))
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]
        plan = self._apollo_plan(
            [
                self._apollo_candidate(79.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=340.0, max_theoretical_risk=1200.0),
                self._apollo_candidate(78.0, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=330.0, max_theoretical_risk=1200.0),
                self._apollo_candidate(77.0, profile="Standard", short_strike=5220.0, long_strike=5210.0, total_premium=360.0, max_theoretical_risk=1200.0),
            ],
            open_records=open_records,
        )

        action_taken = self.service._evaluate_apollo_portfolio(state, plan, open_records)

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        self.assertTrue(action_taken)
        self.assertEqual(len(open_trades), 2)
        self.assertTrue(any(item.get("category") == "apollo-portfolio" and "Opened" in item.get("detail", "") for item in state["activity_log"]))

    def test_evaluate_apollo_portfolio_logs_reason_when_no_additional_position_is_needed(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        self.trade_store.create_trade(self._trade_payload("Apollo", profile="Standard"))
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]
        plan = self._apollo_plan([self._apollo_candidate(80.0, profile="Standard")], open_records=open_records)

        action_taken = self.service._evaluate_apollo_portfolio(state, plan, open_records)

        self.assertFalse(action_taken)
        self.assertTrue(any("Apollo" in item.get("reason", "") for item in state["skip_log"]))

    def test_evaluate_apollo_portfolio_rejects_candidate_that_would_exceed_live_black_swan_cap(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        self.trade_store.create_trade(self._trade_payload("Apollo", profile="Standard", short_strike=5250.0, long_strike=5240.0, contracts=11))
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]
        plan = self._apollo_plan(
            [
                self._apollo_candidate(79.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=340.0, max_theoretical_risk=1000.0),
                self._apollo_candidate(78.0, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=330.0, max_theoretical_risk=2200.0),
            ],
            account_balance=100000.0,
            open_records=open_records,
        )

        action_taken = self.service._evaluate_apollo_portfolio(state, plan, open_records)

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        self.assertFalse(action_taken)
        self.assertEqual(len(open_trades), 1)
        self.assertTrue(any("still above remaining cap" in item.get("reason", "") for item in state["skip_log"]))

    def test_evaluate_apollo_portfolio_stops_when_live_black_swan_cap_is_already_reached(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        state = self.service._default_state()
        self.trade_store.create_trade(self._trade_payload("Apollo", profile="Standard", short_strike=5250.0, long_strike=5240.0, contracts=13))
        open_records = self.open_trade_manager.evaluate_open_trades()["records"]
        plan = self._apollo_plan(
            [
                self._apollo_candidate(79.0, profile="Standard", short_strike=5250.0, long_strike=5240.0, total_premium=340.0, max_theoretical_risk=1000.0),
                self._apollo_candidate(78.0, profile="Fortress", short_strike=5235.0, long_strike=5225.0, total_premium=330.0, max_theoretical_risk=1200.0),
            ],
            account_balance=100000.0,
            open_records=open_records,
        )

        action_taken = self.service._evaluate_apollo_portfolio(state, plan, open_records)

        self.assertFalse(action_taken)
        self.assertTrue(any("already at or above the hard cap" in item.get("reason", "") for item in state["skip_log"]))

    def test_score_candidate_prefers_standard_apollo_profile_when_trade_quality_matches(self) -> None:
        standard_score = self.service._score_candidate(
            system_name="Apollo",
            candidate_profile="Standard",
            vix_value=18.5,
            em_multiple=1.7,
            distance_points=48.0,
            short_delta=0.10,
            expected_move=30.0,
            credit=1.8,
            spread_width=10.0,
            max_loss=820.0,
            total_premium=180.0,
            raw_candidate_score=72.0,
            structure_context="Good",
            macro_context="None",
        )
        fortress_score = self.service._score_candidate(
            system_name="Apollo",
            candidate_profile="Fortress",
            vix_value=18.5,
            em_multiple=1.7,
            distance_points=48.0,
            short_delta=0.10,
            expected_move=30.0,
            credit=1.8,
            spread_width=10.0,
            max_loss=820.0,
            total_premium=180.0,
            raw_candidate_score=72.0,
            structure_context="Good",
            macro_context="None",
        )

        self.assertGreater(standard_score["standard_preference_component"], fortress_score["standard_preference_component"])
        self.assertGreater(standard_score["candidate_score"], fortress_score["candidate_score"])

    def test_manage_open_trades_closes_only_exact_due_gate_quantity(self) -> None:
        payload = self._trade_payload("Kairos")
        payload["contracts"] = 4
        trade_id = self.trade_store.create_trade(payload)
        state = self.service._default_state()

        action_taken = self.service._manage_open_trades(
            state,
            [
                {
                    "trade_id": trade_id,
                    "trade_number": 1,
                    "system_name": "Kairos",
                    "contracts": 4,
                    "current_spread_mark": 1.1,
                    "exit_plan_gates": [
                        {"label": "Gate 1", "due_now": True, "quantity_to_close": 1},
                        {"label": "Gate 2", "due_now": True, "quantity_to_close": 2},
                    ],
                    "evaluated_at_display": "Now",
                    "expiration": "2026-04-20",
                }
            ],
        )

        trade = self.trade_store.get_trade(trade_id)
        self.assertTrue(action_taken)
        self.assertEqual(int(trade.get("remaining_contracts") or 0), 3)
        self.assertTrue(any(item.get("category") == "gate-close" for item in state["activity_log"]))

    def test_manage_open_trades_blocks_close_during_first_fifteen_minutes(self) -> None:
        self.now = datetime(2026, 4, 20, 8, 35, tzinfo=self.chicago)
        payload = self._trade_payload("Kairos")
        payload["contracts"] = 2
        trade_id = self.trade_store.create_trade(payload)
        state = self.service._default_state()

        action_taken = self.service._manage_open_trades(
            state,
            [
                {
                    "trade_id": trade_id,
                    "trade_number": 1,
                    "system_name": "Kairos",
                    "contracts": 2,
                    "current_spread_mark": 1.1,
                    "exit_plan_gates": [{"label": "Gate 1", "due_now": True, "quantity_to_close": 1}],
                    "expiration": "2026-04-20",
                }
            ],
        )

        trade = self.trade_store.get_trade(trade_id)
        self.assertFalse(action_taken)
        self.assertEqual(int(trade.get("remaining_contracts") or trade.get("contracts") or 0), 2)
        self.assertTrue(any("first 15 minutes" in item.get("reason", "") for item in state["skip_log"]))

    def test_rotation_requires_more_than_seventy_five_percent_and_equal_or_better_score(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        trade_id = self.trade_store.create_trade(self._trade_payload("Apollo"))
        state = self.service._default_state()
        state["trade_metadata"] = {str(trade_id): {"candidate_score": 72.0}}
        candidate = self._candidate("Apollo", 72.0)

        blocked = self.service._evaluate_slot(
            state,
            "Apollo",
            candidate,
            [
                {
                    "trade_id": trade_id,
                    "trade_number": 1,
                    "system_name": "Apollo",
                    "contracts": 1,
                    "current_spread_mark": 0.2,
                    "percent_credit_captured": 75.0,
                    "talos_candidate_score": 72.0,
                }
            ],
        )
        self.assertFalse(blocked)

        rotated = self.service._evaluate_slot(
            state,
            "Apollo",
            candidate,
            [
                {
                    "trade_id": trade_id,
                    "trade_number": 1,
                    "system_name": "Apollo",
                    "contracts": 1,
                    "current_spread_mark": 0.2,
                    "percent_credit_captured": 76.0,
                    "talos_candidate_score": 72.0,
                }
            ],
        )
        self.assertTrue(rotated)


if __name__ == "__main__":
    unittest.main()