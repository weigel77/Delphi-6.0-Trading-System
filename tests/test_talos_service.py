import tempfile
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path
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

    def _trade_payload(self, system_name: str, profile: str = "Standard") -> dict:
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
            "short_strike": 5250.0,
            "long_strike": 5240.0,
            "spread_width": 10.0,
            "contracts": 1,
            "actual_entry_credit": 1.8,
            "net_credit_per_contract": 1.8,
            "distance_to_short": 50.0,
            "actual_distance_to_short": 50.0,
            "actual_em_multiple": 1.4,
            "short_delta": 0.11,
            "total_premium": 180.0,
            "max_theoretical_risk": 820.0,
            "notes_entry": f"{system_name} test entry",
        }

    def _candidate(self, system_name: str, score: float) -> dict:
        return {
            "candidate_score": score,
            "decision_note": f"{system_name} weighted score {score:.1f}.",
            "selection_basis": "Deterministic Talos weighted score.",
            "score_breakdown": {"candidate_score": score},
            "score_breakdown_display": f"Weighted score {score:.1f}.",
            "pricing_basis_label": "Conservative executable entry credit",
            "pricing_math_display": "short bid 3.20 - long ask 0.40 = 2.80",
            "entry_gate_label": f"Talos accepted {system_name}.",
            "trade_payload": self._trade_payload(system_name),
        }

    def test_run_cycle_enforces_one_apollo_one_kairos_and_two_total(self) -> None:
        self.now = datetime(2026, 4, 20, 13, 0, tzinfo=self.chicago)
        self.service._build_apollo_candidate = lambda account_balance: self._candidate("Apollo", 72.0)
        self.service._build_kairos_candidate = lambda account_balance: self._candidate("Kairos", 72.0)

        self.service.run_cycle(trigger_reason="manual")
        self.service.run_cycle(trigger_reason="manual")

        open_trades = [
            trade
            for trade in self.trade_store.list_trades("talos")
            if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
        ]
        counts = Counter(str(trade.get("system_name") or "") for trade in open_trades)
        self.assertEqual(len(open_trades), 2)
        self.assertEqual(counts["Apollo"], 1)
        self.assertEqual(counts["Kairos"], 1)

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
        self.assertTrue(any("did not auto-close" in item.get("reason", "") for item in payload["skip_log"]))

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

    def test_talos_rejects_candidates_below_minimum_entry_score(self) -> None:
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

    def test_current_commitment_payload_uses_open_max_loss_against_account_value(self) -> None:
        commitment = self.service._build_current_commitment_payload(
            [
                {"max_loss": 820.0},
                {"remaining_risk": 410.0},
            ],
            {"current_account_value": 135250.0},
        )

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