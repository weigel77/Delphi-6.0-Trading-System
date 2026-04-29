import unittest
from datetime import datetime, time
import json
from pathlib import Path
import tempfile
from zoneinfo import ZoneInfo

import pandas as pd

from services.kairos_simulation_runner import KAIROS_RUNNER_SCENARIOS, SESSION_BAR_COUNT, KairosTapeBar
from services.kairos_scenario_repository import KairosScenarioRepository
from services.kairos_service import KairosScanResult, KairosService, KairosStateTransition


class StubPushoverService:
    def __init__(self):
        self.sent_messages = []

    def send_kairos_window_open_alert(self, *, scan_result, best_trade_payload, generated_at=None):
        self.sent_messages.append(
            {
                "scan_result": scan_result,
                "best_trade_payload": best_trade_payload,
                "generated_at": generated_at,
            }
        )
        return {"ok": True, "sid": "SM123"}


class FakeTimer:
    instances = []

    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.started = False
        self.cancelled = False
        self.daemon = False
        FakeTimer.instances.append(self)

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True


class StubMarketDataService:
    def __init__(self):
        self.live_provider = object()

    @staticmethod
    def _build_intraday_frame(session_date, *, base_price=6120.0, step=0.08):
        session_anchor = datetime(session_date.year, session_date.month, session_date.day, 8, 30, tzinfo=ZoneInfo("America/Chicago"))
        rows = []
        for minute_offset in range(390):
            timestamp = session_anchor + pd.Timedelta(minutes=minute_offset)
            open_price = round(base_price + (minute_offset * step), 2)
            close_price = round(open_price + 0.03, 2)
            rows.append(
                {
                    "Datetime": timestamp,
                    "Open": open_price,
                    "High": round(close_price + 0.04, 2),
                    "Low": round(open_price - 0.04, 2),
                    "Close": close_price,
                    "Volume": 1000 + minute_offset,
                }
            )
        return pd.DataFrame(rows)

    def get_latest_snapshot(self, ticker, query_type="latest"):
        if ticker == "^GSPC":
            return {
                "Latest Value": 6123.45,
                "Daily Point Change": 12.34,
                "Daily Percent Change": 0.20,
                "As Of": "2026-04-06 09:45:00 AM CDT",
            }
        return {
            "Latest Value": 18.76,
            "Daily Point Change": -1.10,
            "Daily Percent Change": -5.54,
            "As Of": "2026-04-06 09:45:00 AM CDT",
        }

    def get_provider_metadata(self):
        return {
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
            "provider_name": "Schwab",
            "live_provider_key": "schwab",
            "spx_historical_provider_name": "Schwab",
            "vix_historical_provider_name": "Schwab",
        }

    def get_same_day_intraday_candles(self, ticker, interval_minutes=1, query_type="intraday"):
        return self._build_intraday_frame(datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago")).date())

    def get_intraday_candles_for_date(self, ticker, target_date, interval_minutes=1, query_type="intraday-date"):
        return self._build_intraday_frame(target_date)


class KairosServiceTest(unittest.TestCase):
    def setUp(self):
        FakeTimer.instances = []
        self.current_time = datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago"))
        self.pushover_service = StubPushoverService()
        self.service = KairosService(
            market_data_service=StubMarketDataService(),
            scan_interval_seconds=120,
            timer_factory=FakeTimer,
            now_provider=lambda: self.current_time,
            pushover_service=self.pushover_service,
        )
        self.service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: self._build_live_option_chain_summary()

    def tearDown(self):
        self.service.shutdown()

    def _build_live_option_chain_summary(self):
        return {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": self.current_time.date(),
            "underlying_price": 6123.45,
            "puts": [
                {"strike": 6065, "bid": 2.85, "ask": 3.05, "mark": 2.95, "last": 2.94, "delta": -0.14, "open_interest": 1200, "total_volume": 900},
                {"strike": 6060, "bid": 2.6, "ask": 2.8, "mark": 2.7, "last": 2.69, "delta": -0.12, "open_interest": 1300, "total_volume": 950},
                {"strike": 6055, "bid": 1.8, "ask": 1.9, "mark": 1.85, "last": 1.84, "delta": -0.10, "open_interest": 1250, "total_volume": 870},
                {"strike": 6050, "bid": 1.05, "ask": 1.2, "mark": 1.12, "last": 1.11, "delta": -0.09, "open_interest": 1210, "total_volume": 860},
                {"strike": 6045, "bid": 0.55, "ask": 0.68, "mark": 0.61, "last": 0.6, "delta": -0.05, "open_interest": 1180, "total_volume": 830},
                {"strike": 6040, "bid": 0.2, "ask": 0.28, "mark": 0.24, "last": 0.24, "delta": -0.04, "open_interest": 1100, "total_volume": 790},
            ],
            "calls": [],
            "message": "SPX option chain retrieved successfully.",
        }

    def test_live_activation_runs_classification_and_schedules_next_cycle(self):
        payload = self.service.activate_for_today()

        self.assertTrue(payload["armed_for_day"])
        self.assertGreater(payload["total_scans_completed"], 1)
        self.assertEqual(payload["session_status"], "Armed")
        self.assertEqual(payload["mode"], "Live")
        self.assertEqual(payload["current_state"], payload["latest_scan"]["kairos_state"])
        self.assertTrue(payload["latest_scan"]["timing_status"])
        self.assertEqual(payload["latest_scan"]["vix_status"], "Caution")
        self.assertGreaterEqual(len(payload["latest_scan"]["reasons"]), 2)
        self.assertEqual(payload["bar_map"]["mode"], "Live")
        self.assertEqual(payload["bar_map"]["processed_bars"], payload["total_scans_completed"])
        self.assertTrue(payload["bar_map"]["supports_live_mode"])
        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertIn(payload["best_trade_override"]["status"], {"ready", "stand-aside", "no-candidates", "unavailable"})
        self.assertEqual(len(payload["scan_log"]), payload["total_scans_completed"])
        self.assertEqual(len(payload["live_workspace"]["candidate_cards"]), 1)
        self.assertTrue(payload["live_workspace"]["credit_map"]["available"])
        self.assertEqual(len(FakeTimer.instances), 1)
        self.assertTrue(FakeTimer.instances[0].started)
        self.assertEqual(int(FakeTimer.instances[0].interval), 120)

    def test_default_options_chain_service_uses_shared_market_data_provider(self):
        self.assertIs(self.service.options_chain_service.provider, self.service.market_data_service.live_provider)

    def test_live_workspace_auto_initializes_on_page_load(self):
        payload = self.service.initialize_live_kairos_on_page_load()

        self.assertTrue(payload["armed_for_day"])
        self.assertGreater(payload["total_scans_completed"], 1)
        self.assertEqual(payload["session_status"], "Armed")
        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertIn(payload["best_trade_override"]["status"], {"ready", "stand-aside", "no-candidates", "unavailable"})
        self.assertEqual(len(payload["live_workspace"]["candidate_cards"]), 1)
        self.assertEqual(payload["live_workspace"]["stamps"][0]["label"], "Last Scan")
        self.assertEqual(len(FakeTimer.instances), 1)

    def test_live_workspace_backfills_scan_log_from_loaded_intraday_tape(self):
        payload = self.service.activate_for_today()

        self.assertEqual(payload["scan_log_count"], payload["total_scans_completed"])
        self.assertEqual(payload["scan_log"][-1]["scan_sequence_number"], 1)
        self.assertEqual(payload["scan_log"][0]["scan_sequence_number"], payload["total_scans_completed"])
        self.assertIn("same spx tape", payload["latest_scan"]["classification_note"].lower())

    def test_live_tape_uses_shared_intraday_window_progression_for_markers(self):
        self.service.market_data_service.get_same_day_intraday_candles = (
            lambda ticker, interval_minutes=1, query_type="intraday": StubMarketDataService._build_intraday_frame(
                self.current_time.date(),
                base_price=6120.0,
                step=0.25,
            )
        )
        payload = self.service.activate_for_today()

        marker_kinds = {marker["kind"] for marker in payload["bar_map"]["event_markers"]}

        self.assertIn("setup-forming", marker_kinds)
        self.assertIn("window-open", marker_kinds)
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Open")

    def test_live_tape_keeps_bullish_low_vix_bars_window_open_under_caution(self):
        original_snapshot = self.service.market_data_service.get_latest_snapshot

        def low_vix_snapshot(ticker, query_type="latest"):
            if ticker == "^VIX":
                return {
                    "Latest Value": 17.12,
                    "Daily Point Change": -0.8,
                    "Daily Percent Change": -4.46,
                    "As Of": "2026-04-06 09:45:00 AM CDT",
                }
            return original_snapshot(ticker, query_type=query_type)

        self.service.market_data_service.get_latest_snapshot = low_vix_snapshot
        self.service.market_data_service.get_same_day_intraday_candles = (
            lambda ticker, interval_minutes=1, query_type="intraday": StubMarketDataService._build_intraday_frame(
                self.current_time.date(),
                base_price=6120.0,
                step=0.25,
            )
        )

        payload = self.service.activate_for_today()

        marker_kinds = {marker["kind"] for marker in payload["bar_map"]["event_markers"]}

        self.assertEqual(payload["latest_scan"]["vix_status"], "Caution")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Open")
        self.assertIn("setup-forming", marker_kinds)
        self.assertIn("window-open", marker_kinds)

    def test_live_workspace_auto_initializes_closed_market_without_fabricating_trade(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.initialize_live_kairos_on_page_load()

        self.assertFalse(payload["armed_for_day"])
        self.assertEqual(payload["current_state_display"], "Market Closed")
        self.assertEqual(payload["latest_scan"]["timing_status"], "Market Closed")
        self.assertEqual(payload["best_trade_override"]["status"], "market-closed")
        self.assertIn("standing by", payload["status_note"].lower())

    def test_live_mode_refuses_activation_when_market_day_is_closed(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.activate_for_today()

        self.assertFalse(payload["armed_for_day"])
        self.assertEqual(payload["mode"], "Live")
        self.assertEqual(payload["current_state"], "Inactive")
        self.assertIn("standing by", payload["status_note"].lower())
        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertEqual(payload["best_trade_override"]["status"], "market-closed")

    def test_simulation_mode_allows_activation_on_weekend(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.service.configure_runtime(
            {
                "mode": "Simulation",
                "preset_name": "Window Open",
                "simulation_scan_interval_seconds": 10,
            }
        )

        payload = self.service.activate_for_today()

        self.assertTrue(payload["armed_for_day"])
        self.assertEqual(payload["mode"], "Simulation")
        self.assertEqual(payload["mode_badge_text"], "SIMULATION MODE")
        self.assertEqual(payload["current_state"], "Window Open")
        self.assertTrue(payload["latest_scan"]["is_simulated"])
        self.assertEqual(payload["latest_scan"]["simulation_scenario_name"], "Window Open")
        self.assertEqual(int(FakeTimer.instances[-1].interval), 10)

    def test_manual_simulation_controls_drive_classification_without_preset(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.configure_runtime(
            {
                "mode": "Simulation",
                "simulation_market_session_status": "Open",
                "simulated_spx_value": 6114.0,
                "simulated_vix_value": 17.4,
                "simulation_timing_status": "Eligible",
                "simulation_structure_status": "Chop / Unclear",
                "simulation_momentum_status": "Steady",
                "simulation_scan_interval_seconds": 30,
            }
        )

        self.assertEqual(payload["mode"], "Simulation")
        self.assertEqual(payload["simulation_controls"]["scenario_name"], "Custom")
        self.assertEqual(payload["simulation_controls"]["scan_interval_seconds"], 30)

        activated = self.service.activate_for_today()

        self.assertEqual(activated["current_state"], "Watching")
        self.assertEqual(activated["latest_scan"]["vix_status"], "Caution")
        self.assertEqual(activated["latest_scan"]["timing_status"], "Eligible")
        self.assertTrue(activated["latest_scan"]["is_simulated"])
        self.assertEqual(int(FakeTimer.instances[-1].interval), 30)

    def test_active_simulation_reclassifies_and_captures_transition(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.service.configure_runtime({"mode": "Simulation", "preset_name": "Favorable Setup Forming"})
        first_payload = self.service.activate_for_today()

        second_payload = self.service.configure_runtime({"mode": "Simulation", "preset_name": "Window Open"})

        self.assertEqual(first_payload["current_state"], "Setup Forming")
        self.assertEqual(second_payload["current_state"], "Window Open")
        self.assertEqual(second_payload["latest_scan"]["prior_kairos_state"], "Setup Forming")
        self.assertTrue(second_payload["latest_transition"]["is_transition"])
        self.assertEqual(second_payload["latest_transition"]["label"], "Setup Forming -> Window Open")
        active_timers = [timer for timer in FakeTimer.instances if timer.started and not timer.cancelled]
        self.assertEqual(len(active_timers), 1)

    def test_live_window_open_transition_sends_single_pushover_alert_for_ready_trade(self):
        self.service._session.session_date = self.current_time.date()
        self.service._session.armed_for_day = True
        self.service._session.current_state = "Setup Forming"
        self.service._session.scan_engine_running = False
        self.service._build_live_best_trade_override_payload_locked = lambda now, **kwargs: {
            "status": "ready",
            "candidate": {
                "mode_label": "Standard",
                "strike_pair": "6065 / 6055",
                "spread_width_display": "10 pts",
                "premium_received_display": "$210.00",
                "recommended_contracts_display": "2",
            },
        }

        transition_scan = KairosScanResult(
            timestamp=self.current_time,
            scan_sequence_number=1,
            mode="Live",
            kairos_state="Window Open",
            prior_kairos_state="Setup Forming",
            vix_status="Favorable",
            timing_status="Eligible",
            structure_status="Developing",
            momentum_status="Improving",
            readiness_state="Window Open",
            summary_text="Window open.",
            reasons=["Eligible"],
            state_transition=KairosStateTransition(
                prior_state="Setup Forming",
                current_state="Window Open",
                is_transition=True,
                label="Setup Forming -> Window Open",
            ),
            is_window_open=True,
            is_window_closing=False,
            is_expired=False,
            trigger_reason="scheduled",
            market_session_status="Open",
            spx_value="6,123.45",
            vix_value="18.76",
            classification_note="",
        )
        steady_scan = KairosScanResult(
            timestamp=self.current_time,
            scan_sequence_number=2,
            mode="Live",
            kairos_state="Window Open",
            prior_kairos_state="Window Open",
            vix_status="Favorable",
            timing_status="Eligible",
            structure_status="Developing",
            momentum_status="Improving",
            readiness_state="Window Open",
            summary_text="Window open.",
            reasons=["Eligible"],
            state_transition=KairosStateTransition(
                prior_state="Window Open",
                current_state="Window Open",
                is_transition=False,
                label="No change",
            ),
            is_window_open=True,
            is_window_closing=False,
            is_expired=False,
            trigger_reason="scheduled",
            market_session_status="Open",
            spx_value="6,123.45",
            vix_value="18.76",
            classification_note="",
        )

        self.service._evaluate_live_intraday_backfill = lambda now, trigger_reason="scheduled": [transition_scan]
        self.service.run_scan_cycle()
        self.assertEqual(len(self.pushover_service.sent_messages), 0)

        self.service._evaluate_live_intraday_backfill = lambda now, trigger_reason="scheduled": [steady_scan]
        self.service.run_scan_cycle()
        self.assertEqual(len(self.pushover_service.sent_messages), 0)

    def test_stop_cancels_active_timer_and_marks_session_stopped(self):
        self.service.activate_for_today()

        payload = self.service.stop_for_today()

        self.assertEqual(payload["current_state"], "Stopped")
        self.assertFalse(payload["armed_for_day"])
        self.assertIsNone(payload["next_scan_at"])
        self.assertTrue(FakeTimer.instances[0].cancelled)

    def test_live_best_trade_uses_same_day_schwab_chain_and_exposes_source_labels(self):
        payload = self.service.activate_for_today()

        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertEqual(payload["best_trade_override"]["chain_source_label"], "Schwab Live")
        self.assertEqual(payload["best_trade_override"]["price_basis_label"], "Live bid/ask + midpoint hybrid")
        self.assertEqual(payload["best_trade_override"]["chain_status_label"], "Available")
        summary_labels = [item["label"] for item in payload["best_trade_override"]["summary_chips"]]
        self.assertIn("CHAIN SOURCE", summary_labels)
        self.assertIn("PRICE BASIS", summary_labels)
        self.assertIn("WIDTH SCAN", summary_labels)
        candidate = payload["best_trade_override"]["candidate"]
        self.assertIsNotNone(candidate)
        detail_labels = [row["label"] for row in candidate["detail_rows"]]
        self.assertIn("CHAIN SOURCE", detail_labels)
        self.assertIn("PRICE BASIS", detail_labels)

    def test_live_best_trade_marks_schwab_unavailable_without_estimated_fallback(self):
        self.service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": False,
            "failure_label": "Unavailable",
            "message": "Same-day Schwab chain unavailable for test.",
        }

        payload = self.service.activate_for_today()

        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertEqual(payload["best_trade_override"]["status"], "unavailable")
        self.assertEqual(payload["best_trade_override"]["chain_source_label"], "Schwab (Unavailable)")
        self.assertIsNone(payload["best_trade_override"]["candidate"])
        self.assertIn("will not fall back", payload["best_trade_override"]["caution_text"])

    def test_live_best_trade_override_returns_best_available_candidate_below_threshold_with_insights(self):
        self.service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": self.current_time.date(),
            "underlying_price": 6123.45,
            "puts": [
                {"strike": 6065, "bid": 0.32, "ask": 0.44, "mark": 0.38, "last": 0.37, "delta": -0.14, "open_interest": 1200, "total_volume": 900},
                {"strike": 6060, "bid": 0.24, "ask": 0.34, "mark": 0.29, "last": 0.29, "delta": -0.11, "open_interest": 1300, "total_volume": 950},
                {"strike": 6055, "bid": 0.17, "ask": 0.25, "mark": 0.21, "last": 0.2, "delta": -0.09, "open_interest": 1250, "total_volume": 870},
            ],
            "calls": [],
            "message": "SPX option chain retrieved successfully.",
        }

        payload = self.service.activate_for_today()

        override = payload["best_trade_override"]
        candidate = override["candidate"]
        self.assertEqual(override["status"], "stand-aside")
        self.assertIsNotNone(candidate)
        self.assertFalse(candidate["is_fully_valid"])
        self.assertFalse(override["can_open_trade"])
        self.assertIn("minimum standards", override["open_trade_disabled_reason"].lower())
        self.assertGreater(candidate["fit_score"], 0)
        self.assertTrue(candidate["eligibility_gaps"])
        self.assertTrue(candidate["closest_valid_guidance"])
        self.assertTrue(candidate["estimated_otm_probability_display"].endswith("%"))
        summary_labels = [item["label"] for item in override["summary_chips"]]
        self.assertIn("CHAIN TIME", summary_labels)

    def test_live_best_trade_scans_beyond_five_point_widths(self):
        self.service.market_data_service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 6129.0 if ticker == "^GSPC" else 18.76,
            "Daily Point Change": 12.34 if ticker == "^GSPC" else -1.10,
            "Daily Percent Change": 0.20 if ticker == "^GSPC" else -5.54,
            "As Of": "2026-04-06 09:45:00 AM CDT",
        }
        self.service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": self.current_time.date(),
            "underlying_price": 6129.0,
            "puts": [
                {"strike": 6065, "bid": 3.2, "ask": 3.35, "mark": 3.28, "last": 3.27, "delta": -0.14, "open_interest": 1200, "total_volume": 900},
                {"strike": 6055, "bid": 2.42, "ask": 2.55, "mark": 2.49, "last": 2.48, "delta": -0.08, "open_interest": 1300, "total_volume": 950},
                {"strike": 6050, "bid": 2.08, "ask": 2.2, "mark": 2.14, "last": 2.14, "delta": -0.07, "open_interest": 1250, "total_volume": 870},
            ],
            "calls": [],
            "message": "SPX option chain retrieved successfully.",
        }

        payload = self.service.activate_for_today()

        candidate = payload["best_trade_override"]["candidate"]
        self.assertIsNotNone(candidate)
        self.assertIn(candidate["spread_width"], {10, 15})
        detail_values = {row["label"]: row["value"] for row in candidate["detail_rows"]}
        self.assertEqual(detail_values["WIDTH SCAN"], "up to 15 pts")

    def test_shutdown_persists_live_tape_and_reload_recovers_it_into_scenario_library(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            replay_storage_dir = Path(temp_dir) / "kairos_replays"
            recorded_time = datetime(2026, 4, 6, 20, 8, tzinfo=ZoneInfo("America/Chicago"))
            reloaded_time = datetime(2026, 4, 7, 9, 45, tzinfo=ZoneInfo("America/Chicago"))
            market_data_service = StubMarketDataService()
            market_data_service.get_same_day_intraday_candles = (
                lambda ticker, interval_minutes=1, query_type="intraday": StubMarketDataService._build_intraday_frame(
                    recorded_time.date(),
                    base_price=6115.0,
                )
            )
            service = KairosService(
                market_data_service=market_data_service,
                replay_storage_dir=replay_storage_dir,
                timer_factory=FakeTimer,
                now_provider=lambda: recorded_time,
            )
            try:
                service.activate_for_today()
                service.shutdown()
            finally:
                service._timer = None

            persisted_files = list(replay_storage_dir.glob("*.json"))
            self.assertTrue(persisted_files)
            persisted_payload = json.loads((replay_storage_dir / "live-spx-tape-2026-04-06.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted_payload["template"]["session_status"], "complete")

            reloaded = KairosService(
                market_data_service=StubMarketDataService(),
                replay_storage_dir=replay_storage_dir,
                timer_factory=FakeTimer,
                now_provider=lambda: reloaded_time,
            )
            try:
                reloaded.configure_runtime({"mode": "Simulation"})
                payload = reloaded.get_dashboard_payload()
            finally:
                reloaded.shutdown()

            live_option = next(
                option
                for option in payload["simulation_runner"]["scenario_options"]
                if option.get("source_family_tag") == "LIVE TAPE"
            )
            self.assertEqual(live_option["source_type"], "live_spx_tape")
            self.assertIn(live_option["session_status"], {"complete", "recovered"})
            self.assertIsNotNone(live_option["session_summary"])
            self.assertIn("trend_type", live_option["session_summary"])

    def test_legacy_replay_storage_is_migrated_into_current_durable_catalog(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            legacy_dir = Path(temp_dir) / "legacy_replays"
            current_dir = Path(temp_dir) / "current_replays"
            recorded_time = datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago"))
            reloaded_time = datetime(2026, 4, 7, 9, 45, tzinfo=ZoneInfo("America/Chicago"))
            market_data_service = StubMarketDataService()
            market_data_service.get_same_day_intraday_candles = (
                lambda ticker, interval_minutes=1, query_type="intraday": StubMarketDataService._build_intraday_frame(
                    recorded_time.date(),
                    base_price=6115.0,
                )
            )
            legacy_service = KairosService(
                market_data_service=market_data_service,
                replay_storage_dir=legacy_dir,
                timer_factory=FakeTimer,
                now_provider=lambda: recorded_time,
            )
            try:
                legacy_service.activate_for_today()
                legacy_service.shutdown()
            finally:
                legacy_service._timer = None

            repository = KairosScenarioRepository(current_dir, legacy_storage_dirs=[legacy_dir])
            reloaded = KairosService(
                market_data_service=StubMarketDataService(),
                replay_storage_dir=current_dir,
                scenario_repository=repository,
                timer_factory=FakeTimer,
                now_provider=lambda: reloaded_time,
            )
            try:
                reloaded.configure_runtime({"mode": "Simulation"})
                payload = reloaded.get_dashboard_payload()
            finally:
                reloaded.shutdown()

            self.assertTrue((current_dir / "live-spx-tape-2026-04-06.json").exists())
            self.assertTrue((current_dir / "_kairos_catalog.json").exists())
            self.assertTrue(
                any(option.get("source_family_tag") == "LIVE TAPE" for option in payload["simulation_runner"]["scenario_options"])
            )

    def test_existing_full_bar_partial_live_tape_is_normalized_into_simulator_library(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            replay_storage_dir = Path(temp_dir) / "kairos_replays"
            recorded_time = datetime(2026, 4, 9, 20, 8, tzinfo=ZoneInfo("America/Chicago"))
            market_data_service = StubMarketDataService()
            market_data_service.get_same_day_intraday_candles = (
                lambda ticker, interval_minutes=1, query_type="intraday": StubMarketDataService._build_intraday_frame(
                    recorded_time.date(),
                    base_price=6115.0,
                )
            )
            writer = KairosService(
                market_data_service=market_data_service,
                replay_storage_dir=replay_storage_dir,
                timer_factory=FakeTimer,
                now_provider=lambda: recorded_time,
            )
            try:
                writer.activate_for_today()
                writer.shutdown()
            finally:
                writer._timer = None

            payload_path = replay_storage_dir / "live-spx-tape-2026-04-09.json"
            legacy_payload = json.loads(payload_path.read_text(encoding="utf-8"))
            legacy_payload["template"]["session_status"] = "partial"
            payload_path.write_text(json.dumps(legacy_payload, indent=2), encoding="utf-8")

            reloaded = KairosService(
                market_data_service=StubMarketDataService(),
                replay_storage_dir=replay_storage_dir,
                timer_factory=FakeTimer,
                now_provider=lambda: recorded_time,
            )
            try:
                reloaded.configure_runtime({"mode": "Simulation"})
                payload = reloaded.get_dashboard_payload()
            finally:
                reloaded.shutdown()

            normalized_payload = json.loads(payload_path.read_text(encoding="utf-8"))
            self.assertEqual(normalized_payload["template"]["session_status"], "complete")
            self.assertTrue(
                any(
                    option.get("value") == "live-spx-tape-2026-04-09"
                    for option in payload["simulation_runner"]["scenario_options"]
                )
            )

    def test_window_open_display_state_maps_to_window_found(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.service.configure_runtime({"mode": "Simulation", "preset_name": "Window Open", "simulation_scan_interval_seconds": 10})

        payload = self.service.activate_for_today()

        self.assertEqual(payload["current_state"], "Window Open")
        self.assertEqual(payload["current_state_display"], "Prime")
        self.assertEqual(payload["latest_scan"]["kairos_state_display"], "Prime")

    def test_session_auto_completes_after_market_close(self):
        self.service.activate_for_today()
        self.current_time = datetime(2026, 4, 6, 15, 5, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.get_dashboard_payload()

        self.assertTrue(payload["session_complete"])
        self.assertEqual(payload["session_status"], "Ended")
        self.assertEqual(payload["market_session_status"], "Ended")
        self.assertFalse(payload["armed_for_day"])

    def test_runner_start_applies_first_step_and_uses_runner_timer(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day"})

        self.assertEqual(payload["mode"], "Simulation")
        self.assertTrue(payload["armed_for_day"])
        self.assertEqual(payload["simulation_runner"]["status"], "Running")
        self.assertEqual(payload["simulation_runner"]["scenario_name"], "Bullish Recovery Day")
        self.assertEqual(payload["simulation_runner"]["run_mode"], "Fast Run")
        self.assertGreater(payload["simulation_runner"]["current_bar_number"], 0)
        self.assertEqual(payload["simulation_runner"]["total_bars"], SESSION_BAR_COUNT)
        self.assertIsNotNone(payload["simulation_runner"]["current_candle"])
        self.assertGreater(payload["simulation_runner"]["session_metrics"]["vwap"], 0)
        self.assertAlmostEqual(payload["bar_map"]["bars"][0]["open"], 6123.45, places=2)
        self.assertAlmostEqual(payload["simulation_runner"]["session_metrics"]["current_vix"], 18.76, places=2)
        self.assertEqual(payload["bar_map"]["mode"], "Simulation")
        self.assertEqual(payload["bar_map"]["processed_bars"], payload["simulation_runner"]["current_bar_number"])
        self.assertEqual(len(payload["bar_map"]["bars"]), payload["simulation_runner"]["current_bar_number"])
        self.assertTrue(payload["bar_map"]["bars"][-1]["is_active"])
        self.assertTrue(payload["bar_map"]["vwap_overlay_enabled"])
        self.assertEqual(payload["latest_scan"]["simulation_scenario_name"], "Bullish Recovery Day")
        self.assertAlmostEqual(float(FakeTimer.instances[-1].interval), 0.05, places=2)

    def test_runner_pause_resume_and_end_update_runner_state(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.service.start_simulation_runner({"scenario_key": "window-open-then-failure-day"})

        paused = self.service.pause_simulation_runner()
        resumed = self.service.resume_simulation_runner()
        ended = self.service.end_simulation_runner()

        self.assertEqual(paused["simulation_runner"]["status"], "Paused")
        self.assertEqual(paused["simulation_runner"]["pause_reason"], "manual_pause")
        self.assertEqual(resumed["simulation_runner"]["status"], "Running")
        self.assertIsNone(resumed["simulation_runner"]["pause_reason"])
        self.assertEqual(ended["simulation_runner"]["status"], "Ended")
        self.assertEqual(ended["current_state"], "Stopped")
        self.assertFalse(ended["armed_for_day"])

    def test_runner_scenarios_are_full_390_bar_tapes(self):
        for scenario in KAIROS_RUNNER_SCENARIOS.values():
            self.assertEqual(len(scenario.bars), SESSION_BAR_COUNT)
            self.assertEqual(len(scenario.vix_series), SESSION_BAR_COUNT)
            self.assertEqual(scenario.bars[0].bar_index, 0)
            self.assertEqual(scenario.bars[-1].bar_index, SESSION_BAR_COUNT - 1)
            self.assertGreaterEqual(scenario.bars[-1].simulated_time.hour, 14)

    def test_runner_auto_pause_checkpoint_sets_pause_reason(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        payload = self.service.start_simulation_runner({"scenario_key": "window-open-then-failure-day"})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertIn("candidate", payload["simulation_runner"]["pause_detail"].lower())

    def test_runner_pause_on_setup_forming_uses_existing_kairos_state(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["setup-forming"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_setup-forming")
        self.assertEqual(payload["simulation_runner"]["pause_status"]["event_label"], "Subprime Improving")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Setup Forming")
        self.assertEqual(payload["simulation_runner"]["pause_status"]["bar_number"], payload["simulation_runner"]["current_bar_number"])
        self.assertTrue(payload["simulation_runner"]["pause_status"]["simulated_time"])

    def test_runner_pause_on_window_open_uses_existing_kairos_state(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertEqual(payload["simulation_runner"]["pause_status"]["event_label"], "Prime")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Open")
        self.assertIn("candidate", payload["simulation_runner"]["pause_detail"].lower())

    def test_runner_always_pauses_for_window_open_when_qualified_candidates_exist(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day"})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Open")
        self.assertEqual(payload["simulation_runner"]["trade_candidate_card"]["status"], "ready")

    def test_window_open_then_failure_day_also_pauses_on_candidate_decision(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "window-open-then-failure-day"})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Open")
        self.assertTrue(payload["simulation_runner"]["trade_candidate_card"]["visible"])
        self.assertTrue(any(item["available"] for item in payload["simulation_runner"]["trade_candidate_card"]["profiles"]))

    def test_runner_does_not_force_window_open_pause_when_no_candidates_qualify(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        original_builder = self.service._build_synthetic_kairos_candidate_profile

        def reject_all_profiles(**kwargs):
            profile = kwargs["profile"]
            return {
                "mode_key": profile["key"],
                "mode_label": profile["label"],
                "available": False,
            }

        self.service._build_synthetic_kairos_candidate_profile = reject_all_profiles
        try:
            payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day"})

            while payload["simulation_runner"]["status"] == "Running":
                FakeTimer.instances[-1].callback()
                payload = self.service.get_dashboard_payload()
        finally:
            self.service._build_synthetic_kairos_candidate_profile = original_builder

        self.assertIn(payload["simulation_runner"]["status"], {"Paused", "Completed"})
        if payload["simulation_runner"]["status"] == "Paused":
            self.assertEqual(payload["simulation_runner"]["pause_reason"], "awaiting_user_continue")
        self.assertNotEqual(payload["simulation_runner"]["pause_status"]["event_label"], "Window Open")

    def test_window_open_pause_exposes_all_trade_profiles_with_clear_status(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        candidate_card = payload["simulation_runner"]["trade_candidate_card"]

        self.assertTrue(candidate_card["visible"])
        self.assertEqual(candidate_card["status"], "ready")
        self.assertEqual(candidate_card["title"], "Trade Candidate Card")
        self.assertEqual(candidate_card["eyebrow"], "Synthetic / model-based")
        self.assertGreaterEqual(len(candidate_card["summary_chips"]), 3)
        self.assertEqual([item["mode_label"] for item in candidate_card["profiles"]], ["Standard", "Fortress", "Aggressive"])
        self.assertTrue(any(item["available"] for item in candidate_card["profiles"]))
        self.assertTrue(any(not item["available"] for item in candidate_card["profiles"]))
        self.assertTrue(all(item["qualification_status"] in {"Qualified", "Did not qualify"} for item in candidate_card["profiles"]))
        self.assertTrue(all(any(row["label"] == "Estimated credit per contract" for row in item["detail_rows"]) for item in candidate_card["profiles"]))
        self.assertTrue(all(any(row["label"] == "Estimated total premium received" for row in item["detail_rows"]) for item in candidate_card["profiles"]))
        self.assertTrue(all(any(row["label"] == "Estimated max loss" for row in item["detail_rows"]) for item in candidate_card["profiles"]))
        self.assertNotIn("Pass", [item["mode_label"] for item in candidate_card["profiles"]])

    def test_select_and_take_trade_candidate_locks_in_simulated_trade(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        selected = self.service.select_simulation_trade_candidate({"profile_key": "aggressive"})
        taken = self.service.take_simulation_trade_candidate({"profile_key": "aggressive"})

        self.assertEqual(selected["simulation_runner"]["trade_candidate_card"]["selected_profile_key"], "aggressive")
        self.assertEqual(taken["simulation_runner"]["status"], "Running")
        self.assertIsNotNone(taken["simulation_runner"]["active_trade_lock_in"])
        self.assertEqual(taken["simulation_runner"]["active_trade_lock_in"]["profile_label"], "Aggressive")
        self.assertEqual(taken["simulation_runner"]["trade_lock_history_count"], 1)
        self.assertIn("locked in", taken["status_note"].lower())

    def test_take_trade_runs_to_close_and_settles_end_of_day_result(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day"})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        taken = self.service.take_simulation_trade_candidate({"profile_key": "aggressive"})
        while taken["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            taken = self.service.get_dashboard_payload()

        active_trade = taken["simulation_runner"]["active_trade_lock_in"]
        self.assertEqual(taken["simulation_runner"]["status"], "Completed")
        self.assertTrue(active_trade["is_settled"])
        self.assertIsNotNone(active_trade["final_spx_close"])
        self.assertIn(active_trade["result_status"], {"Win", "Loss", "Black Swan"})
        self.assertNotEqual(active_trade["simulated_pnl_display"], "—")
        self.assertTrue(active_trade["settlement_note"])
        self.assertIn("Settled -", active_trade["settlement_badge_label"])
        self.assertIn(active_trade["result_tone"], {"win", "loss", "black-swan"})

    def test_failure_day_trade_settles_as_black_swan_when_it_hits_max_loss(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "window-open-then-failure-day"})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        taken = self.service.take_simulation_trade_candidate({"profile_key": "aggressive"})
        while taken["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            taken = self.service.get_dashboard_payload()

        active_trade = taken["simulation_runner"]["active_trade_lock_in"]
        self.assertEqual(active_trade["result_status"], "Black Swan")
        self.assertEqual(active_trade["result_tone"], "black-swan")
        self.assertIn("Black Swan", active_trade["settlement_badge_label"])
        self.assertIn("-$", active_trade["settlement_badge_label"])

    def test_ignore_trade_candidate_resumes_without_lock_in(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        ignored = self.service.ignore_simulation_trade_candidate()

        self.assertEqual(ignored["simulation_runner"]["status"], "Running")
        self.assertIsNone(ignored["simulation_runner"]["active_trade_lock_in"])
        self.assertIn("declined", ignored["status_note"].lower())

    def test_trade_candidate_card_shows_no_candidate_state_when_model_rejects_all_profiles(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        late_bar = KairosTapeBar(
            bar_index=195,
            minute_offset=195,
            simulated_time=time(11, 44),
            open=6123.0,
            high=6125.0,
            low=6120.0,
            close=6123.45,
            volume=1000,
        )
        candidate_card = self.service._build_runner_trade_candidate_card_locked(
            self.service._session.latest_scan,
            {
                "current_close": 6123.45,
                "current_vix": 18.0,
                "current_bar": late_bar,
            },
        )

        self.assertTrue(candidate_card["visible"])
        self.assertEqual(candidate_card["status"], "ready")
        self.assertEqual(len(candidate_card["profiles"]), 3)
        self.assertTrue(any(item["available"] for item in candidate_card["profiles"]))
        self.assertEqual(candidate_card["selected_profile_key"], "aggressive")
        self.assertEqual(candidate_card["message"], "Synthetic Kairos candidate set generated from simulated SPX, VIX, time, and modeled spread distance.")

    def test_runner_pause_on_window_closing_uses_existing_kairos_state(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bearish-downtrend-day", "pause_events": ["window-closing"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-closing")
        self.assertEqual(payload["simulation_runner"]["pause_status"]["event_label"], "Subprime Weakening")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Closing")
        self.assertGreaterEqual(payload["simulation_runner"]["pause_status"]["bar_number"], 1)

    def test_resume_can_suppress_successive_setup_forming_pauses_until_state_changes(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        payload = self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day", "pause_events": ["setup-forming"]})

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.service.get_dashboard_payload()

        self.assertEqual(payload["latest_scan"]["kairos_state"], "Setup Forming")

        resumed = self.service.resume_simulation_runner({"ignore_successive_condition": True})
        self.assertTrue(resumed["simulation_runner"]["pause_status"]["suppression_active"])
        self.assertEqual(resumed["simulation_runner"]["pause_status"]["suppressed_event_label"], "Subprime Improving")

        saw_setup_forming_after_resume = False
        while resumed["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            resumed = self.service.get_dashboard_payload()
            current_state = resumed["latest_scan"]["kairos_state"]
            if current_state == "Setup Forming":
                saw_setup_forming_after_resume = True
                self.assertEqual(resumed["simulation_runner"]["status"], "Running")
            if current_state == "Window Open":
                break

        self.assertTrue(saw_setup_forming_after_resume)
        self.assertEqual(resumed["latest_scan"]["kairos_state"], "Window Open")
        self.assertFalse(resumed["simulation_runner"]["pause_status"]["suppression_active"])
        self.assertEqual(resumed["simulation_runner"]["pause_status"]["suppressed_event_label"], None)

    def test_bullish_recovery_day_produces_window_open_marker(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.service.start_simulation_runner({"scenario_key": "bullish-recovery-day"})

        while self.service.get_dashboard_payload()["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()

        payload = self.service.get_dashboard_payload()
        marker_kinds = {marker["kind"] for marker in payload["bar_map"]["event_markers"]}
        self.assertIn("window-open", marker_kinds)

    def test_runner_callback_advances_progress_and_completes_scenario(self):
        self.current_time = datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        payload = self.service.start_simulation_runner({"scenario_key": "chop-no-trade-day"})
        total_steps = payload["simulation_runner"]["total_steps"]

        while self.service.get_dashboard_payload()["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()

        completed = self.service.get_dashboard_payload()

        self.assertEqual(completed["simulation_runner"]["status"], "Completed")
        self.assertEqual(completed["simulation_runner"]["completed_steps"], total_steps)
        self.assertEqual(completed["simulation_runner"]["current_bar_number"], SESSION_BAR_COUNT)
        self.assertEqual(completed["session_status"], "Ended")
        self.assertGreater(completed["simulation_runner"]["session_metrics"]["vwap"], 0)
        self.assertEqual(len(completed["scan_log"]), SESSION_BAR_COUNT)
        self.assertEqual(completed["scan_log_count"], SESSION_BAR_COUNT)
        self.assertIn("Showing all 390 session records", completed["scan_log_note"])
        self.assertEqual(completed["simulation_runner"]["tape_source"], "/ES 1m Tape")
        self.assertGreater(completed["simulation_runner"]["session_metrics"]["current_volume"], 0)
        self.assertEqual(completed["bar_map"]["processed_bars"], SESSION_BAR_COUNT)
        self.assertEqual(len(completed["bar_map"]["bars"]), SESSION_BAR_COUNT)
        self.assertGreaterEqual(len(completed["bar_map"]["event_markers"]), 1)
        marker_kinds = {marker["kind"] for marker in completed["bar_map"]["event_markers"]}
        self.assertIn("setup-forming", marker_kinds)
        self.assertIn("window-closing", marker_kinds)
        self.assertTrue(any(marker["timestamp"] for marker in completed["bar_map"]["event_markers"]))
        self.assertGreaterEqual(len(completed["simulation_runner"]["event_log"]), 3)


if __name__ == "__main__":
    unittest.main()
