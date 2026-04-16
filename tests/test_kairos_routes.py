import tempfile
import unittest
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from app import create_app
from services import MarketDataError


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


class KairosRoutesTest(unittest.TestCase):
    def setUp(self):
        FakeTimer.instances = []
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "horme_trades.db"
        self.replay_storage_path = Path(self.temp_dir.name) / "kairos_replays"
        self.app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(self.database_path),
                "KAIROS_REPLAY_STORAGE_DIR": str(self.replay_storage_path),
            }
        )
        self.client = self.app.test_client()
        self.service = self.app.extensions["market_data_service"]
        self.service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 6123.45 if ticker == "^GSPC" else 18.76,
            "Daily Point Change": 10.0 if ticker == "^GSPC" else -1.0,
            "Daily Percent Change": 0.16 if ticker == "^GSPC" else -5.06,
            "As Of": "2026-04-06 09:45:00 AM CDT",
        }
        self.service.get_provider_metadata = lambda: {
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
            "provider_name": "Schwab",
            "live_provider_key": "schwab",
            "spx_historical_provider_name": "Schwab",
            "vix_historical_provider_name": "Schwab",
        }
        self.service.get_same_day_intraday_candles = lambda ticker, interval_minutes=1, query_type="intraday": pd.DataFrame(
            self._build_regular_session_rows(
                datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago")).date(),
                base_price=6120.0,
                include_extended_hours=False,
                step=0.08,
            )
        )
        self.service.get_intraday_candles_for_date = lambda ticker, target_date, interval_minutes=1, query_type="intraday-date": pd.DataFrame(
            self._build_regular_session_rows(
                target_date,
                base_price=6120.0,
                include_extended_hours=False,
                step=0.08,
            )
        )
        self.kairos_service = self.app.extensions["kairos_service"]
        self.kairos_live_service = self.app.extensions["kairos_live_service"]
        self.kairos_sim_service = self.app.extensions["kairos_sim_service"]
        for service in {self.kairos_live_service, self.kairos_sim_service}:
            service.now_provider = lambda: datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago"))
            service.timer_factory = FakeTimer
            service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date, _self=self: _self._build_live_option_chain_summary()

    def tearDown(self):
        self.kairos_live_service.shutdown()
        self.kairos_sim_service.shutdown()
        self.temp_dir.cleanup()

    def _build_live_option_chain_summary(self):
        return {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago")).date(),
            "underlying_price": 6123.45,
            "puts": [
                {"strike": 6120, "bid": 9.8, "ask": 10.2, "mark": 10.0, "last": 9.98, "delta": -0.48, "open_interest": 2400, "total_volume": 1800},
                {"strike": 6115, "bid": 7.85, "ask": 8.15, "mark": 8.0, "last": 7.99, "delta": -0.43, "open_interest": 2200, "total_volume": 1640},
                {"strike": 6110, "bid": 6.0, "ask": 6.25, "mark": 6.12, "last": 6.11, "delta": -0.38, "open_interest": 2100, "total_volume": 1500},
                {"strike": 6065, "bid": 2.85, "ask": 3.05, "mark": 2.95, "last": 2.94, "delta": -0.14, "open_interest": 1200, "total_volume": 900},
                {"strike": 6060, "bid": 2.6, "ask": 2.8, "mark": 2.7, "last": 2.69, "delta": -0.12, "open_interest": 1300, "total_volume": 950},
                {"strike": 6055, "bid": 1.8, "ask": 1.9, "mark": 1.85, "last": 1.84, "delta": -0.10, "open_interest": 1250, "total_volume": 870},
                {"strike": 6050, "bid": 1.05, "ask": 1.2, "mark": 1.12, "last": 1.11, "delta": -0.09, "open_interest": 1210, "total_volume": 860},
                {"strike": 6045, "bid": 0.55, "ask": 0.68, "mark": 0.61, "last": 0.6, "delta": -0.05, "open_interest": 1180, "total_volume": 830},
                {"strike": 6040, "bid": 0.2, "ask": 0.28, "mark": 0.24, "last": 0.24, "delta": -0.04, "open_interest": 1100, "total_volume": 790},
            ],
            "calls": [
                {"strike": 6125, "bid": 10.2, "ask": 10.6, "mark": 10.4, "last": 10.39, "delta": 0.51, "open_interest": 2450, "total_volume": 1750},
                {"strike": 6130, "bid": 8.1, "ask": 8.45, "mark": 8.27, "last": 8.26, "delta": 0.46, "open_interest": 2230, "total_volume": 1605},
                {"strike": 6135, "bid": 6.15, "ask": 6.45, "mark": 6.3, "last": 6.29, "delta": 0.4, "open_interest": 2120, "total_volume": 1490},
            ],
            "message": "SPX option chain retrieved successfully.",
        }

    def _build_regular_session_rows(self, session_date, *, base_price=510.0, include_extended_hours=False, step=0.05):
        session_anchor = datetime(session_date.year, session_date.month, session_date.day, 8, 30, tzinfo=ZoneInfo("America/Chicago"))
        rows = []

        if include_extended_hours:
            rows.append(
                {
                    "Datetime": session_anchor.replace(hour=7, minute=59),
                    "Open": round(base_price - 1.0, 2),
                    "High": round(base_price - 0.8, 2),
                    "Low": round(base_price - 1.2, 2),
                    "Close": round(base_price - 0.9, 2),
                    "Volume": 500,
                }
            )

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

        if include_extended_hours:
            rows.append(
                {
                    "Datetime": session_anchor.replace(hour=15, minute=1),
                    "Open": round(base_price + 25.0, 2),
                    "High": round(base_price + 25.2, 2),
                    "Low": round(base_price + 24.8, 2),
                    "Close": round(base_price + 25.1, 2),
                    "Volume": 700,
                }
            )

        return rows

    def _build_polygon_payload(self, session_date, *, base_price=520.0, include_extended_hours=False, step=0.05):
        rows = self._build_regular_session_rows(
            session_date,
            base_price=base_price,
            include_extended_hours=include_extended_hours,
            step=step,
        )
        return {
            "results": [
                {
                    "t": int(row["Datetime"].timestamp() * 1000),
                    "o": row["Open"],
                    "h": row["High"],
                    "l": row["Low"],
                    "c": row["Close"],
                    "v": row["Volume"],
                }
                for row in rows
            ]
        }

    def _advance_sim_until_not_running(self, payload):
        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.client.get("/kairos/sim/status").get_json()
        return payload

    def _advance_sim_to_completion(self, payload, *, exit_gate_action="skip"):
        while payload["simulation_runner"]["status"] != "Completed":
            if payload["simulation_runner"]["status"] == "Running":
                FakeTimer.instances[-1].callback()
                payload = self.client.get("/kairos/sim/status").get_json()
                continue
            if payload["simulation_runner"]["pause_reason"] == "pause_on_exit-gate":
                route = "/kairos/sim/runner/exit/accept" if exit_gate_action == "accept" else "/kairos/sim/runner/exit/skip"
                payload = self.client.post(route).get_json()
                continue
            raise AssertionError(f"Unexpected pause while advancing simulation: {payload['simulation_runner']['pause_reason']}")
        return payload

    def test_kairos_root_redirects_to_live_workspace(self):
        response = self.client.get("/kairos")

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/kairos/live"))

    def test_kairos_live_page_renders_operator_console_sections(self):
        response = self.client.get("/kairos/live")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Kairos Live", response.data)
        self.assertIn(b"The God of the Right Moment", response.data)
        self.assertIn(b"Kairos Live", response.data)
        self.assertIn(b"Kairos Sim", response.data)
        self.assertIn(b"Kairos Credit Map", response.data)
        self.assertIn(b"Kairos Candidates", response.data)
        self.assertNotIn(b"Live Workspace Control", response.data)
        self.assertNotIn(b"Live Watchtower Status", response.data)
        self.assertIn(b"Kairos Session Tape", response.data)
        self.assertNotIn(b"KAIROS Scan Summary".replace(b"KAIROS", b"Kairos"), response.data)
        self.assertIn(b"Bars processed", response.data)
        self.assertIn(b"Latest candle time", response.data)
        self.assertIn(b"Tape source", response.data)
        self.assertIn(b"Subprime Improving", response.data)
        self.assertIn(b"Prime", response.data)
        self.assertIn(b"Subprime Weakening", response.data)
        self.assertIn(b"Not Eligible / Expired", response.data)
        self.assertIn(b"kairos-bar-map-axis", response.data)
        self.assertIn(b"kairos-bar-map-scroll", response.data)
        self.assertIn(b"kairos-bar-map-svg-wrap", response.data)
        self.assertIn(b"kairosBarMapAutoFollow", response.data)
        self.assertIn(b"visibleBarTarget = 144", response.data)
        self.assertIn(b"sessionEnvelopeInitialRangePoints = 20", response.data)
        self.assertIn(b"sessionEnvelopeMarginPoints = 10", response.data)
        self.assertIn(b"buildBarMapEnvelope", response.data)
        self.assertIn(b"kairos-bar-map-price-label", response.data)
        self.assertIn(b"kairos-watchtower-metrics", response.data)
        self.assertIn(b"kairos-live-credit-map-shell", response.data)
        self.assertIn(b"kairos-live-candidate-cards", response.data)
        self.assertIn(b"Structure", response.data)
        self.assertIn(b"Countdown", response.data)
        self.assertIn(b"Total Scans Today", response.data)
        self.assertIn(b"Scan Interval", response.data)
        self.assertIn(b"Last Scan", response.data)
        self.assertIn(b"Intraday Scan Log", response.data)
        self.assertIn(b"Distance to Short", response.data)
        self.assertIn(b"EM Multiple", response.data)
        self.assertIn(b"kairos-live-trade-lock-panel", response.data)
        self.assertIn(b"kairos-live-exit-prompt-panel", response.data)
        self.assertNotIn(b"Current session status", response.data)
        self.assertNotIn(b"kairos-live-box-panel", response.data)
        self.assertNotIn(b"kairos-summary-text", response.data)
        self.assertNotIn(b"kairos-classification-note", response.data)
        self.assertLess(response.data.index(b"Kairos Session Tape"), response.data.index(b"Kairos Credit Map"))
        self.assertLess(response.data.index(b"Kairos Credit Map"), response.data.index(b"Kairos Candidates"))
        self.assertLess(response.data.index(b"Kairos Candidates"), response.data.index(b"kairos-live-trade-monitor-panel"))
        self.assertLess(response.data.index(b"kairos-live-trade-monitor-panel"), response.data.index(b"Intraday Scan Log"))
        self.assertLess(response.data.index(b"Kairos Candidates"), response.data.index(b"Intraday Scan Log"))
        self.assertIn(b"apollo-collapsible-summary", response.data)
        self.assertNotIn(b"Activate Kairos for Today", response.data)
        self.assertIn(b"SPX", response.data)
        self.assertIn(b"VIX", response.data)
        self.assertNotIn(b"Full-Day Progression Simulator", response.data)
        self.assertNotIn(b"Advanced simulation inputs", response.data)
        self.assertNotIn(b'id="kairos-runner-start-button"', response.data)
        self.assertNotIn(b'id="kairos-runner-candidate-card"', response.data)
        self.assertNotIn(b'id="kairos-runner-trade-lock-panel"', response.data)
        self.assertNotIn(b'id="kairos-apply-simulation-button"', response.data)
        self.assertNotIn(b"Main Menu", response.data)

    def test_live_page_auto_initializes_workspace_without_manual_activation(self):
        response = self.client.get("/kairos/live")
        payload = self.client.get("/kairos/live/status").get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["armed_for_day"])
        self.assertEqual(payload["session_status"], "Armed")
        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertNotIn(b"Activate Kairos for Today", response.data)

    def test_live_page_auto_initializes_closed_market_state_without_manual_activation(self):
        self.kairos_live_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        response = self.client.get("/kairos/live")
        payload = self.client.get("/kairos/live/status").get_json()

        self.assertEqual(response.status_code, 200)
        self.assertFalse(payload["armed_for_day"])
        self.assertEqual(payload["current_state_display"], "Market Closed")
        self.assertEqual(payload["best_trade_override"]["status"], "market-closed")
        self.assertIn("standing by", payload["status_note"].lower())

    def test_kairos_sim_page_renders_lab_sections(self):
        response = self.client.get("/kairos/sim")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Kairos Sim", response.data)
        self.assertIn(b"Historical SPY Replay Import", response.data)
        self.assertIn(b"Full-Day Progression Simulator", response.data)
        self.assertIn(b"Kairos Session Tape Lab", response.data)
        self.assertIn(b"Simulation Scan Log", response.data)
        self.assertIn(b"Pause on Subprime Improving", response.data)
        self.assertIn(b"Pause on Prime", response.data)
        self.assertIn(b"Pause on Subprime Weakening", response.data)
        self.assertIn(b"Trade Candidate Card", response.data)
        self.assertIn(b"Kairos Exit Status", response.data)
        self.assertIn(b"kairos-runner-trade-lock-panel", response.data)
        self.assertIn(b'id="kairos-runner-best-trade-button"', response.data)
        self.assertIn(b"kairos-log-disclosure", response.data)
        self.assertIn(b"apollo-collapsible-summary", response.data)
        self.assertIn(b"kairosRunnerSelectedScenario", response.data)
        self.assertIn(b"syncRunnerScenarioSelection", response.data)
        self.assertIn(b"SIMULATION MODE", response.data)
        self.assertIn(b"visibleBarTarget = 144", response.data)
        self.assertNotIn(b"Simulation Control Deck", response.data)
        self.assertNotIn(b"Simulation Watchtower Status", response.data)
        self.assertNotIn(b"Kairos Simulation Summary", response.data)
        self.assertNotIn(b"Kairos Simulation Lifecycle", response.data)
        self.assertNotIn(b"Runner event log", response.data)
        self.assertNotIn(b"Advanced simulation inputs", response.data)
        self.assertNotIn(b"Polygon / Massive JSON fallback", response.data)
        self.assertNotIn(b'id="kairos-apply-simulation-button"', response.data)
        self.assertNotIn(b'data-kairos-preset', response.data)
        self.assertNotIn(b"renderRunnerLog", response.data)
        self.assertNotIn(b"Daily Simulation Tapes", response.data)
        self.assertNotIn(b"Capture Today Into Tape Library", response.data)
        self.assertNotIn(b"kairos-replay-library-list", response.data)

    def test_kairos_sim_page_lists_auto_recorded_live_tape_in_runner_menu(self):
        self.kairos_live_service.now_provider = lambda: datetime(2026, 4, 6, 20, 8, tzinfo=ZoneInfo("America/Chicago"))
        self.kairos_live_service.activate_for_today()
        self.kairos_live_service.shutdown()

        response = self.client.get("/kairos/sim")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Live SPX Tape 2026-04-06", response.data)
        self.assertIn(b'value="live-spx-tape-2026-04-06"', response.data)

    def test_kairos_sim_capture_today_route_is_removed(self):
        response = self.client.post("/kairos/sim/replay/capture-today")

        self.assertEqual(response.status_code, 404)

    def test_live_and_sim_workspaces_hold_independent_state(self):
        weekend_now = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.kairos_live_service.now_provider = weekend_now
        self.kairos_sim_service.now_provider = weekend_now

        live_activate = self.client.post("/kairos/live/activate")
        sim_configure = self.client.post(
            "/kairos/sim/configure",
            json={"preset_name": "Window Open", "simulation_scan_interval_seconds": 10},
        )
        sim_activate = self.client.post("/kairos/sim/activate")
        live_status = self.client.get("/kairos/live/status")
        sim_status = self.client.get("/kairos/sim/status")

        self.assertEqual(live_activate.status_code, 200)
        self.assertEqual(sim_configure.status_code, 200)
        self.assertEqual(sim_activate.status_code, 200)
        self.assertEqual(live_status.status_code, 200)
        self.assertEqual(sim_status.status_code, 200)

        live_payload = live_status.get_json()
        sim_payload = sim_status.get_json()
        self.assertEqual(live_payload["mode"], "Live")
        self.assertFalse(live_payload["armed_for_day"])
        self.assertEqual(sim_payload["mode"], "Simulation")
        self.assertTrue(sim_payload["armed_for_day"])
        self.assertEqual(sim_payload["current_state"], "Window Open")

    def test_live_mode_respects_closed_market_and_simulation_allows_weekend_activation(self):
        self.kairos_live_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        live_response = self.client.post("/kairos/activate")
        live_payload = live_response.get_json()
        self.assertEqual(live_response.status_code, 200)
        self.assertFalse(live_payload["armed_for_day"])
        self.assertIn("Market closed", live_payload["status_note"])

        configure_response = self.client.post(
            "/kairos/configure",
            json={"mode": "Simulation", "preset_name": "Window Open", "simulation_scan_interval_seconds": 10},
        )
        configure_payload = configure_response.get_json()
        self.assertEqual(configure_response.status_code, 200)
        self.assertEqual(configure_payload["mode"], "Simulation")
        self.assertEqual(configure_payload["mode_badge_text"], "SIMULATION MODE")

        activate_response = self.client.post("/kairos/activate")
        activate_payload = activate_response.get_json()
        self.assertEqual(activate_response.status_code, 200)
        self.assertTrue(activate_payload["armed_for_day"])
        self.assertEqual(activate_payload["current_state"], "Window Open")
        self.assertTrue(activate_payload["latest_scan"]["is_simulated"])

        status_response = self.client.get("/kairos/status")
        status_payload = status_response.get_json()
        self.assertEqual(status_response.status_code, 200)
        self.assertEqual(status_payload["mode"], "Simulation")
        self.assertEqual(status_payload["total_scans_completed"], 1)

    def test_active_simulation_configure_updates_transition(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        self.client.post("/kairos/sim/configure", json={"preset_name": "Favorable Setup Forming"})
        self.client.post("/kairos/sim/activate")

        response = self.client.post("/kairos/sim/configure", json={"preset_name": "Window Open"})
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["current_state"], "Window Open")
        self.assertEqual(payload["latest_scan"]["prior_kairos_state"], "Setup Forming")
        self.assertTrue(payload["latest_transition"]["is_transition"])
        self.assertEqual(payload["latest_transition"]["label"], "Setup Forming -> Window Open")

    def test_manual_simulation_configure_updates_scan_inputs(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        configure_response = self.client.post(
            "/kairos/sim/configure",
            json={
                "simulation_market_session_status": "Open",
                "simulated_spx_value": 6110.0,
                "simulated_vix_value": 17.5,
                "simulation_timing_status": "Eligible",
                "simulation_structure_status": "Chop / Unclear",
                "simulation_momentum_status": "Steady",
                "simulation_scan_interval_seconds": 30,
            },
        )
        configure_payload = configure_response.get_json()

        self.assertEqual(configure_response.status_code, 200)
        self.assertEqual(configure_payload["mode"], "Simulation")
        self.assertEqual(configure_payload["simulation_controls"]["scenario_name"], "Custom")

        activate_response = self.client.post("/kairos/sim/activate")
        activate_payload = activate_response.get_json()

        self.assertEqual(activate_response.status_code, 200)
        self.assertEqual(activate_payload["current_state"], "Watching")
        self.assertEqual(activate_payload["latest_scan"]["vix_status"], "Caution")
        self.assertEqual(activate_payload["scan_interval_seconds"], 30)
        self.assertTrue(activate_payload["latest_scan"]["is_simulated"])

    def test_runner_routes_start_pause_resume_restart_and_end(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post("/kairos/sim/runner/start", json={"scenario_key": "bullish-recovery-day"})
        start_payload = start_response.get_json()
        self.assertEqual(start_response.status_code, 200)
        self.assertEqual(start_payload["simulation_runner"]["status"], "Running")
        self.assertEqual(start_payload["mode"], "Simulation")
        self.assertTrue(start_payload["armed_for_day"])
        self.assertGreater(start_payload["simulation_runner"]["current_bar_number"], 0)
        self.assertEqual(start_payload["simulation_runner"]["run_mode"], "Fast Run")

        pause_response = self.client.post("/kairos/sim/runner/pause")
        pause_payload = pause_response.get_json()
        self.assertEqual(pause_response.status_code, 200)
        self.assertEqual(pause_payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(pause_payload["simulation_runner"]["pause_reason"], "manual_pause")

        resume_response = self.client.post("/kairos/sim/runner/resume")
        resume_payload = resume_response.get_json()
        self.assertEqual(resume_response.status_code, 200)
        self.assertEqual(resume_payload["simulation_runner"]["status"], "Running")

        restart_response = self.client.post("/kairos/sim/runner/restart", json={"scenario_key": "window-open-then-failure-day"})
        restart_payload = restart_response.get_json()
        self.assertEqual(restart_response.status_code, 200)
        self.assertEqual(restart_payload["simulation_runner"]["scenario_name"], "Window Open Then Failure Day")
        self.assertEqual(restart_payload["simulation_runner"]["total_bars"], 390)

        end_response = self.client.post("/kairos/sim/runner/end")
        end_payload = end_response.get_json()
        self.assertEqual(end_response.status_code, 200)
        self.assertEqual(end_payload["simulation_runner"]["status"], "Ended")
        self.assertEqual(end_payload["current_state"], "Stopped")

    def test_sim_historical_replay_import_from_schwab_adds_runner_scenario_without_affecting_live(self):
        session_date = datetime(2026, 4, 1, 10, 0, tzinfo=ZoneInfo("America/Chicago")).date()
        spx_open = 5780.0

        def fake_intraday_candles(symbol, target_date, interval_minutes=1, query_type=None):
            self.assertEqual(target_date, session_date)
            self.assertEqual(interval_minutes, 1)
            if symbol == "SPY":
                return pd.DataFrame(
                    self._build_regular_session_rows(
                        session_date,
                        base_price=512.0,
                        include_extended_hours=True,
                        step=-0.11,
                    )
                )
            if symbol == "^VIX":
                raise MarketDataError("No intraday ^VIX available for test")
            raise AssertionError(f"Unexpected symbol {symbol}")

        self.service.get_intraday_candles_for_date = fake_intraday_candles
        self.service.get_single_day_bar = lambda ticker, target_date, query_type="single_close": {"Open": spx_open, "Close": 5660.0}

        import_response = self.client.post(
            "/kairos/sim/replay/import",
            json={"session_date": session_date.isoformat(), "source": "schwab"},
        )
        import_payload = import_response.get_json()

        self.assertEqual(import_response.status_code, 200)
        self.assertEqual(import_payload["mode"], "Simulation")
        self.assertIn("Historical replay ready", import_payload["status_note"])

        historical_replay = import_payload["simulation_controls"]["historical_replay"]
        imported = next(
            item
            for item in historical_replay["imported_scenarios"]
            if item["session_date"] == session_date.isoformat() and item["source"] == "Schwab historical 1m"
        )
        self.assertIsNotNone(imported)
        self.assertEqual(imported["session_date"], session_date.isoformat())
        imported_template = historical_replay["last_imported"] if historical_replay.get("last_imported", {}).get("scenario_key") == imported["scenario_key"] else self.kairos_sim_service._historical_replay_templates[imported["scenario_key"]].to_payload()
        self.assertEqual(imported_template["source_label"], "Schwab historical 1m")
        self.assertEqual(imported_template["tape_source"], "Synthetic SPX rebased from SPY 1m")
        self.assertEqual(imported_template["vix_source_label"], "Flat VIX fallback")

        scenario = self.kairos_sim_service._historical_replay_scenarios[imported["scenario_key"]]
        self.assertEqual(scenario.bars[0].open, spx_open)
        self.assertGreater(scenario.bars[0].open, 5000)
        self.assertLess(scenario.bars[-1].close, scenario.bars[0].open)
        self.assertGreater(abs(scenario.bars[-1].close - scenario.bars[0].open), 400)

        imported_option = next(
            option
            for option in import_payload["simulation_runner"]["scenario_options"]
            if option["value"] == imported["scenario_key"]
        )
        self.assertTrue(imported_option["is_imported"])
        self.assertEqual(imported_option["source_label"], "Schwab historical 1m")

        live_status = self.client.get("/kairos/live/status")
        live_payload = live_status.get_json()
        self.assertEqual(live_status.status_code, 200)
        self.assertFalse(
            any(
                option["value"] == imported["scenario_key"]
                for option in live_payload["simulation_runner"]["scenario_options"]
            )
        )

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": imported["scenario_key"]},
        )
        start_payload = start_response.get_json()

        self.assertEqual(start_response.status_code, 200)
        self.assertEqual(start_payload["simulation_runner"]["scenario_key"], imported["scenario_key"])
        self.assertEqual(start_payload["simulation_runner"]["scenario_name"], "SPY Real Day 2026-04-01")
        self.assertEqual(start_payload["simulation_runner"]["tape_source"], "Synthetic SPX rebased from SPY 1m")
        self.assertEqual(start_payload["simulation_runner"]["total_bars"], 390)
        self.assertEqual(start_payload["simulation_runner"]["status"], "Running")
        self.assertGreater(start_payload["simulation_runner"]["current_candle"]["open"], 5000)

    def test_sim_historical_replay_import_from_polygon_json_adds_imported_scenario(self):
        session_date = datetime(2026, 4, 2, 10, 0, tzinfo=ZoneInfo("America/Chicago")).date()
        spx_open = 5840.0
        payload = self._build_polygon_payload(
            session_date,
            base_price=523.0,
            include_extended_hours=True,
            step=-0.09,
        )
        self.service.get_single_day_bar = lambda ticker, target_date, query_type="single_close": {"Open": spx_open, "Close": 5712.0}

        import_response = self.client.post(
            "/kairos/sim/replay/import",
            json={
                "session_date": session_date.isoformat(),
                "source": "polygon-json",
                "json_payload": json.dumps(payload),
            },
        )
        import_payload = import_response.get_json()

        self.assertEqual(import_response.status_code, 200)
        self.assertIn("Historical replay ready", import_payload["status_note"])

        historical_replay = import_payload["simulation_controls"]["historical_replay"]
        imported = next(
            item
            for item in historical_replay["imported_scenarios"]
            if item["session_date"] == session_date.isoformat() and item["source"] == "Polygon / Massive JSON"
        )
        self.assertEqual(imported["session_date"], session_date.isoformat())
        imported_template = historical_replay["last_imported"] if historical_replay.get("last_imported", {}).get("scenario_key") == imported["scenario_key"] else self.kairos_sim_service._historical_replay_templates[imported["scenario_key"]].to_payload()
        self.assertEqual(imported_template["source_label"], "Polygon / Massive JSON")
        self.assertEqual(imported_template["vix_source_label"], "Flat VIX fallback")
        self.assertEqual(imported_template["tape_source"], "Synthetic SPX rebased from SPY 1m")

        scenario = self.kairos_sim_service._historical_replay_scenarios[imported["scenario_key"]]
        self.assertEqual(scenario.bars[0].open, spx_open)
        self.assertLess(scenario.bars[-1].close, scenario.bars[0].open)
        self.assertGreater(abs(scenario.bars[-1].close - scenario.bars[0].open), 350)

        imported_option = next(
            option
            for option in import_payload["simulation_runner"]["scenario_options"]
            if option["value"] == imported["scenario_key"]
        )
        self.assertTrue(imported_option["is_imported"])
        self.assertEqual(imported_option["session_date"], session_date.isoformat())

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": imported["scenario_key"]},
        )
        start_payload = start_response.get_json()

        self.assertEqual(start_response.status_code, 200)
        self.assertEqual(start_payload["simulation_runner"]["scenario_key"], imported["scenario_key"])
        self.assertEqual(start_payload["simulation_runner"]["scenario_name"], "SPY Real Day 2026-04-02")
        self.assertEqual(start_payload["simulation_runner"]["status"], "Running")

    def test_sim_historical_replay_import_persists_and_reloads_on_startup(self):
        session_date = datetime(2026, 4, 2, 10, 0, tzinfo=ZoneInfo("America/Chicago")).date()
        payload = self._build_polygon_payload(session_date, base_price=523.0, include_extended_hours=True, step=-0.09)
        self.service.get_single_day_bar = lambda ticker, target_date, query_type="single_close": {"Open": 5840.0, "Close": 5712.0}

        import_payload = self.client.post(
            "/kairos/sim/replay/import",
            json={
                "session_date": session_date.isoformat(),
                "source": "polygon-json",
                "json_payload": json.dumps(payload),
            },
        ).get_json()
        scenario_key = next(
            item["scenario_key"]
            for item in import_payload["simulation_controls"]["historical_replay"]["imported_scenarios"]
            if item["session_date"] == session_date.isoformat() and item["source"] == "Polygon / Massive JSON"
        )

        self.assertTrue((self.replay_storage_path / f"{scenario_key}.json").exists())

        extra_app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(Path(self.temp_dir.name) / "second_horme_trades.db"),
                "KAIROS_REPLAY_STORAGE_DIR": str(self.replay_storage_path),
            }
        )
        try:
            reloaded_payload = extra_app.extensions["kairos_sim_service"].get_dashboard_payload()
            self.assertTrue(
                any(item["scenario_key"] == scenario_key for item in reloaded_payload["simulation_controls"]["historical_replay"]["imported_scenarios"])
            )
        finally:
            extra_app.extensions["kairos_live_service"].shutdown()
            extra_app.extensions["kairos_sim_service"].shutdown()

    def test_sim_capture_today_route_is_gone_from_legacy_coverage(self):
        response = self.client.post("/kairos/sim/replay/capture-today")

        self.assertEqual(response.status_code, 404)

    def test_sim_runner_lifecycle_repeats_after_completion_end_and_scenario_switch(self):
        session_date = datetime(2026, 4, 3, 10, 0, tzinfo=ZoneInfo("America/Chicago")).date()
        payload = self._build_polygon_payload(
            session_date,
            base_price=524.0,
            include_extended_hours=True,
            step=-0.08,
        )
        self.service.get_single_day_bar = lambda ticker, target_date, query_type="single_close": {"Open": 5865.0, "Close": 5738.0}

        import_response = self.client.post(
            "/kairos/sim/replay/import",
            json={
                "session_date": session_date.isoformat(),
                "source": "polygon-json",
                "json_payload": json.dumps(payload),
            },
        )
        imported_key = next(
            item["scenario_key"]
            for item in import_response.get_json()["simulation_controls"]["historical_replay"]["imported_scenarios"]
            if item["session_date"] == session_date.isoformat() and item["source"] == "Polygon / Massive JSON"
        )

        first_run = self.client.post("/kairos/sim/runner/start", json={"scenario_key": imported_key}).get_json()
        first_complete = self._advance_sim_until_not_running(first_run)
        self.assertEqual(first_complete["simulation_runner"]["status"], "Completed")
        self.assertFalse(first_complete["simulation_runner"]["can_request_best_trade_override"])

        restart_run = self.client.post("/kairos/sim/runner/restart", json={"scenario_key": imported_key}).get_json()
        self.assertEqual(restart_run["simulation_runner"]["status"], "Running")
        self.assertEqual(restart_run["simulation_runner"]["current_bar_number"], 24)
        self.assertIsNone(restart_run["simulation_runner"]["active_trade_lock_in"])
        self.assertEqual(restart_run["simulation_runner"]["pause_event_filters"], [])

        end_payload = self.client.post("/kairos/sim/runner/end").get_json()
        self.assertEqual(end_payload["simulation_runner"]["status"], "Ended")

        third_run = self.client.post("/kairos/sim/runner/start", json={"scenario_key": imported_key}).get_json()
        self.assertEqual(third_run["simulation_runner"]["status"], "Running")
        self.assertEqual(third_run["simulation_runner"]["current_bar_number"], 24)

        switched_run = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": ["window-open"]},
        ).get_json()
        self.assertEqual(switched_run["simulation_runner"]["status"], "Running")
        self.assertEqual(switched_run["simulation_runner"]["scenario_key"], "window-open-then-failure-day")
        self.assertEqual(switched_run["simulation_runner"]["pause_event_filters"], ["window-open"])

    def test_window_open_auto_pauses_even_without_optional_pause_filters(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": []},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)

        self.assertEqual(paused_payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(paused_payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertTrue(paused_payload["simulation_runner"]["trade_candidate_card"]["visible"])

    def test_pause_on_setup_forming_selected_pauses_only_there(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["setup-forming"]},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)

        self.assertEqual(paused_payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(paused_payload["simulation_runner"]["pause_reason"], "pause_on_setup-forming")
        self.assertEqual(paused_payload["latest_scan"]["kairos_state"], "Setup Forming")

    def test_pause_on_window_closing_selected_pauses_only_there(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-closing"]},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)

        self.assertEqual(paused_payload["simulation_runner"]["pause_reason"], "pause_on_window-open")

        resumed_payload = self.client.post("/kairos/sim/runner/candidate/ignore").get_json()
        paused_payload = self._advance_sim_until_not_running(resumed_payload)

        self.assertEqual(paused_payload["simulation_runner"]["status"], "Paused")
        self.assertEqual(paused_payload["simulation_runner"]["pause_reason"], "pause_on_window-closing")
        self.assertEqual(paused_payload["latest_scan"]["kairos_state"], "Window Closing")

    def test_status_route_returns_trade_candidate_card_when_runner_pauses_on_window_open(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]},
        )
        self.assertEqual(start_response.status_code, 200)

        payload = self._advance_sim_until_not_running(start_response.get_json())

        candidate_card = payload["simulation_runner"]["trade_candidate_card"]
        self.assertTrue(candidate_card["visible"])
        self.assertEqual(candidate_card["status"], "ready")
        self.assertEqual([item["mode_label"] for item in candidate_card["profiles"]], ["Standard", "Fortress", "Aggressive"])
        self.assertTrue(any(item["available"] for item in candidate_card["profiles"]))
        self.assertTrue(any(not item["available"] for item in candidate_card["profiles"]))

    def test_failure_day_status_route_also_returns_trade_candidate_card_on_window_open(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": ["window-open"]},
        )
        self.assertEqual(start_response.status_code, 200)

        payload = self._advance_sim_until_not_running(start_response.get_json())

        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertEqual(payload["latest_scan"]["kairos_state"], "Window Open")
        self.assertTrue(payload["simulation_runner"]["trade_candidate_card"]["visible"])

    def test_candidate_take_route_creates_simulated_trade_lock_in(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]},
        )
        self.assertEqual(start_response.status_code, 200)

        payload = self._advance_sim_until_not_running(start_response.get_json())

        select_response = self.client.post("/kairos/sim/runner/candidate/select", json={"profile_key": "aggressive"})
        self.assertEqual(select_response.status_code, 200)
        self.assertEqual(select_response.get_json()["simulation_runner"]["trade_candidate_card"]["selected_profile_key"], "aggressive")

        take_response = self.client.post("/kairos/sim/runner/candidate/take", json={"profile_key": "aggressive"})
        take_payload = take_response.get_json()

        self.assertEqual(take_response.status_code, 200)
        self.assertEqual(take_payload["simulation_runner"]["status"], "Running")
        self.assertIsNotNone(take_payload["simulation_runner"]["active_trade_lock_in"])
        self.assertEqual(take_payload["simulation_runner"]["active_trade_lock_in"]["profile_label"], "Aggressive")
        self.assertEqual(take_payload["simulation_runner"]["trade_lock_history_count"], 1)
        self.assertTrue(take_payload["simulation_runner"]["trade_exit_status"]["visible"])
        self.assertEqual(take_payload["simulation_runner"]["trade_exit_status"]["trade_status"], "Active")
        self.assertIn("estimated_otm_probability_display", take_payload["simulation_runner"]["active_trade_lock_in"])

    def test_candidate_profiles_include_estimated_otm_probability(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)
        candidate = paused_payload["simulation_runner"]["trade_candidate_card"]["profiles"][0]

        self.assertIn("estimated_otm_probability", candidate)
        self.assertTrue(candidate["estimated_otm_probability_display"].endswith("%"))
        self.assertTrue(any(row["label"] == "Estimated OTM probability" for row in candidate["detail_rows"]))

    def test_exit_gate_pause_can_accept_partial_reduction(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": []},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)
        take_payload = self.client.post(
            "/kairos/sim/runner/candidate/take",
            json={"profile_key": "aggressive"},
        ).get_json()

        while take_payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            take_payload = self.client.get("/kairos/sim/status").get_json()
            if take_payload["simulation_runner"]["pause_reason"] == "pause_on_exit-gate":
                break

        self.assertEqual(take_payload["simulation_runner"]["pause_reason"], "pause_on_exit-gate")
        self.assertTrue(take_payload["simulation_runner"]["trade_exit_prompt"]["visible"])

        before_contracts = take_payload["simulation_runner"]["active_trade_lock_in"]["remaining_contracts"]
        accepted_payload = self.client.post("/kairos/sim/runner/exit/accept").get_json()

        self.assertEqual(accepted_payload["simulation_runner"]["status"], "Running")
        self.assertLess(accepted_payload["simulation_runner"]["active_trade_lock_in"]["remaining_contracts"], before_contracts)
        self.assertFalse(accepted_payload["simulation_runner"]["trade_exit_prompt"]["visible"])

    def test_exit_gate_skip_marks_gate_skipped(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": []},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)
        take_payload = self.client.post(
            "/kairos/sim/runner/candidate/take",
            json={"profile_key": "aggressive"},
        ).get_json()

        while take_payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            take_payload = self.client.get("/kairos/sim/status").get_json()
            if take_payload["simulation_runner"]["pause_reason"] == "pause_on_exit-gate":
                break

        skipped_payload = self.client.post("/kairos/sim/runner/exit/skip").get_json()
        self.assertEqual(skipped_payload["simulation_runner"]["status"], "Running")
        self.assertFalse(skipped_payload["simulation_runner"]["trade_exit_prompt"]["visible"])
        gate_states = skipped_payload["simulation_runner"]["active_trade_lock_in"]["exit_gate_states"]
        self.assertTrue(any(item["status"] == "skipped" for item in gate_states))

    def test_window_open_trade_lock_in_runs_rest_of_session_without_more_setup_pauses(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-closing"]},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)

        self.assertEqual(paused_payload["simulation_runner"]["pause_reason"], "pause_on_window-open")

        take_payload = self.client.post(
            "/kairos/sim/runner/candidate/take",
            json={"profile_key": "aggressive"},
        ).get_json()

        take_payload = self._advance_sim_to_completion(take_payload, exit_gate_action="skip")

        self.assertEqual(take_payload["simulation_runner"]["status"], "Completed")
        self.assertNotEqual(take_payload["simulation_runner"]["pause_reason"], "pause_on_window-closing")

    def test_candidate_take_route_auto_continues_and_posts_settlement_at_close(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]},
        )
        self.assertEqual(start_response.status_code, 200)

        payload = self._advance_sim_until_not_running(start_response.get_json())

        take_response = self.client.post("/kairos/sim/runner/candidate/take", json={"profile_key": "aggressive"})
        self.assertEqual(take_response.status_code, 200)
        payload = take_response.get_json()

        payload = self._advance_sim_to_completion(payload, exit_gate_action="skip")

        active_trade = payload["simulation_runner"]["active_trade_lock_in"]
        self.assertEqual(payload["simulation_runner"]["status"], "Completed")
        self.assertTrue(active_trade["is_settled"])
        self.assertNotEqual(active_trade["simulated_pnl_display"], "—")
        self.assertTrue(active_trade["settlement_note"])
        self.assertIn("Settled -", active_trade["settlement_badge_label"])
        self.assertIn(active_trade["result_tone"], {"win", "loss", "black-swan"})

    def test_exit_status_updates_to_long_touch_when_long_strike_is_hit(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": []},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)

        take_payload = self.client.post(
            "/kairos/sim/runner/candidate/take",
            json={"profile_key": "aggressive"},
        ).get_json()

        long_touch_seen = False
        while take_payload["simulation_runner"]["status"] != "Completed":
            if take_payload["simulation_runner"]["status"] == "Running":
                FakeTimer.instances[-1].callback()
                take_payload = self.client.get("/kairos/sim/status").get_json()
                continue
            if take_payload["simulation_runner"]["pause_reason"] == "pause_on_exit-gate":
                if take_payload["simulation_runner"]["trade_exit_prompt"]["title"] == "Long Strike Touch":
                    long_touch_seen = True
                    break
                take_payload = self.client.post("/kairos/sim/runner/exit/skip").get_json()
                continue
            break

        self.assertTrue(long_touch_seen)

    def test_exit_status_resolves_as_expired_worthless_when_session_ends_otm(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": []},
        ).get_json()
        paused_payload = self._advance_sim_until_not_running(start_payload)

        take_payload = self.client.post(
            "/kairos/sim/runner/candidate/take",
            json={"profile_key": "aggressive"},
        ).get_json()

        take_payload = self._advance_sim_to_completion(take_payload, exit_gate_action="skip")

        self.assertEqual(take_payload["simulation_runner"]["status"], "Completed")
        self.assertEqual(take_payload["simulation_runner"]["trade_exit_status"]["realized_status_summary"], "Expired Worthless")
        self.assertEqual(take_payload["simulation_runner"]["trade_exit_status"]["trade_status"], "Expired")

    def test_candidate_ignore_route_resumes_without_trade(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]},
        )
        self.assertEqual(start_response.status_code, 200)

        payload = self._advance_sim_until_not_running(start_response.get_json())

        ignore_response = self.client.post("/kairos/sim/runner/candidate/ignore")
        ignore_payload = ignore_response.get_json()

        self.assertEqual(ignore_response.status_code, 200)
        self.assertEqual(ignore_payload["simulation_runner"]["status"], "Running")
        self.assertIsNone(ignore_payload["simulation_runner"]["active_trade_lock_in"])
        self.assertIn("declined", ignore_payload["status_note"].lower())

    def test_live_best_trade_endpoint_returns_advisory_candidate_payload(self):
        response = self.client.post("/kairos/live/activate")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertIn(payload["best_trade_override"]["status"], {"ready", "stand-aside", "no-candidates", "unavailable"})
        self.assertEqual(payload["best_trade_override"]["chain_source_label"], "Schwab Live")
        self.assertEqual(payload["best_trade_override"]["price_basis_label"], "Live bid/ask + midpoint hybrid")
        summary_labels = [item["label"] for item in payload["best_trade_override"]["summary_chips"]]
        self.assertIn("CHAIN SOURCE", summary_labels)
        self.assertIn("PRICE BASIS", summary_labels)
        self.assertIn("WIDTH SCAN", summary_labels)

    def test_live_best_trade_endpoint_marks_schwab_unavailable_without_fallback(self):
        self.kairos_live_service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": False,
            "failure_label": "Unavailable",
            "message": "Same-day Schwab chain unavailable for route test.",
        }

        payload = self.client.post("/kairos/live/activate").get_json()

        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertEqual(payload["best_trade_override"]["status"], "unavailable")
        self.assertEqual(payload["best_trade_override"]["chain_source_label"], "Schwab (Unavailable)")
        self.assertIsNone(payload["best_trade_override"]["candidate"])
        self.assertIn("will not fall back", payload["best_trade_override"]["caution_text"])

    def test_live_best_trade_endpoint_returns_stand_aside_candidate_with_override_insights(self):
        self.kairos_live_service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago")).date(),
            "underlying_price": 6123.45,
            "puts": [
                {"strike": 6065, "bid": 0.32, "ask": 0.44, "mark": 0.38, "last": 0.37, "delta": -0.14, "open_interest": 1200, "total_volume": 900},
                {"strike": 6060, "bid": 0.24, "ask": 0.34, "mark": 0.29, "last": 0.29, "delta": -0.11, "open_interest": 1300, "total_volume": 950},
                {"strike": 6055, "bid": 0.17, "ask": 0.25, "mark": 0.21, "last": 0.2, "delta": -0.09, "open_interest": 1250, "total_volume": 870},
            ],
            "calls": [],
            "message": "SPX option chain retrieved successfully.",
        }

        response = self.client.post("/kairos/live/activate")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["best_trade_override"]["status"], "stand-aside")
        candidate = payload["best_trade_override"]["candidate"]
        self.assertIsNotNone(candidate)
        self.assertFalse(candidate["is_fully_valid"])
        self.assertTrue(candidate["eligibility_gaps"])
        self.assertTrue(candidate["closest_valid_guidance"])
        self.assertGreater(candidate["fit_score"], 0)
        self.assertFalse(payload["best_trade_override"]["can_open_trade"])

    def test_live_activate_closed_market_shows_closed_trade_card(self):
        self.kairos_live_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        response = self.client.post("/kairos/live/activate")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertIn("Market closed", payload["status_note"])
        self.assertTrue(payload["best_trade_override"]["visible"])
        self.assertEqual(payload["best_trade_override"]["status"], "market-closed")

    def test_live_activate_can_select_ten_or_fifteen_point_widths(self):
        self.service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 6129.0 if ticker == "^GSPC" else 18.76,
            "Daily Point Change": 10.0 if ticker == "^GSPC" else -1.0,
            "Daily Percent Change": 0.16 if ticker == "^GSPC" else -5.06,
            "As Of": "2026-04-06 09:45:00 AM CDT",
        }
        self.kairos_live_service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": datetime(2026, 4, 6, 9, 45, tzinfo=ZoneInfo("America/Chicago")).date(),
            "underlying_price": 6129.0,
            "puts": [
                {"strike": 6065, "bid": 3.2, "ask": 3.35, "mark": 3.28, "last": 3.27, "delta": -0.14, "open_interest": 1200, "total_volume": 900},
                {"strike": 6055, "bid": 2.42, "ask": 2.55, "mark": 2.49, "last": 2.48, "delta": -0.08, "open_interest": 1300, "total_volume": 950},
                {"strike": 6050, "bid": 2.08, "ask": 2.2, "mark": 2.14, "last": 2.14, "delta": -0.07, "open_interest": 1250, "total_volume": 870},
            ],
            "calls": [],
            "message": "SPX option chain retrieved successfully.",
        }

        payload = self.client.post("/kairos/live/activate").get_json()

        candidate = payload["best_trade_override"]["candidate"]
        self.assertIsNotNone(candidate)
        self.assertIn(candidate["spread_width"], {10, 15})
        detail_values = {row["label"]: row["value"] for row in candidate["detail_rows"]}
        self.assertEqual(detail_values["WIDTH SCAN"], "up to 15 pts")

    def test_live_kairos_uses_same_day_atm_straddle_for_expected_move_and_credit_map_labels(self):
        self.service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 6772.12 if ticker == "^GSPC" else 20.1,
            "Daily Point Change": 12.0 if ticker == "^GSPC" else -0.8,
            "Daily Percent Change": 0.18 if ticker == "^GSPC" else -3.8,
            "As Of": "2026-04-06 11:20:00 AM CDT",
        }
        self.kairos_live_service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date: {
            "success": True,
            "source_name": "Schwab",
            "expiration_date": datetime(2026, 4, 6, 11, 20, tzinfo=ZoneInfo("America/Chicago")).date(),
            "underlying_price": 6772.12,
            "puts": [
                {"strike": 6770, "bid": 12.2, "ask": 12.6, "mark": 12.4, "last": 12.38, "delta": -0.49, "open_interest": 6200, "total_volume": 3900},
                {"strike": 6765, "bid": 10.0, "ask": 10.4, "mark": 10.2, "last": 10.19, "delta": -0.44, "open_interest": 5800, "total_volume": 3500},
                {"strike": 6700, "bid": 2.95, "ask": 3.15, "mark": 3.05, "last": 3.04, "delta": -0.17, "open_interest": 2600, "total_volume": 1800},
                {"strike": 6695, "bid": 2.62, "ask": 2.82, "mark": 2.72, "last": 2.71, "delta": -0.15, "open_interest": 2400, "total_volume": 1700},
                {"strike": 6690, "bid": 2.32, "ask": 2.5, "mark": 2.41, "last": 2.4, "delta": -0.14, "open_interest": 2250, "total_volume": 1600},
                {"strike": 6685, "bid": 2.0, "ask": 2.16, "mark": 2.08, "last": 2.07, "delta": -0.12, "open_interest": 2100, "total_volume": 1500},
                {"strike": 6680, "bid": 1.72, "ask": 1.88, "mark": 1.8, "last": 1.8, "delta": -0.11, "open_interest": 1900, "total_volume": 1430},
            ],
            "calls": [
                {"strike": 6775, "bid": 13.9, "ask": 14.3, "mark": 14.1, "last": 14.09, "delta": 0.51, "open_interest": 6400, "total_volume": 4020},
                {"strike": 6780, "bid": 11.6, "ask": 12.0, "mark": 11.8, "last": 11.79, "delta": 0.46, "open_interest": 5900, "total_volume": 3600},
            ],
            "message": "SPX option chain retrieved successfully.",
        }

        payload = self.client.post("/kairos/live/activate").get_json()

        candidate = payload["best_trade_override"]["candidate"]
        self.assertIsNotNone(candidate)
        summary_values = {item["label"]: item["value"] for item in payload["best_trade_override"]["summary_chips"]}
        self.assertEqual(summary_values["EM SOURCE"], "Same-day ATM straddle")
        self.assertEqual(summary_values["Expected move"], "26.50 pts")
        self.assertEqual(summary_values["Percent floor"], "67.72 pts")
        self.assertEqual(summary_values["EM floor"], "53.00 pts")
        self.assertEqual(summary_values["Boundary source"], "Percent Floor")
        detail_values = {row["label"]: row["value"] for row in candidate["detail_rows"]}
        self.assertEqual(detail_values["EXPECTED MOVE"], "26.50 pts")
        self.assertEqual(detail_values["EM INPUTS"], "6770P 12.40 + 6775C 14.10")
        self.assertEqual(detail_values["Percent floor"], "1.00% (67.72 pts)")
        self.assertEqual(detail_values["EM floor"], "2.00x (53.00 pts)")
        self.assertEqual(detail_values["Boundary source"], "Percent Floor")
        self.assertAlmostEqual(candidate["distance_points"], 77.12, places=2)
        self.assertAlmostEqual(candidate["daily_move_multiple"], 2.91, places=2)

        credit_map = payload["live_workspace"]["credit_map"]
        em_barrier = next(item for item in credit_map["barrier_markers"] if item["layout_key"] == "em-barrier")
        self.assertEqual(em_barrier["value_label"], "6,745.62")
        self.assertIn("same-day ATM expected-move barrier", credit_map["positioning_note"])
        self.assertEqual(credit_map["boundary_binding_source"], "Percent Floor")
        self.assertIn("label_x", credit_map["markers"][0])
        self.assertIn("text_anchor", credit_map["markers"][0])
        self.assertIn("label_y", em_barrier)
        self.assertIn("spot_top_label_y", credit_map)

    def test_live_page_renders_kairos_prefill_buttons_without_trade_not_ready_or_inline_barrier_labels(self):
        payload = self.kairos_live_service.get_dashboard_payload()
        payload["live_workspace"]["credit_map"]["barrier_markers"] = [
            {
                "x": 320,
                "line_top_y": 28,
                "line_bottom_y": 176,
                "label_y": 188,
                "value_y": 202,
                "css_class": "apollo-risk-reference-em",
                "short_label": "EM Barrier",
                "value_label": "6,745.62",
                "tooltip": "Expected move barrier",
            },
            {
                "x": 410,
                "line_top_y": 28,
                "line_bottom_y": 176,
                "label_y": 188,
                "value_y": 202,
                "css_class": "apollo-risk-reference-barrier",
                "short_label": "Window Boundary",
                "value_label": "6,712.50",
                "tooltip": "Prime boundary",
            },
        ]
        payload["live_workspace"]["candidate_cards"] = [
            {
                **payload["live_workspace"]["candidate_cards"][0],
                "prefill_enabled": True,
                "prefill_fields": {
                    "system_name": "Kairos",
                    "journal_name": "Horme",
                    "system_version": "4.0",
                    "candidate_profile": "Standard",
                    "short_strike": 6065,
                    "long_strike": 6060,
                },
                "can_open_trade": False,
            }
        ]
        self.kairos_live_service.initialize_live_kairos_on_page_load = lambda: payload

        response = self.client.get("/kairos/live")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Send to Real Trades", response.data)
        self.assertIn(b"Send to Simulated Trades", response.data)
        self.assertIn(b"/kairos/prefill-candidate", response.data)
        self.assertNotIn(b"Trade Not Ready", response.data)
        self.assertNotIn(b">EM Barrier<", response.data)
        self.assertNotIn(b">Window Boundary<", response.data)

    def test_kairos_prefill_candidate_redirects_to_trade_draft(self):
        response = self.client.post(
            "/kairos/prefill-candidate",
            data={
                "target_mode": "simulated",
                "candidate_profile": "Fortress",
                "short_strike": "6065",
                "long_strike": "6060",
                "spread_width": "5",
                "spx_at_entry": "6123.45",
                "vix_at_entry": "18.76",
                "expected_move": "26.50",
                "expected_move_used": "26.50",
                "distance_to_short": "58.45",
                "actual_distance_to_short": "58.45",
                "actual_em_multiple": "2.21",
            },
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Review Kairos Candidate Draft", response.data)
        self.assertIn(b'value="Kairos" selected', response.data)
        self.assertIn(b'name="candidate_profile"', response.data)
        self.assertIn(b'value="Fortress"', response.data)

    def test_live_tape_reloads_into_sim_scenario_options_with_live_tag_after_restart(self):
        activate_response = self.client.post("/kairos/live/activate")

        self.assertEqual(activate_response.status_code, 200)
        self.kairos_live_service.shutdown()

        extra_app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(Path(self.temp_dir.name) / "third_horme_trades.db"),
                "KAIROS_REPLAY_STORAGE_DIR": str(self.replay_storage_path),
            }
        )
        try:
            extra_service = extra_app.extensions["market_data_service"]
            extra_service.get_latest_snapshot = self.service.get_latest_snapshot
            extra_service.get_provider_metadata = self.service.get_provider_metadata
            extra_service.get_same_day_intraday_candles = self.service.get_same_day_intraday_candles
            extra_service.get_intraday_candles_for_date = self.service.get_intraday_candles_for_date

            extra_live = extra_app.extensions["kairos_live_service"]
            extra_sim = extra_app.extensions["kairos_sim_service"]
            for service in {extra_live, extra_sim}:
                service.now_provider = lambda: datetime(2026, 4, 7, 9, 45, tzinfo=ZoneInfo("America/Chicago"))
                service.timer_factory = FakeTimer
                service.options_chain_service.get_spx_option_chain_summary = lambda expiration_date, _self=self: _self._build_live_option_chain_summary()

            extra_sim.configure_runtime({"mode": "Simulation"})
            payload = extra_sim.get_dashboard_payload()
            live_option = next(
                option
                for option in payload["simulation_runner"]["scenario_options"]
                if option.get("source_family_tag") == "LIVE TAPE"
            )
            self.assertEqual(live_option["source_type"], "live_spx_tape")
            self.assertIn(live_option["session_status"], {"complete", "recovered"})
            self.assertIsNotNone(live_option["session_summary"])
        finally:
            extra_app.extensions["kairos_live_service"].shutdown()
            extra_app.extensions["kairos_sim_service"].shutdown()

    def test_live_trade_open_prefills_real_trade_journal_without_auto_saving(self):
        self.client.post("/kairos/live/activate")
        self.client.post("/kairos/live/best-trade")

        open_response = self.client.post("/kairos/live/trade/open")
        open_payload = open_response.get_json()

        self.assertEqual(open_response.status_code, 200)
        self.assertIn("redirect_url", open_payload)
        self.assertIsNotNone(open_payload["live_trade_manager"]["active_trade"])
        self.assertEqual(open_payload["live_trade_manager"]["active_trade"]["entry_metadata"]["source_label"], "Live")
        self.assertEqual(open_payload["live_trade_manager"]["active_trade"]["entry_metadata"]["chain_source_label"], "Schwab Live")
        self.assertEqual(open_payload["live_trade_manager"]["active_trade"]["entry_metadata"]["price_basis_label"], "Live bid/ask + midpoint hybrid")
        active_trade = open_payload["live_trade_manager"]["active_trade"]

        journal_response = self.client.get("/trades/real?prefill=1")
        direct_response = self.client.get("/trades/real")

        self.assertEqual(journal_response.status_code, 200)
        self.assertIn(b"Review Kairos Live Draft", journal_response.data)
        self.assertIn(b"Kairos live trade data is loaded into this draft.", journal_response.data)
        self.assertIn(b'value="Kairos" selected', journal_response.data)
        self.assertIn(
            f'name="expected_move" type="number" step="0.01" value="{active_trade["daily_move_anchor"]:.2f}"'.encode(),
            journal_response.data,
        )
        self.assertNotEqual(round(active_trade["daily_move_anchor"], 2), round(active_trade["distance_points"], 2))
        self.assertNotIn(b"Review Kairos Live Draft", direct_response.data)

    def test_live_exit_recommendation_can_be_accepted_and_annotated_on_tape(self):
        self.client.post("/kairos/live/activate")
        best_trade_payload = self.client.post("/kairos/live/best-trade").get_json()
        opened_payload = self.client.post("/kairos/live/trade/open").get_json()
        active_trade = opened_payload["live_trade_manager"]["active_trade"]
        short_strike = active_trade["short_strike"]

        self.service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": (short_strike + 10.0) if ticker == "^GSPC" else 18.76,
            "Daily Point Change": -55.0 if ticker == "^GSPC" else -1.0,
            "Daily Percent Change": -0.90 if ticker == "^GSPC" else -5.06,
            "As Of": "2026-04-06 10:05:00 AM CDT",
        }
        capture_base_price = (short_strike + 10.0) - (95 * 0.08) - 0.03
        self.service.get_same_day_intraday_candles = lambda ticker, interval_minutes=1, query_type="intraday": pd.DataFrame(
            self._build_regular_session_rows(
                datetime(2026, 4, 6, 10, 5, tzinfo=ZoneInfo("America/Chicago")).date(),
                base_price=capture_base_price,
                include_extended_hours=False,
                step=0.08,
            )
        )
        self.kairos_live_service.now_provider = lambda: datetime(2026, 4, 6, 10, 5, tzinfo=ZoneInfo("America/Chicago"))

        scan_payload = self.kairos_live_service.run_scan_cycle(trigger_reason="test-live-exit")
        exit_prompt = scan_payload["live_trade_manager"]["exit_prompt"]

        self.assertTrue(exit_prompt["visible"])
        self.assertIn(exit_prompt["gate_key"], {"short-strike-proximity", "structure-break"})
        self.assertIn("resulting_current_credit_display", exit_prompt)

        accepted_payload = self.client.post("/kairos/live/trade/exit/accept").get_json()
        self.assertFalse(accepted_payload["live_trade_manager"]["exit_prompt"]["visible"])
        self.assertGreaterEqual(accepted_payload["live_trade_manager"]["active_trade"]["closed_contracts"], 1)
        marker_kinds = {item["kind"] for item in accepted_payload["bar_map"]["event_markers"]}
        self.assertIn("trade-open", marker_kinds)
        self.assertTrue({"trade-partial-close", "trade-full-close"}.intersection(marker_kinds))

    def test_sim_best_trade_override_stays_disabled_at_window_open(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["window-open"]},
        )
        payload = start_response.get_json()

        while payload["simulation_runner"]["status"] == "Running":
            FakeTimer.instances[-1].callback()
            payload = self.client.get("/kairos/sim/status").get_json()

        self.assertEqual(payload["simulation_runner"]["pause_reason"], "pause_on_window-open")
        self.assertFalse(payload["simulation_runner"]["can_request_best_trade_override"])
        self.assertIn("disabled at Prime", payload["simulation_runner"]["best_trade_override_disabled_reason"])

    def test_sim_best_trade_override_can_lock_trade_from_valid_non_window_open_pause(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["setup-forming"]},
        )
        self.assertEqual(start_response.status_code, 200)

        pause_payload = self._advance_sim_until_not_running(start_response.get_json())
        self.assertEqual(pause_payload["simulation_runner"]["pause_reason"], "pause_on_setup-forming")
        self.assertTrue(pause_payload["simulation_runner"]["can_request_best_trade_override"])

        best_trade_response = self.client.post("/kairos/sim/runner/best-trade")
        best_trade_payload = best_trade_response.get_json()
        candidate_card = best_trade_payload["simulation_runner"]["trade_candidate_card"]

        self.assertEqual(best_trade_response.status_code, 200)
        self.assertTrue(candidate_card["visible"])
        self.assertTrue(candidate_card["is_override"])
        self.assertEqual(candidate_card["title"], "Best Trade Override")

        selected_profile_key = candidate_card["selected_profile_key"] or (candidate_card["profiles"][0]["mode_key"] if candidate_card["profiles"] else "")
        self.assertTrue(selected_profile_key)

        take_response = self.client.post("/kairos/sim/runner/candidate/take", json={"profile_key": selected_profile_key})
        take_payload = take_response.get_json()

        self.assertEqual(take_response.status_code, 200)
        self.assertEqual(take_payload["simulation_runner"]["status"], "Running")
        self.assertIsNotNone(take_payload["simulation_runner"]["active_trade_lock_in"])
        self.assertIn("Override", take_payload["status_note"])

    def test_sim_runner_best_trade_partial_response_omits_static_controls(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_response = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "bullish-recovery-day", "pause_events": ["setup-forming"]},
        )
        self.assertEqual(start_response.status_code, 200)

        pause_payload = self._advance_sim_until_not_running(start_response.get_json())
        self.assertEqual(pause_payload["simulation_runner"]["pause_reason"], "pause_on_setup-forming")

        best_trade_response = self.client.post("/kairos/sim/runner/best-trade?response=runner")
        best_trade_payload = best_trade_response.get_json()

        self.assertEqual(best_trade_response.status_code, 200)
        self.assertEqual(best_trade_payload["partial_response"], "runner")
        self.assertIn("simulation_runner", best_trade_payload)
        self.assertIn("bar_map", best_trade_payload)
        self.assertNotIn("simulation_controls", best_trade_payload)

    def test_completed_sim_is_not_treated_as_paused(self):
        self.kairos_sim_service.now_provider = lambda: datetime(2026, 4, 4, 10, 0, tzinfo=ZoneInfo("America/Chicago"))

        start_payload = self.client.post(
            "/kairos/sim/runner/start",
            json={"scenario_key": "window-open-then-failure-day", "pause_events": []},
        ).get_json()

        paused_payload = self._advance_sim_until_not_running(start_payload)
        resumed_payload = self.client.post("/kairos/sim/runner/candidate/ignore").get_json()
        completed_payload = self._advance_sim_to_completion(resumed_payload, exit_gate_action="skip")

        self.assertEqual(completed_payload["simulation_runner"]["status"], "Completed")
        self.assertFalse(completed_payload["simulation_runner"]["can_request_best_trade_override"])
        self.assertEqual(completed_payload["simulation_runner"]["trade_candidate_card"]["visible"], False)
        self.assertIn("only available while the replay is paused", completed_payload["simulation_runner"]["best_trade_override_disabled_reason"])


if __name__ == "__main__":
    unittest.main()