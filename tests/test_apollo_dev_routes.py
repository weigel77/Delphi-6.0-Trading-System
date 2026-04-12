import unittest
from unittest.mock import patch

from app import app


class ApolloDevRoutesTest(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True)
        self.client = app.test_client()
        service = app.extensions["market_data_service"]
        service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 6123.45 if ticker == "^GSPC" else 18.76,
            "Daily Point Change": 10.0 if ticker == "^GSPC" else -1.0,
            "Daily Percent Change": 0.16 if ticker == "^GSPC" else -5.06,
            "As Of": "2026-04-04 11:00:00 AM CDT",
        }
        service.get_provider_metadata = lambda: {
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
            "provider_name": "Schwab",
            "live_provider_key": "schwab",
            "spx_historical_provider_name": "Schwab",
            "vix_historical_provider_name": "Schwab",
        }

    def tearDown(self):
        app.config.update(TESTING=False)

    @patch("app.execute_apollo_precheck")
    def test_apollo_autorun_query_runs_shared_workflow(self, execute_apollo_precheck):
        execute_apollo_precheck.return_value = {
            "title": "Apollo Gate 1 -- SPX Structure",
            "structure_grade": "Allowed",
            "structure_grade_class": "allowed",
            "structure_base_grade": "Allowed",
            "structure_final_grade": "Allowed",
            "structure_rsi_modifier": "None",
            "structure_rsi_note": "Daily RSI unavailable; base structure kept.",
            "structure_source_used": "Schwab",
            "structure_session_note": "",
            "structure_trend_classification": "Balanced",
            "structure_damage_classification": "Contained",
            "structure_session_low": "5,000.00",
            "structure_session_high": "5,100.00",
            "structure_range_position": "50.00%",
            "structure_ema8": "5,048.00",
            "structure_ema21": "5,042.00",
            "structure_recent_price_action": "Stable",
            "structure_session_window": "09:30 to 16:00",
            "structure_available": False,
            "structure_chart": {"available": False, "points": []},
            "structure_message": "Test structure",
            "structure_rules": [],
            "structure_attempted_sources": [],
            "structure_fallback_reason": "",
            "structure_preferred_source": "Schwab",
            "spx_value": "5,050.00",
            "spx_as_of": "Now",
            "vix_value": "20.00",
            "vix_as_of": "Now",
            "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
            "macro_title": "Apollo Gate 2 -- Macro Event",
            "macro_source": "MarketWatch",
            "macro_grade": "None",
            "macro_grade_class": "good",
            "macro_target_day": "Mon Apr 06, 2026",
            "macro_checked_at": "Fri 2026-04-03 10:00 AM CDT",
            "macro_major_events": [],
            "macro_minor_events": [],
            "macro_diagnostic": {},
            "macro_source_attempts": [],
            "option_chain_rows_setting": "Adaptive",
            "option_chain_min_premium_target": "1.00",
            "option_chain_grouping": "Puts ascending → Calls ascending",
            "trade_candidates_title": "Apollo Gate 3 -- Trade Candidates",
            "trade_candidates_status": "Stand Aside",
            "trade_candidates_status_class": "not-available",
            "trade_candidates_count": 0,
            "trade_candidates_count_label": "0 Candidates",
            "trade_candidates_message": "No trade candidates were produced.",
            "trade_candidates_expected_move": "—",
            "trade_candidates_short_barrier_put": "—",
            "trade_candidates_short_barrier_call": "—",
            "trade_candidates_items": [],
            "trade_candidates_diagnostics": {},
            "apollo_trigger_source": "autorun URL",
            "apollo_trigger_note": "Apollo was triggered by autorun URL.",
            "reasons": [],
        }

        response = self.client.get("/apollo?autorun=1")

        self.assertEqual(response.status_code, 200)
        execute_apollo_precheck.assert_called_once()
        self.assertIn(b"Apollo was triggered by autorun URL.", response.data)

    @patch("app.execute_apollo_precheck")
    def test_debug_run_apollo_returns_json(self, execute_apollo_precheck):
        execute_apollo_precheck.return_value = {
            "title": "Apollo Gate 1 -- SPX Structure",
            "status": "Allowed",
            "structure_grade": "Allowed",
            "macro_grade": "None",
            "trade_candidates_count": 2,
            "apollo_trigger_source": "autorun URL",
            "apollo_trigger_note": "Apollo was triggered by autorun URL.",
        }

        response = self.client.get("/debug/run-apollo")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        execute_apollo_precheck.assert_called_once()
        self.assertEqual(payload["trigger_source"], "autorun URL")
        self.assertTrue(payload["ok"])

    @patch("app.execute_apollo_precheck")
    def test_apollo_route_renders_failure_type_and_no_candidate_state(self, execute_apollo_precheck):
        execute_apollo_precheck.return_value = {
            "title": "Apollo Gate 1 -- SPX Structure",
            "status": "Allowed",
            "status_class": "allowed",
            "structure_grade": "Good",
            "structure_grade_class": "good",
            "structure_base_grade": "Neutral",
            "structure_final_grade": "Good",
            "structure_rsi_modifier": "Oversold Boost",
            "structure_rsi_note": "Daily RSI 24.80 is oversold and upgrades structure by one level.",
            "structure_source_used": "Schwab",
            "structure_preferred_source": "Schwab",
            "structure_session_note": "",
            "structure_trend_classification": "Balanced",
            "structure_damage_classification": "Contained",
            "structure_session_low": "6,450.00",
            "structure_session_high": "6,550.00",
            "structure_range_position": "50.00%",
            "structure_ema8": "6,498.00",
            "structure_ema21": "6,490.00",
            "structure_recent_price_action": "Stable",
            "structure_session_window": "09:30 to 16:00",
            "structure_available": True,
            "structure_chart": {"available": False, "points": []},
            "structure_message": "",
            "structure_rules": [],
            "structure_attempted_sources": [],
            "structure_fallback_reason": "",
            "spx_value": "6,500.00",
            "spx_as_of": "Now",
            "vix_value": "20.00",
            "vix_as_of": "Now",
            "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
            "macro_title": "Apollo Gate 2 -- Macro Event",
            "macro_source": "MarketWatch",
            "macro_grade": "None",
            "macro_grade_class": "good",
            "macro_target_day": "Mon Apr 06, 2026",
            "macro_checked_at": "Fri 2026-04-03 10:00 AM CDT",
            "macro_major_events": [],
            "macro_minor_events": [],
            "macro_diagnostic": {},
            "macro_source_attempts": [],
            "option_chain_success": False,
            "option_chain_status": "Upstream unavailable",
            "option_chain_status_class": "not-available",
            "option_chain_source": "Schwab",
            "option_chain_failure_category": "upstream-unavailable",
            "option_chain_failure_label": "Upstream unavailable",
            "option_chain_heading_date": "Mon Apr 06, 2026",
            "option_chain_symbol_requested": "$SPX",
            "option_chain_expiration_target": "2026-04-06",
            "option_chain_expiration": "2026-04-06",
            "option_chain_expiration_count": 0,
            "option_chain_puts_count": 0,
            "option_chain_calls_count": 0,
            "option_chain_rows_displayed": 0,
            "option_chain_display_puts_count": 0,
            "option_chain_display_calls_count": 0,
            "option_chain_min_premium_target": "1.00",
            "option_chain_rows_setting": "Adaptive",
            "option_chain_grouping": "Puts ascending → Calls ascending",
            "option_chain_strike_range": "—",
            "option_chain_message": "Schwab returned a temporary service outage while Apollo requested the SPX chain.",
            "option_chain_preview_rows": [],
            "option_chain_final_symbol": "$SPX",
            "option_chain_final_expiration_sent": "2026-04-06",
            "option_chain_request_attempt_used": "Attempt A",
            "option_chain_raw_params_sent": {"symbol": "$SPX"},
            "option_chain_error_detail": "HTTP 503",
            "option_chain_attempt_results": [
                {
                    "label": "Attempt A",
                    "status": "failed",
                    "status_code": 503,
                    "params": {"symbol": "$SPX"},
                    "error_detail": "HTTP 503",
                    "failure_label": "Upstream unavailable",
                }
            ],
            "trade_candidates_title": "Apollo Gate 3 -- Trade Candidates",
            "trade_candidates_status": "Stand Aside",
            "trade_candidates_status_class": "poor",
            "trade_candidates_message": "No Gate 3 mode produced a valid SPX 1DTE trade for this expiration.",
            "trade_candidates_count": 0,
            "trade_candidates_valid_count": 0,
            "trade_candidates_outcome_category": "no-candidates",
            "trade_candidates_outcome_label": "No candidates",
            "trade_candidates_count_label": "0 Candidates",
            "trade_candidates_expected_move": "—",
            "trade_candidates_short_barrier_put": "—",
            "trade_candidates_short_barrier_call": "—",
            "trade_candidates_items": [],
            "trade_candidates_diagnostics": {},
            "apollo_trigger_source": "button",
            "apollo_trigger_note": "Apollo was triggered by button.",
            "reasons": [],
        }

        response = self.client.post("/apollo")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Base Structure", response.data)
        self.assertIn(b"RSI Modifier", response.data)
        self.assertIn(b"Final Structure", response.data)
        self.assertIn(b"Failure type", response.data)
        self.assertIn(b"Upstream unavailable", response.data)
        self.assertIn(b"No candidates", response.data)


if __name__ == "__main__":
    unittest.main()