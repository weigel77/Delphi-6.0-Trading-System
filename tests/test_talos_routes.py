import tempfile
import unittest
from pathlib import Path

from app import create_app


class TalosRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "routes.db"
        self.app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(self.database_path),
            }
        )
        self.client = self.app.test_client()
        market_data_service = self.app.extensions["market_data_service"]
        market_data_service.get_latest_snapshot = lambda ticker, query_type="latest": {
            "Latest Value": 5300.0 if ticker == "^GSPC" else 18.5,
            "Daily Point Change": 0.0,
            "Daily Percent Change": 0.0,
            "As Of": "Now",
        }
        market_data_service.get_provider_metadata = lambda: {
            "requires_auth": False,
            "authenticated": True,
            "live_provider_name": "Local",
            "provider_name": "Local",
            "live_provider_key": "local",
            "spx_historical_provider_name": "Local",
            "vix_historical_provider_name": "Local",
        }

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_talos_dashboard_uses_engine_layout_and_runtime_controls(self) -> None:
        talos_service = self.app.extensions["talos_service"]
        talos_service.get_dashboard_payload = lambda: {
            "enabled": True,
            "account_balance_display": "$135,000.00",
            "settled_balance_display": "$135,000.00",
            "current_account_value_display": "$135,250.00",
            "unrealized_pnl_display": "$250.00",
            "open_trade_count": 1,
            "open_records": [
                {
                    "trade_number": 12,
                    "system_name": "Apollo",
                    "profile_label": "Standard",
                    "expiration_display": "Mon 2026-04-20",
                    "contracts_display": "3",
                    "short_strike_display": "5300",
                    "long_strike_display": "5280",
                    "status": "Healthy",
                    "status_key": "healthy",
                    "action_recommendation": "Watch",
                    "exit_plan_summary": "Next: 15-point short-strike proximity close 1 at SPX <= 5300.",
                    "current_pl_display": "$250.00",
                    "talos_candidate_score_display": "71.4",
                    "talos_score_band_label": "Acceptable",
                    "talos_score_band_key": "acceptable",
                    "talos_entry_ready": True,
                    "exit_plan_gates": [
                        {
                            "label": "Gate 1",
                            "comparison": "<=",
                            "trigger_value_display": "5300",
                            "quantity_to_close_display": "1",
                            "state_label": "Pending",
                            "state_key": "pending",
                        },
                        {
                            "label": "Gate 2",
                            "comparison": "<=",
                            "trigger_value_display": "5290",
                            "quantity_to_close_display": "1",
                            "state_label": "Executed",
                            "state_key": "executed",
                        },
                    ],
                    "talos_score_breakdown_items": [{"label": "Safety", "value": "+28.0"}],
                    "talos_score_explanation_lines": [
                        "Weighted score 71.4.",
                        "Pricing: Entry uses short bid - long ask. Exit uses short ask - long bid.",
                    ],
                }
            ],
            "activity_log": [],
            "decision_model": {
                "formula": "Score = safety 40 + history 25 + credit 15 + profit 10 + fit 10 - loss penalty 20.",
                "minimum_entry_score": 70.0,
                "weights": ["Safety 40%"],
                "pricing_rule": "Simulated entry fill = short bid - long ask. Simulated exit fill = short ask - long bid.",
                "rotation_rule": "Talos only rotates after 50% capture.",
                "kairos_entry_rule": "Talos only opens Kairos when the best-trade override is Ready and the mode is Prime or Window Open.",
                "example_record": {
                    "trade_number": 12,
                    "talos_score_breakdown_items": [{"label": "Safety", "value": "+28.0"}],
                },
            },
            "header_market_snapshots": {},
            "last_cycle_at_display": "2026-04-20 10:00:00 AM CDT",
            "last_cycle_reason": "manual",
            "last_cycle_status": "completed",
            "next_scan_at_display": "2026-04-20 10:01:00 AM CDT",
            "runtime_status_display": "Ready For Next Scan",
            "runtime_status_key": "ready",
            "formula_chart": {
                "segments": [
                    {"label": "Safety", "label_display": "Safety 40%", "weight_display": "40%", "color": "#ef4444", "shade_color": "#b91c1c"},
                    {"label": "History", "label_display": "History 25%", "weight_display": "25%", "color": "#f97316", "shade_color": "#c2410c"},
                ],
                "penalty_label": "Loss Penalty",
                "penalty_weight_display": "20% penalty",
                "penalty_note": "Loss penalty is applied separately.",
            },
            "current_commitment": {
                "value": 820.0,
                "value_display": "$820.00",
                "gauge_max": 135250.0,
                "gauge_max_display": "$135,250.00",
                "ratio": 0.0061,
            },
            "settled_equity_curve": {"area_points": "24,196 24,110 616,110 616,196", "polyline_points": "24,110 616,110", "markers": [{"x": 24, "y": 110, "label": "Start", "value_display": "$135,000.00"}], "start_value_display": "$135,000.00", "end_value_display": "$135,000.00", "closed_trade_count": 0},
            "performance_metrics": {
                "totals": {"total_trades": 8, "open_trades": 1, "closed_trades": 7},
                "win_rate": {"value": 71.4, "wins": 5, "losses": 2},
                "expectancy": {"value": 62.5, "scale": 100.0},
                "net_pnl": {"value": 842.11},
                "outcome_mix": {"win_percent": 71.4, "loss_percent": 14.3, "black_swan_percent": 14.3},
                "average_win_loss": {"average_win": 210.0, "average_loss": 95.0, "average_black_swan_loss": 310.0},
            },
            "recovery_archive_dir": str(Path(self.temp_dir.name) / "talos_recovery"),
            "paused": False,
        }

        response = self.client.get("/talos")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Talos Engine", response.data)
        self.assertIn(b"Talos: Automaton of Crete and Wall Street Options Thinker", response.data)
        self.assertIn(b"Version 7.1", response.data)
        self.assertIn(b"Current Talos Orders", response.data)
        self.assertIn(b"Current Talos Commitment", response.data)
        self.assertIn(b"talos-title-panel", response.data)
        self.assertIn(b"talos-visual-grid", response.data)
        self.assertIn(b'src="/static/icons/talos-icon.png"', response.data)
        self.assertIn(b"Win / Loss / Black Swan %", response.data)
        self.assertIn(b"Expiration", response.data)
        self.assertIn(b"Qty", response.data)
        self.assertIn(b"Strikes", response.data)
        self.assertIn(b"Score / Gate", response.data)
        self.assertIn(b"Exit Plan", response.data)
        self.assertIn(b"5300", response.data)
        self.assertIn(b"Close 1", response.data)
        self.assertIn(b"Executed", response.data)
        self.assertIn(b"Explain score for Trade #12", response.data)
        self.assertIn(b"<details class=\"talos-trade-detail-panel\">", response.data)
        self.assertNotIn(b"<details class=\"talos-trade-detail-panel\" open>", response.data)
        self.assertIn(b"new_starting_balance", response.data)
        self.assertIn(b"Archive And Reset", response.data)
        self.assertIn(b"Talos Decision Model", response.data)
        self.assertIn(b"Settled Equity Curve", response.data)
        self.assertNotIn(b"Runtime Status", response.data)
        self.assertNotIn(b"Realized P/L", response.data)
        self.assertNotIn(b"Scoring Policy", response.data)
        self.assertNotIn(b"Open Exposure Mix", response.data)
        self.assertNotIn(b"Run Talos Cycle", response.data)
        self.assertNotIn(b"Open Talos Journal", response.data)
        self.assertNotIn(b"Skip Log", response.data)

    def test_talos_styles_keep_reset_and_model_summaries_compact_when_collapsed(self) -> None:
        response = self.client.get("/static/styles.css")

        self.assertEqual(response.status_code, 200)
        css = response.get_data(as_text=True)
        summary_start = css.index(".talos-reset-details summary,")
        marker_start = css.index(".talos-reset-details summary::-webkit-details-marker,")
        summary_block = css[summary_start:marker_start]

        self.assertIn("padding: 10px 0;", summary_block)
        self.assertIn("cursor: pointer;", summary_block)
        self.assertNotIn("min-height", summary_block)

    def test_talos_reset_requires_new_starting_balance(self) -> None:
        response = self.client.post("/talos/reset", data={}, follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Talos reset requires a new starting balance greater than zero", response.data)


if __name__ == "__main__":
    unittest.main()