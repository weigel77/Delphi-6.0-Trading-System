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
            "decision_log": [],
            "apollo_portfolio": {
                "premium_target": 1350.0,
                "premium_target_display": "$1,350.00",
                "selected_premium": 1020.0,
                "selected_premium_display": "$1,020.00",
                "current_premium": 250.0,
                "current_premium_display": "$250.00",
                "projected_black_swan_loss_used": 4200.0,
                "projected_black_swan_loss_used_display": "$4,200.00",
                "projected_black_swan_loss_current": 820.0,
                "projected_black_swan_loss_current_display": "$820.00",
                "projected_black_swan_loss_max": 13500.0,
                "projected_black_swan_loss_max_display": "$13,500.00",
                "black_swan_roi_used": -0.44,
                "black_swan_roi_used_display": "-44.00%",
                "selected_profile_mix": "Standard x1, Fortress x1",
                "current_profile_mix": "Standard x1",
                "combination_shape": "mixed-profile",
                "selected_portfolio_score": 84.7,
                "portfolio_rationale": "mixed-profile Standard x1, Fortress x1 beat alternatives on portfolio utility 84.7; premium $1,020.00 vs target $1,350.00; selected Black Swan loss $4,200.00; mix quality 0.84; overlap penalty 0.00; complexity penalty 4.0; quantity scaling qualified the portfolio; qty 2->1; target proximity improved score but the target remained an objective rather than a gate.",
                "quantity_scaling_applied": True,
                "scaled_candidate_count": 1,
                "default_contract_quantity_total": 2,
                "selected_contract_quantity_total": 1,
                "default_projected_black_swan_loss_total": 5200.0,
                "default_projected_black_swan_loss_total_display": "$5,200.00",
                "next_addition_summary": "mixed-profile Standard x1, Fortress x1",
                "scaling_summary": "quantity scaling qualified the portfolio; qty 2->1; Black Swan $5,200.00->$3,380.00",
                "considered_candidates_summary": "Standard 79.0; Fortress 78.0; Aggressive 73.0",
                "combinations_evaluated_summary": "Standard x1 79.4, Fortress x1 74.6, Aggressive x1 68.5, Standard x1, Fortress x1 84.7",
                "selected_position_count": 2,
                "current_position_count": 1,
                "additional_position_available": True,
                "additional_position_count": 1,
                "selection_note": "Apollo selected the highest-scoring legal option at score 84.7: mixed-profile Standard x1, Fortress x1; portfolio premium $1,020.00 vs $1,350.00 target; target proximity improved Credit and Profit scoring, but the premium target remained a scoring objective rather than a gate; live Black Swan $4,200.00 of $13,500.00.",
            },
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
                    "percent_credit_captured_display": "81.2%",
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
            "activity_log": [
                {
                    "timestamp": "04-23-26 13:12 CST",
                    "category": "apollo-portfolio",
                    "title": "Apollo portfolio review",
                    "detail": "Selected option mixed-profile Standard x1, Fortress x1 won because mixed-profile Standard x1, Fortress x1 beat alternatives on portfolio utility 84.7; portfolio empty before entry yes; bootstrap trade yes; remaining live Black Swan capacity $9,300.00; more capacity remains yes.",
                }
            ],
            "decision_model": {
                "formula": "Score = safety 32 + history 22 + credit 14 + profit 12 + fit 10 + standard preference 10 - loss penalty 20.",
                "minimum_entry_score": 70.0,
                "weights": ["Safety 32%", "Standard Preference 10%"],
                "pricing_rule": "Simulated entry fill = short bid - long ask. Simulated exit fill = short ask - long bid.",
                "rotation_rule": "Talos only rotates after more than 75% of credit is captured, a replacement candidate is available, and the replacement score is equal or better.",
                "capture_close_rule": "Talos automatically liquidates any open Talos position once credit captured reaches 80.0% or greater.",
                "kairos_entry_rule": "Talos only opens Kairos when the best-trade override is Ready and the mode is Prime or Window Open.",
                "apollo_portfolio_rule": "Talos actively builds up to three Apollo positions from the current live state while keeping projected Black Swan loss under 10% of settled balance and using the daily premium target as a scoring objective rather than a gate.",
                "example_record": {
                    "trade_number": 12,
                    "talos_score_breakdown_items": [{"label": "Safety", "value": "+28.0"}],
                },
            },
            "learning_state": {
                "active_segment": "Kairos_LOW_VIX",
                "current_weights": {
                    "safety": 32.0,
                    "history": 22.0,
                    "credit": 14.0,
                    "profit": 12.0,
                    "fit": 10.0,
                    "standard_preference": 10.0,
                    "loss_penalty": 20.0,
                },
                "dataset_size": 8,
                "weighted_dataset_size": 8.7,
                "win_rate": 71.4,
                "black_swan_rate": 14.3,
                "adaptive_label": "Adaptive",
                "adaptive_enabled": True,
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
                    {"label": "Safety", "label_display": "Safety 32%", "weight_display": "32%", "color": "#ef4444", "shade_color": "#b91c1c"},
                    {"label": "History", "label_display": "History 22%", "weight_display": "22%", "color": "#f97316", "shade_color": "#c2410c"},
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
                "basis": "projected_black_swan_loss",
                "black_swan_roi_used": -0.44,
                "black_swan_roi_used_display": "-44.00%",
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
        self.assertIn(b"Version 7.2.11", response.data)
        self.assertIn(b"Apollo Portfolio Construction", response.data)
        self.assertIn(b"Premium Target", response.data)
        self.assertIn(b"Selected Mix Standard x1, Fortress x1", response.data)
        self.assertIn(b"Portfolio Shape Mixed Profile", response.data)
        self.assertIn(b"Portfolio Score 84.7", response.data)
        self.assertIn(b"Quantity Scaled Yes", response.data)
        self.assertIn(b"Selected Qty 1 / 2 Default", response.data)
        self.assertIn(b"Selected option:", response.data)
        self.assertIn(b"Premium target remains a scoring objective", response.data)
        self.assertIn(b"Talos actively builds Apollo from the current live state", response.data)
        self.assertIn(b"Scaling:", response.data)
        self.assertIn(b"Combination rationale:", response.data)
        self.assertIn(b"Candidates considered:", response.data)
        self.assertIn(b"Combinations evaluated:", response.data)
        self.assertIn(b"04-23-26 13:12 CST", response.data)
        self.assertIn(b"talos-activity-log-wrap", response.data)
        self.assertIn(b"Black Swan ROI -44.00%", response.data)
        self.assertIn(b"Talos Learning State", response.data)
        self.assertIn(b'<details class="talos-model-details talos-collapsible-section talos-apollo-portfolio-details">', response.data)
        self.assertIn(b'<details class="talos-model-details talos-collapsible-section talos-learning-state-details">', response.data)
        self.assertNotIn(b'<details class="talos-model-details talos-collapsible-section talos-apollo-portfolio-details" open>', response.data)
        self.assertNotIn(b'<details class="talos-model-details talos-collapsible-section talos-learning-state-details" open>', response.data)
        self.assertIn(b"Kairos_LOW_VIX", response.data)
        self.assertIn(b"Current Talos Orders", response.data)
        self.assertIn(b"Current Talos Commitment", response.data)
        self.assertIn(b"Projected Black Swan loss across currently open Talos positions against current account value.", response.data)
        self.assertIn(b"Projected Black Swan Loss $820.00", response.data)
        self.assertIn(b"talos-title-panel", response.data)
        self.assertIn(b"talos-visual-grid", response.data)
        self.assertIn(b'src="/static/icons/talos-icon.png"', response.data)
        self.assertIn(b"Win / Loss / Black Swan %", response.data)
        self.assertIn(b"Expiration", response.data)
        self.assertIn(b"Qty", response.data)
        self.assertIn(b"Strikes", response.data)
        self.assertIn(b"Score / Gate", response.data)
        self.assertIn(b"Exit Plan", response.data)
        self.assertIn(b"Credit Captured %", response.data)
        self.assertIn(b"81.2%", response.data)
        self.assertIn(b"5300", response.data)
        self.assertIn(b"Close 1", response.data)
        self.assertIn(b"Executed", response.data)
        self.assertIn(b"Explain score for Trade #12", response.data)
        self.assertIn(b"<details class=\"talos-trade-detail-panel\">", response.data)
        self.assertNotIn(b"<details class=\"talos-trade-detail-panel\" open>", response.data)
        self.assertIn(b"new_starting_balance", response.data)
        self.assertIn(b"Archive And Reset", response.data)
        self.assertIn(b"Talos Decision Model", response.data)
        self.assertIn(b"Apollo Portfolio Rule", response.data)
        self.assertIn(b"Capture Close Rule", response.data)
        self.assertIn(b"80.0% or greater", response.data)
        self.assertIn(b"standard preference 10", response.data.lower())
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