from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path
from tempfile import mkdtemp
from threading import Thread
from typing import Any, Dict
import shutil

from werkzeug.serving import make_server

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module

try:
    from playwright.sync_api import sync_playwright
except ImportError as exc:  # pragma: no cover - manual validation helper
    raise SystemExit(
        "Playwright is required for this validation script. Install it with 'py -3.12 -m pip install playwright' and run 'py -3.12 -m playwright install chromium'."
    ) from exc


def build_apollo_render_payload() -> Dict[str, Any]:
    candidate_prefill = {
        "system_name": "Apollo",
        "journal_name": "Apollo Main",
        "system_version": "2.0",
        "candidate_profile": "Standard",
        "expiration_date": "2026-04-06",
        "underlying_symbol": "SPX",
        "spx_at_entry": "6500",
        "vix_at_entry": "20",
        "structure_grade": "Good",
        "macro_grade": "None",
        "expected_move": "45",
        "option_type": "Put Credit Spread",
        "short_strike": "6450",
        "long_strike": "6445",
        "spread_width": "5",
        "contracts": "2",
        "candidate_credit_estimate": "1.2",
        "distance_to_short": "50",
        "short_delta": "0.12",
        "notes_entry": "Prefilled from Apollo candidate card.",
        "prefill_source": "apollo",
    }
    return {
        "title": "Apollo Gate 1 -- SPX Structure",
        "provider_name": "Test Provider",
        "status": "Allowed",
        "status_class": "allowed",
        "local_datetime": "Fri 2026-04-03 10:00 AM CDT",
        "spx_value": "6,500.00",
        "spx_as_of": "Now",
        "vix_value": "20.00",
        "vix_as_of": "Now",
        "macro_title": "Apollo Gate 2 -- Macro Event",
        "macro_source": "MarketWatch",
        "macro_grade": "None",
        "macro_grade_class": "good",
        "macro_target_day": "Mon Apr 06, 2026",
        "macro_checked_at": "Fri 2026-04-03 10:00 AM CDT",
        "macro_checked_dates": "2026-04-06",
        "macro_available": "Yes",
        "macro_event_count": 0,
        "macro_major_detected": "No",
        "macro_explanation": "—",
        "macro_diagnostic": {},
        "macro_source_attempts": [],
        "macro_events": [],
        "macro_major_events": [],
        "macro_minor_events": [],
        "next_market_day": "2026-04-06",
        "next_market_day_note": "—",
        "holiday_filter_applied": "No",
        "skipped_holiday_name": "—",
        "candidate_date_considered": "2026-04-06",
        "structure_available": False,
        "structure_preferred_source": "Schwab",
        "structure_attempted_sources": [],
        "structure_fallback_reason": "",
        "structure_source_used": "Schwab",
        "structure_grade": "Allowed",
        "structure_grade_class": "allowed",
        "structure_trend_classification": "Balanced",
        "structure_damage_classification": "Contained",
        "structure_summary": "—",
        "structure_message": "Test structure",
        "structure_session_note": "",
        "structure_rules": [],
        "structure_chart": {"available": False, "points": []},
        "structure_session_high": "6,510.00",
        "structure_session_low": "6,490.00",
        "structure_current_price": "6,500.00",
        "structure_range_position": "50.00%",
        "structure_vwap": "6,500.00",
        "structure_ema8": "6,499.00",
        "structure_ema21": "6,495.00",
        "structure_recent_price_action": "Stable",
        "structure_session_window": "09:30 to 16:00",
        "option_chain_success": True,
        "option_chain_status": "Ready",
        "option_chain_status_class": "good",
        "option_chain_source": "Schwab",
        "option_chain_heading_date": "Mon Apr 06, 2026",
        "option_chain_symbol_requested": "SPX",
        "option_chain_expiration_target": "2026-04-06",
        "option_chain_expiration": "2026-04-06",
        "option_chain_expiration_count": 1,
        "option_chain_puts_count": 12,
        "option_chain_calls_count": 12,
        "option_chain_rows_displayed": 24,
        "option_chain_display_puts_count": 12,
        "option_chain_display_calls_count": 12,
        "option_chain_min_premium_target": "1.00",
        "option_chain_rows_setting": "Adaptive",
        "option_chain_grouping": "Puts ascending → Calls ascending",
        "option_chain_strike_range": "—",
        "option_chain_message": "—",
        "option_chain_preview_rows": [],
        "option_chain_final_symbol": "SPX",
        "option_chain_final_expiration_sent": "2026-04-06",
        "option_chain_request_attempt_used": "1",
        "option_chain_raw_params_sent": {},
        "option_chain_error_detail": "—",
        "option_chain_attempt_results": [],
        "trade_candidates_title": "Apollo Gate 3 -- Trade Candidates",
        "trade_candidates_status": "Ready",
        "trade_candidates_status_class": "good",
        "trade_candidates_message": "Three candidates ready for review.",
        "trade_candidates_count": 3,
        "trade_candidates_count_label": "3 Modes",
        "trade_candidates_underlying_price": "6,500.00",
        "trade_candidates_expected_move": "45.00",
        "trade_candidates_expected_move_range": "6,455.00 to 6,545.00",
        "trade_candidates_diagnostics": {"selected_spreads": 3, "evaluated_spread_details": []},
        "trade_candidates_short_barrier_put": "<= 6450",
        "trade_candidates_short_barrier_call": ">= 6550",
        "trade_candidates_credit_map": {"available": False},
        "trade_candidates_items": [
            {
                "mode_key": "fortress",
                "mode_label": "Fortress",
                "mode_descriptor": "Maximum safety",
                "available": True,
                "no_trade_message": "",
                "position_label": "Put Spread",
                "short_strike": "6440",
                "long_strike": "6435",
                "recommended_contract_size": "5",
                "distance_points": "60",
                "em_multiple": "1.33x EM",
                "premium_received_dollars": "$500",
                "premium_probability": "88%",
                "routine_loss": "$300",
                "routine_probability": "8%",
                "black_swan_loss": "$600",
                "tail_probability": "4%",
                "max_loss": "$1,000",
                "max_probability": "<1%",
                "max_loss_per_contract": "100",
                "risk_cap_status": "Within risk cap",
                "rationale": ["Fortress rationale"],
                "exit_plan": ["Fortress exit"],
                "diagnostics": ["Fortress diagnostic"],
                "prefill_fields": {**candidate_prefill, "candidate_profile": "Fortress", "short_strike": "6440", "long_strike": "6435"},
            },
            {
                "mode_key": "standard",
                "mode_label": "Standard",
                "mode_descriptor": "Balanced core",
                "available": True,
                "no_trade_message": "",
                "position_label": "Put Spread",
                "short_strike": "6450",
                "long_strike": "6445",
                "recommended_contract_size": "8",
                "distance_points": "50",
                "em_multiple": "1.11x EM",
                "premium_received_dollars": "$960",
                "premium_probability": "85%",
                "routine_loss": "$400",
                "routine_probability": "10%",
                "black_swan_loss": "$700",
                "tail_probability": "5%",
                "max_loss": "$1,200",
                "max_probability": "<1%",
                "max_loss_per_contract": "120",
                "risk_cap_status": "Within risk cap",
                "rationale": ["Standard rationale"],
                "exit_plan": ["Standard exit"],
                "diagnostics": ["Standard diagnostic"],
                "prefill_fields": candidate_prefill,
            },
            {
                "mode_key": "aggressive",
                "mode_label": "Aggressive",
                "mode_descriptor": "Higher premium",
                "available": True,
                "no_trade_message": "",
                "position_label": "Put Spread",
                "short_strike": "6460",
                "long_strike": "6455",
                "recommended_contract_size": "10",
                "distance_points": "40",
                "em_multiple": "0.89x EM",
                "premium_received_dollars": "$1,250",
                "premium_probability": "82%",
                "routine_loss": "$550",
                "routine_probability": "12%",
                "black_swan_loss": "$850",
                "tail_probability": "6%",
                "max_loss": "$1,500",
                "max_probability": "<1%",
                "max_loss_per_contract": "150",
                "risk_cap_status": "Within risk cap",
                "rationale": ["Aggressive rationale"],
                "exit_plan": ["Aggressive exit"],
                "diagnostics": ["Aggressive diagnostic"],
                "prefill_fields": {**candidate_prefill, "candidate_profile": "Aggressive", "short_strike": "6460", "long_strike": "6455"},
            },
        ],
        "apollo_trigger_source": "autorun URL",
        "apollo_trigger_note": "Apollo was triggered by autorun URL.",
        "reasons": [],
    }


class ServerThread(Thread):
    def __init__(self, server) -> None:
        super().__init__(daemon=True)
        self.server = server

    def run(self) -> None:
        self.server.serve_forever()

    def shutdown(self) -> None:
        self.server.shutdown()


def count_trades(db_path: Path) -> int:
    with sqlite3.connect(db_path) as connection:
        return int(connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0])


def main() -> None:
    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(exist_ok=True)
    screenshot_path = artifacts_dir / "apollo_gate3_buttons.png"

    original_execute_apollo_precheck = app_module.execute_apollo_precheck
    app_module.execute_apollo_precheck = lambda *args, **kwargs: build_apollo_render_payload()

    temp_dir = mkdtemp(prefix="horme-ui-")
    try:
        db_path = Path(temp_dir) / "horme_trades.db"
        flask_app = app_module.create_app({"TESTING": True, "TRADE_DATABASE": str(db_path)})
        server = make_server("127.0.0.1", 5001, flask_app)
        server_thread = ServerThread(server)
        server_thread.start()
        time.sleep(0.5)

        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                page = browser.new_page(viewport={"width": 1720, "height": 1800})
                page.goto("http://127.0.0.1:5001/apollo?autorun=1", wait_until="networkidle")

                cards = page.locator(".apollo-candidate-card")
                if cards.count() != 3:
                    raise AssertionError(f"Expected 3 candidate cards, found {cards.count()}.")

                visible_button_total = 0
                for mode_name in ("Fortress", "Standard", "Aggressive"):
                    card = page.locator(".apollo-candidate-card", has=page.locator(f"text={mode_name}")).first
                    if not card.is_visible():
                        raise AssertionError(f"{mode_name} card is not visible.")
                    real_button = card.get_by_role("button", name="Send to Real Trades")
                    simulated_button = card.get_by_role("button", name="Send to Simulated Trades")
                    if not real_button.is_visible() or not simulated_button.is_visible():
                        raise AssertionError(f"Transfer buttons are not visibly rendered in the {mode_name} card.")
                    visible_button_total += 2

                page.screenshot(path=str(screenshot_path), full_page=True)

                trade_count_before_transfer = count_trades(db_path)
                aggressive_card = page.locator(".apollo-candidate-card", has=page.locator("text=Aggressive")).first
                aggressive_form = aggressive_card.locator("form").nth(1)
                expected_short_strike = aggressive_form.locator('input[name="short_strike"]').input_value()
                expected_long_strike = aggressive_form.locator('input[name="long_strike"]').input_value()
                aggressive_card.get_by_role("button", name="Send to Simulated Trades").click()
                page.wait_for_url("**/trades/simulated?prefill=1**")

                if count_trades(db_path) != trade_count_before_transfer:
                    raise AssertionError("A trade was saved during transfer. Transfer must only prefill.")
                if not page.locator("text=Apollo candidate data is loaded into this draft").is_visible():
                    raise AssertionError("Prefill banner did not appear on the simulated trade page.")
                if page.locator("#trade_mode").input_value() != "simulated":
                    raise AssertionError("Simulated trade page did not receive the correct trade mode.")
                if page.locator("#candidate_profile").input_value() != "Aggressive":
                    raise AssertionError("Candidate profile was not prefilled for the simulated trade draft.")
                if page.locator("#short_strike").input_value() != expected_short_strike:
                    raise AssertionError("Short strike was not prefilled correctly on the simulated trade page.")
                if page.locator("#long_strike").input_value() != expected_long_strike:
                    raise AssertionError("Long strike was not prefilled correctly on the simulated trade page.")

                page.get_by_role("button", name="Save Trade").click()
                page.wait_for_url("**/trades/simulated")
                trade_count_after_save = count_trades(db_path)
                if trade_count_after_save != trade_count_before_transfer + 1:
                    raise AssertionError("Save Trade did not create exactly one record in the simulated journal.")

                page.reload(wait_until="networkidle")
                if count_trades(db_path) != trade_count_after_save:
                    raise AssertionError("Refreshing after save created a duplicate trade.")

                page.goto("http://127.0.0.1:5001/apollo?autorun=1", wait_until="networkidle")
                fortress_card = page.locator(".apollo-candidate-card", has=page.locator("text=Fortress")).first
                fortress_card.get_by_role("button", name="Send to Real Trades").click()
                page.wait_for_url("**/trades/real?prefill=1**")

                if count_trades(db_path) != trade_count_after_save:
                    raise AssertionError("Transfer to the real-trades journal saved a trade before Save Trade was clicked.")
                if page.locator("#trade_mode").input_value() != "real":
                    raise AssertionError("Real trade page did not receive the correct trade mode.")
                if page.locator("#candidate_profile").input_value() != "Fortress":
                    raise AssertionError("Candidate profile was not prefilled for the real trade draft.")

                browser.close()

                print(f"visible_button_total={visible_button_total}")
                print(f"screenshot={screenshot_path.resolve()}")
                print("ui_validation=passed")
        finally:
            server_thread.shutdown()
            server_thread.join(timeout=5)
            server.server_close()
            time.sleep(0.2)
            app_module.execute_apollo_precheck = original_execute_apollo_precheck
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
