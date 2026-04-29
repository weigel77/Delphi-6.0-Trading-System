from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from threading import Thread

from werkzeug.serving import make_server

from app import create_app

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - optional browser coverage
    sync_playwright = None
    PlaywrightError = Exception


class ServerThread(Thread):
    def __init__(self, server) -> None:
        super().__init__(daemon=True)
        self.server = server

    def run(self) -> None:
        self.server.serve_forever()

    def shutdown(self) -> None:
        self.server.shutdown()


@unittest.skipIf(sync_playwright is None, "Playwright is not installed.")
class PerformanceBrowserInteractionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "horme_trades.db"
        self.app = create_app({"TESTING": True, "TRADE_DATABASE": str(self.database_path)})
        self.client = self.app.test_client()
        self._seed_dashboard_trades()

        self.server = make_server("127.0.0.1", 0, self.app)
        self.server_thread = ServerThread(self.server)
        self.server_thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

        self.playwright = sync_playwright().start()
        try:
            self.browser = self.playwright.chromium.launch(headless=True)
        except PlaywrightError as exc:  # pragma: no cover - depends on local browser install
            self.playwright.stop()
            self.server_thread.shutdown()
            self.server_thread.join(timeout=5)
            self.server.server_close()
            self.temp_dir.cleanup()
            self.skipTest(f"Playwright browser could not launch: {exc}")
        self.page = self.browser.new_page(viewport={"width": 1500, "height": 1200})

    def tearDown(self) -> None:
        if hasattr(self, "page"):
            self.page.close()
        if hasattr(self, "browser"):
            self.browser.close()
        if hasattr(self, "playwright"):
            self.playwright.stop()
        if hasattr(self, "server_thread"):
            self.server_thread.shutdown()
            self.server_thread.join(timeout=5)
        if hasattr(self, "server"):
            self.server.server_close()
        self.temp_dir.cleanup()

    def _create_trade(self, trade_mode: str, **overrides) -> None:
        payload = {
            "trade_number": overrides.pop("trade_number", ""),
            "trade_mode": trade_mode,
            "system_name": overrides.pop("system_name", "Apollo"),
            "journal_name": overrides.pop("journal_name", "Apollo Main"),
            "system_version": overrides.pop("system_version", "2.0"),
            "candidate_profile": overrides.pop("candidate_profile", "Legacy"),
            "status": overrides.pop("status", "closed"),
            "trade_date": overrides.pop("trade_date", "2026-04-01"),
            "entry_datetime": overrides.pop("entry_datetime", ""),
            "expiration_date": overrides.pop("expiration_date", "2026-04-04"),
            "underlying_symbol": overrides.pop("underlying_symbol", "SPX"),
            "spx_at_entry": overrides.pop("spx_at_entry", ""),
            "vix_at_entry": overrides.pop("vix_at_entry", ""),
            "structure_grade": overrides.pop("structure_grade", "Good"),
            "macro_grade": overrides.pop("macro_grade", "None"),
            "expected_move": overrides.pop("expected_move", ""),
            "option_type": overrides.pop("option_type", "Put Credit Spread"),
            "short_strike": overrides.pop("short_strike", "6450"),
            "long_strike": overrides.pop("long_strike", "6445"),
            "spread_width": overrides.pop("spread_width", "5"),
            "contracts": overrides.pop("contracts", "1"),
            "candidate_credit_estimate": overrides.pop("candidate_credit_estimate", ""),
            "actual_entry_credit": overrides.pop("actual_entry_credit", "1.5"),
            "distance_to_short": overrides.pop("distance_to_short", ""),
            "short_delta": overrides.pop("short_delta", ""),
            "notes_entry": overrides.pop("notes_entry", ""),
            "prefill_source": overrides.pop("prefill_source", ""),
            "exit_datetime": overrides.pop("exit_datetime", ""),
            "spx_at_exit": overrides.pop("spx_at_exit", ""),
            "actual_exit_value": overrides.pop("actual_exit_value", "0"),
            "close_method": overrides.pop("close_method", ""),
            "close_reason": overrides.pop("close_reason", ""),
            "notes_exit": overrides.pop("notes_exit", ""),
        }
        payload.update(overrides)
        response = self.client.post(f"/trades/{trade_mode}/new", data=payload, follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def _seed_dashboard_trades(self) -> None:
        self._create_trade(
            "real",
            system_name="Apollo",
            candidate_profile="Standard",
            trade_date="2026-04-01",
            expiration_date="2026-04-01",
            macro_grade="None",
            structure_grade="Good",
            actual_entry_credit="2.0",
            actual_exit_value="0.5",
        )
        self._create_trade(
            "real",
            system_name="Kairos",
            candidate_profile="",
            trade_date="2026-04-02",
            expiration_date="2026-04-02",
            macro_grade="Major",
            structure_grade="Poor",
            spread_width="10",
            actual_entry_credit="1.0",
            actual_exit_value="5.0",
            close_reason="Black Swan defense failure",
        )
        self._create_trade(
            "simulated",
            system_name="Apollo",
            candidate_profile="Aggressive",
            status="open",
            trade_date="2026-04-04",
            expiration_date="2026-04-04",
            macro_grade="None",
            structure_grade="Good",
            actual_entry_credit="1.25",
            actual_exit_value="",
            close_reason="",
        )

    def test_equity_curve_point_popup_opens_updates_and_closes(self) -> None:
        self.page.goto(f"{self.base_url}/performance", wait_until="networkidle")

        popup = self.page.locator(".performance-line-popup")
        points = self.page.locator(".performance-line-dot")

        self.assertEqual(self.page.locator(".performance-line-tooltip").count(), 0)
        self.assertEqual(points.count(), 2)
        self.assertTrue(popup.is_hidden())

        points.nth(0).click()
        self.assertTrue(popup.is_visible())
        first_text = popup.text_content() or ""
        self.assertIn("Date", first_text)
        self.assertIn("2026-04-01", first_text)
        self.assertIn("Cumulative P/L", first_text)
        self.assertIn("$150", first_text)
        self.assertIn("Trade P/L", first_text)
        self.assertIn("Win", first_text)

        points.nth(1).click()
        self.assertEqual(self.page.locator(".performance-line-popup").count(), 1)
        self.assertTrue(popup.is_visible())
        second_text = popup.text_content() or ""
        self.assertIn("2026-04-02", second_text)
        self.assertIn("-$250", second_text)
        self.assertIn("-$400", second_text)
        self.assertIn("Black Swan", second_text)
        self.assertNotEqual(first_text, second_text)

        self.page.mouse.click(20, 20)
        self.assertTrue(popup.is_hidden())


if __name__ == "__main__":
    unittest.main()
