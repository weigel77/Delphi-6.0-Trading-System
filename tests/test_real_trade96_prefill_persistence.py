import sqlite3
import tempfile
import unittest
from contextlib import closing
from html.parser import HTMLParser
from pathlib import Path

from app import create_app


class _TradeFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_form = False
        self.form_action = ""
        self.fields: list[tuple[str, str]] = []
        self.current_select_name: str | None = None
        self.current_select_value: str | None = None
        self.current_textarea_name: str | None = None
        self.current_textarea_value = ""

    def handle_starttag(self, tag: str, attrs) -> None:
        attributes = dict(attrs)
        if tag == "form" and attributes.get("class") == "trade-form":
            self.in_form = True
            self.form_action = attributes.get("action") or ""
        if not self.in_form:
            return
        if tag == "input":
            name = attributes.get("name")
            if name:
                self.fields.append((name, attributes.get("value", "")))
        elif tag == "textarea":
            self.current_textarea_name = attributes.get("name")
            self.current_textarea_value = ""
        elif tag == "select":
            self.current_select_name = attributes.get("name")
            self.current_select_value = None
        elif tag == "option" and self.current_select_name is not None and "selected" in attributes:
            self.current_select_value = attributes.get("value", "")

    def handle_data(self, data: str) -> None:
        if self.current_textarea_name is not None:
            self.current_textarea_value += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "select" and self.current_select_name is not None:
            self.fields.append((self.current_select_name, self.current_select_value or ""))
            self.current_select_name = None
            self.current_select_value = None
        elif tag == "textarea" and self.current_textarea_name is not None:
            self.fields.append((self.current_textarea_name, self.current_textarea_value))
            self.current_textarea_name = None
            self.current_textarea_value = ""
        elif tag == "form" and self.in_form:
            self.in_form = False


class RealTrade96PrefillPersistenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "real-trade96-prefill.db"
        self.app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(self.database_path),
            }
        )
        self.client = self.app.test_client()
        self.trade_store = self.app.extensions["trade_store"]
        self.open_trade_manager = self.app.extensions["open_trade_manager"]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_real_trade_96_prefill_row_persists_after_reload(self) -> None:
        trade_id = self._create_reduced_trade_96()
        self.open_trade_manager.evaluate_trade_record = lambda target_trade_id: {
            "trade_id": target_trade_id,
            "trade_number": 96,
            "trade_mode": "Real",
            "contracts": 1,
            "current_spread_mark": 0.05,
            "current_total_close_cost_display": "$5.00",
            "evaluated_at_display": "2026-04-21 4:14 PM CDT",
            "alert_state": {
                "last_alert_type": None,
                "last_alert_priority": None,
                "last_alert_sent_at": None,
                "alert_count": 0,
            },
        }

        prefill_response = self.client.post(f"/management/open-trades/{trade_id}/prefill-close", follow_redirects=False)

        self.assertEqual(prefill_response.status_code, 302)
        self.assertIn(f"/trades/real/{trade_id}/edit#position-management", prefill_response.headers["Location"])

        edit_response = self.client.get(prefill_response.headers["Location"], follow_redirects=True)

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b"Manage Trade Prefill", edit_response.data)
        self.assertIn(b"Prefilled from Manage Trades", edit_response.data)
        self.assertIn(b'value="Reduced"', edit_response.data)

        parser = _TradeFormParser()
        parser.feed(edit_response.get_data(as_text=True))
        payload = self._build_payload(parser.fields)

        self.assertEqual(payload["close_event_id"], ["1", ""])
        self.assertEqual(payload["close_event_contracts_closed"], ["2", "1"])
        self.assertEqual(payload["close_event_actual_exit_value"], ["0.25", "0.05"])
        self.assertEqual(payload["close_event_method"], ["Reduce", "Manage Trade Prefill"])

        save_response = self.client.post(f"/trades/real/{trade_id}/edit", data=payload, follow_redirects=True)

        self.assertEqual(save_response.status_code, 200)
        self.assertNotIn(b"One or more close events could not be matched to this trade.", save_response.data)
        self.assertIn(b">Closed<", save_response.data)

        trade = self.trade_store.get_trade(trade_id)
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade["status"], "closed")
        self.assertEqual(trade["derived_status_raw"], "closed")
        self.assertEqual(trade["contracts_closed"], 3)
        self.assertEqual(trade["remaining_contracts"], 0)
        self.assertAlmostEqual(trade["gross_pnl"], 500.0)
        self.assertEqual(len(trade["close_events"]), 2)
        self.assertEqual(trade["close_events"][0]["event_type"], "reduce")
        self.assertEqual(trade["close_events"][1]["event_type"], "close")
        self.assertEqual(trade["close_events"][1]["close_method"], "Manage Trade Prefill")

        reload_response = self.client.get(f"/trades/real/{trade_id}/edit")

        self.assertEqual(reload_response.status_code, 200)
        self.assertIn(b'value="Closed"', reload_response.data)
        self.assertIn(b'id="position-summary-closed">3<', reload_response.data)
        self.assertIn(b'id="position-summary-remaining">0<', reload_response.data)
        reload_parser = _TradeFormParser()
        reload_parser.feed(reload_response.get_data(as_text=True))
        reload_payload = self._build_payload(reload_parser.fields)
        self.assertEqual(len(reload_payload["close_event_id"]), 2)
        self.assertNotEqual(reload_payload["close_event_id"][1], "")

    def _create_reduced_trade_96(self) -> int:
        create_payload = {
            "trade_number": "96",
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Aggressive",
            "status": "open",
            "trade_date": "2026-04-19",
            "entry_datetime": "2026-04-19T14:31",
            "expiration_date": "2026-04-21",
            "underlying_symbol": "SPX",
            "spx_at_entry": "7087.78",
            "vix_at_entry": "29.34",
            "structure_grade": "Good",
            "macro_grade": "Minor",
            "expected_move": "38.30",
            "expected_move_used": "38.30",
            "option_type": "Put Credit Spread",
            "short_strike": "7035",
            "long_strike": "7010",
            "spread_width": "25",
            "contracts": "3",
            "actual_entry_credit": "1.85",
            "distance_to_short": "52.78",
        }
        self.client.post("/trades/real/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute("SELECT id FROM trades WHERE trade_number = 96").fetchone()["id"]

        self.trade_store.reduce_trade(
            trade_id,
            {
                "contracts_closed": 2,
                "actual_exit_value": 0.25,
                "event_datetime": "2026-04-21T04:55",
                "close_method": "Reduce",
                "close_reason": "Reduced 2 contracts",
                "notes_exit": "Prefilled from Manage Trades at 2026-04-21 9:55 AM CDT using current total close cost $120.00.",
            },
        )
        return trade_id

    def _build_payload(self, fields: list[tuple[str, str]]) -> dict[str, object]:
        values: dict[str, list[str]] = {}
        for name, value in fields:
            values.setdefault(name, []).append(value)
        return {
            name: items if len(items) > 1 else items[0]
            for name, items in values.items()
        }


if __name__ == "__main__":
    unittest.main()