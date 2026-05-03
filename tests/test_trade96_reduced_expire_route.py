import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from app import create_app
from services.trade_store import form_trade_record


class Trade96ReducedExpireRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "trade96-route.db"
        self.app = create_app(
            {
                "TESTING": True,
                "TRADE_DATABASE": str(self.database_path),
            }
        )
        self.client = self.app.test_client()
        self.trade_store = self.app.extensions["trade_store"]

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_trade_96_route_allows_expiring_worthless_remaining_reduced_contracts(self) -> None:
        create_payload = {
            "trade_number": "96",
            "trade_mode": "real",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "candidate_profile": "Standard",
            "status": "open",
            "trade_date": "2026-04-21",
            "entry_datetime": "2026-04-21T09:35",
            "expiration_date": "2026-04-21",
            "underlying_symbol": "SPX",
            "spx_at_entry": "5298.40",
            "vix_at_entry": "18.20",
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": "42",
            "expected_move_used": "42",
            "option_type": "Put Credit Spread",
            "short_strike": "5250",
            "long_strike": "5245",
            "spread_width": "5",
            "contracts": "3",
            "actual_entry_credit": "1.85",
            "distance_to_short": "48.40",
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
                "event_datetime": "2026-04-21T09:55",
                "close_method": "Reduce",
                "close_reason": "Reduced 2 contracts",
                "notes_exit": "Scaled out of two contracts.",
            },
        )

        reduced_trade = self.trade_store.get_trade(trade_id)
        self.assertIsNotNone(reduced_trade)
        assert reduced_trade is not None
        self.assertEqual(reduced_trade["status"], "open")
        self.assertEqual(reduced_trade["derived_status_raw"], "reduced")
        self.assertEqual(reduced_trade["contracts_closed"], 2)
        self.assertEqual(reduced_trade["remaining_contracts"], 1)
        self.assertEqual(len(reduced_trade["close_events"]), 1)

        edit_response = self.client.get(f"/trades/real/{trade_id}/edit")

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b'value="Reduced"', edit_response.data)
        self.assertIn(b'value="2"', edit_response.data)
        self.assertIn(b'value="0.25"', edit_response.data)

        reduce_event = reduced_trade["close_events"][0]
        payload = form_trade_record(reduced_trade)
        payload.update(
            {
                "close_events_present": "1",
                "prefill_source": "",
                "system_version": payload.get("system_version", ""),
                "notes_entry": payload.get("notes_entry", ""),
                "notes_exit": payload.get("notes_exit", ""),
                "close_reason": payload.get("close_reason", ""),
                "close_event_id": [str(reduce_event["id"]), ""],
                "close_event_contracts_closed": ["2", "1"],
                "close_event_actual_exit_value": ["0.25", "0.00"],
                "close_event_method": ["Reduce", "Expire"],
                "close_event_event_datetime": [reduce_event.get("event_datetime") or "", ""],
                "close_event_notes_exit": [reduce_event.get("notes_exit") or "", ""],
            }
        )

        response = self.client.post(
            f"/trades/real/{trade_id}/edit",
            data=payload,
            follow_redirects=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b'message-error', response.data)
        self.assertIn(b">Expired<", response.data)

        updated_trade = self.trade_store.get_trade(trade_id)

        self.assertIsNotNone(updated_trade)
        assert updated_trade is not None
        self.assertEqual(updated_trade["status"], "expired")
        self.assertEqual(updated_trade["derived_status_raw"], "expired")
        self.assertEqual(updated_trade["contracts_closed"], 3)
        self.assertEqual(updated_trade["remaining_contracts"], 0)
        self.assertAlmostEqual(updated_trade["gross_pnl"], 505.0)
        self.assertEqual(len(updated_trade["close_events"]), 2)
        self.assertEqual(len({event["id"] for event in updated_trade["close_events"]}), 2)

        first_event, second_event = updated_trade["close_events"]
        self.assertEqual(first_event["event_type"], "reduce")
        self.assertEqual(first_event["contracts_closed"], 2)
        self.assertAlmostEqual(first_event["actual_exit_value"], 0.25)
        self.assertEqual(second_event["event_type"], "expire")
        self.assertEqual(second_event["contracts_closed"], 1)
        self.assertAlmostEqual(second_event["actual_exit_value"], 0.0)
        self.assertEqual(second_event["close_reason"], "Expired Worthless")


if __name__ == "__main__":
    unittest.main()