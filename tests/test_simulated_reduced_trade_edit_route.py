import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from app import create_app


class SimulatedReducedTradeEditRouteTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "simulated-reduced-edit.db"
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

    def test_simulated_route_allows_expiring_remaining_contracts_for_reduced_trade(self) -> None:
        trade_id = self._create_reduced_simulated_trade(trade_number=100, contracts=5, contracts_closed=2, exit_value=1.90)

        edit_response = self.client.get(f"/trades/simulated/{trade_id}/edit")

        self.assertEqual(edit_response.status_code, 200)
        self.assertIn(b'action="/trades/simulated/', edit_response.data)
        self.assertIn(b'value="Reduced"', edit_response.data)

        existing_event = self._extract_close_event_row(edit_response.get_data(as_text=True))
        payload = self._build_edit_payload(
            trade_number="100",
            contracts="5",
            existing_event=existing_event,
            appended_event={
                "close_event_id": "",
                "close_event_contracts_closed": "3",
                "close_event_actual_exit_value": "0.00",
                "close_event_method": "Expire",
                "close_event_event_datetime": "",
                "close_event_notes_exit": "",
            },
        )

        response = self.client.post(f"/trades/simulated/{trade_id}/edit", data=payload, follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"One or more close events could not be matched to this trade.", response.data)
        self.assertIn(b">Expired<", response.data)

        trade = self.trade_store.get_trade(trade_id)
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade["status"], "expired")
        self.assertEqual(trade["derived_status_raw"], "expired")
        self.assertEqual(trade["contracts_closed"], 5)
        self.assertEqual(trade["remaining_contracts"], 0)
        self.assertAlmostEqual(trade["gross_pnl"], 545.0)
        self.assertEqual(len(trade["close_events"]), 2)
        self.assertEqual(trade["close_events"][0]["id"], int(existing_event["close_event_id"]))
        self.assertEqual(trade["close_events"][1]["event_type"], "expire")
        self.assertEqual(trade["close_events"][1]["contracts_closed"], 3)

    def test_simulated_route_allows_additional_reduction_for_reduced_trade(self) -> None:
        trade_id = self._create_reduced_simulated_trade(trade_number=101, contracts=5, contracts_closed=2, exit_value=1.90)
        edit_response = self.client.get(f"/trades/simulated/{trade_id}/edit")
        existing_event = self._extract_close_event_row(edit_response.get_data(as_text=True))

        payload = self._build_edit_payload(
            trade_number="101",
            contracts="5",
            existing_event=existing_event,
            appended_event={
                "close_event_id": "",
                "close_event_contracts_closed": "1",
                "close_event_actual_exit_value": "0.80",
                "close_event_method": "Reduce",
                "close_event_event_datetime": "",
                "close_event_notes_exit": "",
            },
        )

        response = self.client.post(f"/trades/simulated/{trade_id}/edit", data=payload, follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"One or more close events could not be matched to this trade.", response.data)
        self.assertIn(b">Reduced<", response.data)

        trade = self.trade_store.get_trade(trade_id)
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade["status"], "open")
        self.assertEqual(trade["derived_status_raw"], "reduced")
        self.assertEqual(trade["contracts_closed"], 3)
        self.assertEqual(trade["remaining_contracts"], 2)
        self.assertAlmostEqual(trade["gross_pnl"], 465.0)
        self.assertEqual(len(trade["close_events"]), 2)
        self.assertEqual(trade["close_events"][1]["event_type"], "reduce")

    def test_simulated_route_allows_final_close_after_prior_reductions(self) -> None:
        trade_id = self._create_reduced_simulated_trade(trade_number=102, contracts=5, contracts_closed=2, exit_value=1.90)
        edit_response = self.client.get(f"/trades/simulated/{trade_id}/edit")
        existing_event = self._extract_close_event_row(edit_response.get_data(as_text=True))

        payload = self._build_edit_payload(
            trade_number="102",
            contracts="5",
            existing_event=existing_event,
            appended_event={
                "close_event_id": "",
                "close_event_contracts_closed": "3",
                "close_event_actual_exit_value": "0.50",
                "close_event_method": "Close",
                "close_event_event_datetime": "",
                "close_event_notes_exit": "",
            },
        )

        response = self.client.post(f"/trades/simulated/{trade_id}/edit", data=payload, follow_redirects=True)

        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"One or more close events could not be matched to this trade.", response.data)
        self.assertIn(b">Closed<", response.data)

        trade = self.trade_store.get_trade(trade_id)
        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade["status"], "closed")
        self.assertEqual(trade["derived_status_raw"], "closed")
        self.assertEqual(trade["contracts_closed"], 5)
        self.assertEqual(trade["remaining_contracts"], 0)
        self.assertAlmostEqual(trade["gross_pnl"], 395.0)
        self.assertEqual(len(trade["close_events"]), 2)
        self.assertEqual(trade["close_events"][1]["event_type"], "close")

    def _create_reduced_simulated_trade(self, *, trade_number: int, contracts: int, contracts_closed: int, exit_value: float) -> int:
        create_payload = {
            "trade_number": str(trade_number),
            "trade_mode": "simulated",
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
            "contracts": str(contracts),
            "actual_entry_credit": "1.85",
            "distance_to_short": "48.40",
        }
        self.client.post("/trades/simulated/new", data=create_payload, follow_redirects=True)

        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            trade_id = connection.execute(
                "SELECT id FROM trades WHERE trade_number = ? AND trade_mode = 'simulated'",
                (trade_number,),
            ).fetchone()["id"]

        self.trade_store.reduce_trade(
            trade_id,
            {
                "contracts_closed": contracts_closed,
                "actual_exit_value": exit_value,
                "event_datetime": "2026-04-21T10:01",
                "close_method": "Manage Trade Prefill",
                "close_reason": f"Closed {contracts_closed} contracts",
                "notes_exit": "Prefilled from Manage Trades at 2026-04-21 10:01 AM CDT using current total close cost $950.00.",
            },
        )
        return trade_id

    def _build_edit_payload(self, *, trade_number: str, contracts: str, existing_event: dict[str, str], appended_event: dict[str, str]) -> dict[str, object]:
        payload: dict[str, object] = {
            "trade_number": trade_number,
            "trade_mode": "simulated",
            "system_name": "Apollo",
            "journal_name": "Apollo Main",
            "system_version": "",
            "candidate_profile": "Standard",
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
            "expected_move_source": "original",
            "option_type": "Put Credit Spread",
            "short_strike": "5250",
            "long_strike": "5245",
            "spread_width": "5",
            "contracts": contracts,
            "candidate_credit_estimate": "1.85",
            "actual_entry_credit": "1.85",
            "distance_to_short": "48.40",
            "short_delta": "",
            "close_reason": "",
            "notes_entry": "",
            "notes_exit": "",
            "prefill_source": "",
            "close_events_present": "1",
            "close_event_id": [existing_event["close_event_id"], appended_event["close_event_id"]],
            "close_event_contracts_closed": [existing_event["close_event_contracts_closed"], appended_event["close_event_contracts_closed"]],
            "close_event_actual_exit_value": [existing_event["close_event_actual_exit_value"], appended_event["close_event_actual_exit_value"]],
            "close_event_method": [existing_event["close_event_method"], appended_event["close_event_method"]],
            "close_event_event_datetime": [existing_event["close_event_event_datetime"], appended_event["close_event_event_datetime"]],
            "close_event_notes_exit": [existing_event["close_event_notes_exit"], appended_event["close_event_notes_exit"]],
        }
        return payload

    def _extract_close_event_row(self, html: str) -> dict[str, str]:
        patterns = {
            "close_event_id": r'name="close_event_id" value="([^"]*)"',
            "close_event_event_datetime": r'name="close_event_event_datetime" value="([^"]*)"',
            "close_event_notes_exit": r'name="close_event_notes_exit" value="([^"]*)"',
            "close_event_contracts_closed": r'name="close_event_contracts_closed" type="number" min="1" step="1" value="([^"]*)"',
            "close_event_actual_exit_value": r'name="close_event_actual_exit_value" type="number" min="0" step="0.01" value="([^"]*)"',
            "close_event_method": r'<option value="([^"]*)" selected>',
        }
        extracted: dict[str, str] = {}
        for key, pattern in patterns.items():
            match = re.search(pattern, html)
            if not match:
                raise AssertionError(f"Could not extract {key} from simulated edit form.")
            extracted[key] = match.group(1)
        return extracted


if __name__ == "__main__":
    unittest.main()