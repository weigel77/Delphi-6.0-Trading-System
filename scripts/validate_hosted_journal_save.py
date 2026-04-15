from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app
from services.runtime.private_access import RequestIdentity


class _FixedIdentityResolver:
    def __init__(self, identity: RequestIdentity):
        self.identity = identity

    def resolve_request_identity(self, request):
        return self.identity


def _build_payload(*, trade_mode: str, journal_name: str) -> dict[str, object]:
    token = uuid4().hex[:10]
    now = datetime.now().replace(second=0, microsecond=0)
    trade_date = now.date().isoformat()
    entry_datetime = now.isoformat(timespec="minutes")
    trade_number = int(now.strftime("%d%H%M"))
    return {
        "trade_number": trade_number,
        "trade_mode": trade_mode,
        "system_name": "Kairos",
        "journal_name": journal_name,
        "system_version": "5.4",
        "candidate_profile": "Subprime",
        "status": "open",
        "trade_date": trade_date,
        "entry_datetime": entry_datetime,
        "expiration_date": trade_date,
        "underlying_symbol": "SPX",
        "spx_at_entry": 6400.0,
        "vix_at_entry": 17.8,
        "structure_grade": "Bullish Confirmation",
        "macro_grade": "Improving",
        "expected_move": 38.0,
        "expected_move_used": 38.0,
        "option_type": "Put Credit Spread",
        "short_strike": 6380.0,
        "long_strike": 6375.0,
        "spread_width": 5.0,
        "contracts": 1,
        "candidate_credit_estimate": 1.15,
        "actual_entry_credit": 1.15,
        "distance_to_short": 20.0,
        "actual_distance_to_short": 20.0,
        "actual_em_multiple": 0.5263,
        "short_delta": 0.11,
        "notes_entry": f"Hosted journal save validation {token}",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate hosted Delphi 5.4 journal save against the configured Supabase backend.")
    parser.add_argument("--trade-mode", default="simulated", choices=["real", "simulated"])
    parser.add_argument("--journal-name", default="Hosted Save Validation")
    parser.add_argument("--keep", action="store_true", help="Keep the validation trade instead of deleting it after verification.")
    args = parser.parse_args()

    app = create_app(
        {
            "TESTING": True,
            "RUNTIME_TARGET": "hosted",
            "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
        }
    )
    app.extensions["request_identity_resolver"] = _FixedIdentityResolver(
        RequestIdentity(
            user_id="validation-user",
            email="bill@example.com",
            display_name="Bill",
            authenticated=True,
            auth_source="supabase-hosted",
        )
    )

    trade_store = app.extensions["trade_store"]
    client = app.test_client()
    payload = _build_payload(trade_mode=args.trade_mode, journal_name=args.journal_name)

    print(f"validation_trade_mode={args.trade_mode}")
    print(f"validation_trade_number={payload['trade_number']}")
    print(f"validation_journal_name={payload['journal_name']}")

    create_response = client.post(
        f"/hosted/journal?trade_mode={args.trade_mode}",
        data={key: str(value) if value is not None else "" for key, value in payload.items()},
        follow_redirects=False,
    )
    if create_response.status_code != 302:
        raise SystemExit(f"Hosted validation create did not redirect successfully. status={create_response.status_code}")

    created_trade = trade_store.find_recent_duplicate(payload, window_seconds=300)
    if not created_trade:
        raise SystemExit("Hosted validation create did not persist a readable trade.")
    trade_id = int(created_trade["id"])
    created_trade = trade_store.get_trade(trade_id)
    if not created_trade:
        raise SystemExit("Hosted validation save created a trade id but the trade could not be read back.")

    print(f"created_trade_id={trade_id}")
    print(f"created_trade_mode={created_trade.get('trade_mode')}")
    print(f"created_trade_status={created_trade.get('status')}")
    print(f"created_trade_notes={created_trade.get('notes_entry')}")

    if str(created_trade.get("trade_mode") or "") != args.trade_mode:
        raise SystemExit("Hosted validation save wrote the wrong trade mode.")
    if str(created_trade.get("system_version") or "") != "5.4":
        raise SystemExit("Hosted validation save wrote the wrong system version.")

    edit_payload = {
        "trade_number": str(created_trade.get("trade_number") or payload["trade_number"]),
        "trade_mode": str(created_trade.get("trade_mode") or args.trade_mode),
        "system_name": str(created_trade.get("system_name") or "Kairos"),
        "journal_name": str(created_trade.get("journal_name") or args.journal_name),
        "system_version": str(created_trade.get("system_version") or "5.4"),
        "candidate_profile": str(created_trade.get("candidate_profile") or "Subprime"),
        "status": str(created_trade.get("status") or "open"),
        "trade_date": str(created_trade.get("trade_date") or payload["trade_date"]),
        "entry_datetime": str(created_trade.get("entry_datetime") or payload["entry_datetime"]),
        "expiration_date": str(created_trade.get("expiration_date") or payload["expiration_date"]),
        "underlying_symbol": str(created_trade.get("underlying_symbol") or "SPX"),
        "spx_at_entry": str(created_trade.get("spx_at_entry") or payload["spx_at_entry"]),
        "vix_at_entry": str(created_trade.get("vix_at_entry") or payload["vix_at_entry"]),
        "structure_grade": str(created_trade.get("structure_grade") or payload["structure_grade"]),
        "macro_grade": str(created_trade.get("macro_grade") or payload["macro_grade"]),
        "expected_move": str(created_trade.get("expected_move") or payload["expected_move"]),
        "option_type": str(created_trade.get("option_type") or payload["option_type"]),
        "short_strike": str(created_trade.get("short_strike") or payload["short_strike"]),
        "long_strike": str(created_trade.get("long_strike") or payload["long_strike"]),
        "spread_width": str(created_trade.get("spread_width") or payload["spread_width"]),
        "contracts": str(created_trade.get("contracts") or payload["contracts"]),
        "candidate_credit_estimate": str(created_trade.get("candidate_credit_estimate") or payload["candidate_credit_estimate"]),
        "actual_entry_credit": str(created_trade.get("actual_entry_credit") or payload["actual_entry_credit"]),
        "distance_to_short": str(created_trade.get("distance_to_short") or payload["distance_to_short"]),
        "short_delta": str(created_trade.get("short_delta") or payload["short_delta"]),
        "notes_entry": str(created_trade.get("notes_entry") or payload["notes_entry"]),
        "notes_exit": "Hosted validation expire remaining",
        "close_reason": "Expired Worthless",
        "close_events_present": "1",
        "close_event_id": "",
        "close_event_contracts_closed": str(created_trade.get("remaining_contracts") or created_trade.get("contracts") or 1),
        "close_event_actual_exit_value": "0",
        "close_event_method": "Expire",
        "close_event_event_datetime": datetime.now().replace(second=0, microsecond=0).isoformat(timespec="minutes"),
        "close_event_notes_exit": "Hosted validation expire remaining",
    }

    edit_response = client.post(
        f"/hosted/journal/{args.trade_mode}/{trade_id}/edit",
        data=edit_payload,
        follow_redirects=False,
    )
    if edit_response.status_code != 302:
        raise SystemExit(f"Hosted validation edit/save did not redirect successfully. status={edit_response.status_code}")

    updated_trade = trade_store.get_trade(trade_id)
    if not updated_trade:
        raise SystemExit("Hosted validation edit/save could not reload the updated trade.")
    if str(updated_trade.get("derived_status_raw") or updated_trade.get("status") or "") not in {"expired", "closed"}:
        raise SystemExit("Hosted validation edit/save did not persist the expire remaining event.")
    if not updated_trade.get("close_events"):
        raise SystemExit("Hosted validation edit/save did not persist a close event.")

    print(f"updated_trade_status={updated_trade.get('derived_status_raw') or updated_trade.get('status')}")
    print(f"updated_close_event_count={len(updated_trade.get('close_events') or [])}")

    if args.keep:
        print("cleanup=skipped")
    else:
        trade_store.delete_trade(trade_id)
        deleted_trade = trade_store.get_trade(trade_id)
        if deleted_trade is not None:
            raise SystemExit("Hosted validation cleanup failed; the validation trade still exists.")
        print("cleanup=deleted")

    print("hosted_journal_save_validation=passed")


if __name__ == "__main__":
    main()
