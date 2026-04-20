from __future__ import annotations

import json
import shutil
import sys
import tempfile
import sqlite3
from pathlib import Path
from time import perf_counter

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app import (
    _clear_hosted_payload_cache,
    build_hosted_apollo_live_payload,
    build_hosted_open_trade_management_payload,
    create_app,
)
from services.runtime.private_access import RequestIdentity


class _FixedIdentityResolver:
    def __init__(self, identity: RequestIdentity):
        self.identity = identity

    def resolve_request_identity(self, request):
        return self.identity


def _prepare_validation_database(repo_root: Path) -> tuple[tempfile.TemporaryDirectory, Path]:
    source_database = repo_root / "instance" / "horme_trades.db"
    temp_dir = tempfile.TemporaryDirectory()
    cloned_database = Path(temp_dir.name) / "hosted-validation-horme_trades.db"
    shutil.copy2(source_database, cloned_database)
    with sqlite3.connect(cloned_database) as connection:
        connection.execute(
            "UPDATE trades SET close_method = 'Expire' WHERE close_method = 'Expiration'"
        )
        connection.commit()
    return temp_dir, cloned_database


def _build_hosted_app(database_path: Path):
    local_app = create_app(
        {
            "TESTING": True,
            "RUNTIME_TARGET": "local",
            "TRADE_DATABASE": str(database_path),
        }
    )
    hosted_app = create_app(
        {
            "TESTING": True,
            "RUNTIME_TARGET": "hosted",
            "DELPHI_HOSTED_ALLOWED_EMAILS": "validator@example.com",
            "TRADE_DATABASE": str(database_path),
        }
    )
    hosted_app.extensions["request_identity_resolver"] = _FixedIdentityResolver(
        RequestIdentity(
            user_id="local-validator",
            email="validator@example.com",
            display_name="Local Validator",
            authenticated=True,
            auth_source="local-script",
        )
    )
    hosted_app.extensions["open_trade_manager"] = local_app.extensions["open_trade_manager"]
    hosted_app.extensions["apollo_service"] = local_app.extensions["apollo_service"]
    return hosted_app


def _measure_route(client, path: str) -> dict[str, object]:
    started_at = perf_counter()
    response = client.get(path)
    elapsed_ms = round((perf_counter() - started_at) * 1000.0, 2)
    html = response.get_data(as_text=True)
    return {
        "path": path,
        "status_code": response.status_code,
        "elapsed_ms": elapsed_ms,
        "html": html,
    }


def _round_trip(hosted_app, *, cache_key: str, path: str, builder):
    with hosted_app.app_context():
        _clear_hosted_payload_cache(cache_key, app=hosted_app)
    client = hosted_app.test_client()
    route_result = _measure_route(client, path)
    with hosted_app.app_context():
        payload = builder(app=hosted_app, force_refresh=True) if cache_key == "hosted:apollo:live" else builder(app=hosted_app)
    return route_result, payload


def main() -> int:
    repo_root = REPO_ROOT
    temp_dir, database_path = _prepare_validation_database(repo_root)
    try:
        hosted_app = _build_hosted_app(database_path)

        manage_route, manage_payload = _round_trip(
            hosted_app,
            cache_key="hosted:open-trades",
            path="/hosted/manage-trades",
            builder=build_hosted_open_trade_management_payload,
        )
        apollo_route, apollo_payload = _round_trip(
            hosted_app,
            cache_key="hosted:apollo:live",
            path="/hosted/apollo?autorun=1",
            builder=build_hosted_apollo_live_payload,
        )

        manage_perf = manage_payload.get("performance") or {}
        apollo_service = hosted_app.extensions["apollo_service"]
        apollo_precheck = apollo_service.run_precheck(force_refresh=True)
        apollo_perf = apollo_precheck.get("performance") or {}
        prior_manage_apollo = apollo_perf

        manage_current_total = float(manage_perf.get("total_seconds") or 0.0)
        manage_apollo_context = float((manage_perf.get("phases") or {}).get("apollo_context_seconds") or 0.0)
        manage_modeled_before = max(manage_current_total - manage_apollo_context + float(prior_manage_apollo.get("total_seconds") or 0.0), 0.0)

        apollo_current_total = float(apollo_perf.get("total_seconds") or 0.0)
        apollo_modeled_serial = sum(float(value or 0.0) for value in (apollo_perf.get("phases") or {}).values())

        report = {
            "validation_database": {
                "source": str(repo_root / "instance" / "horme_trades.db"),
                "clone": str(database_path),
                "normalized_legacy_close_method": "Expiration -> Expire",
            },
            "manage_trades": {
                "route": {
                    "status_code": manage_route["status_code"],
                    "elapsed_ms": manage_route["elapsed_ms"],
                },
                "ui_validation": {
                    "has_manage_trades_heading": "Manage Trades" in str(manage_route["html"]),
                    "has_remaining_premium_column": "Remaining Premium" in str(manage_route["html"]),
                    "has_next_trigger_column": "Next Trigger" in str(manage_route["html"]),
                },
                "performance": manage_perf,
                "modeled_before_seconds": round(manage_modeled_before, 4),
                "modeled_improvement_seconds": round(max(manage_modeled_before - manage_current_total, 0.0), 4),
                "modeled_improvement_percent": round(((manage_modeled_before - manage_current_total) / manage_modeled_before) * 100.0, 2) if manage_modeled_before > 0 else 0.0,
                "open_trade_count": manage_payload.get("open_trade_count"),
            },
            "apollo": {
                "route": {
                    "status_code": apollo_route["status_code"],
                    "elapsed_ms": apollo_route["elapsed_ms"],
                },
                "ui_validation": {
                    "has_apollo_heading": "Apollo: Greek God of Prophecy and Part-Time Options Trader" in str(apollo_route["html"]),
                    "banner_removed": "Approved for next market day" not in str(apollo_route["html"]) and "Stand aside" not in str(apollo_route["html"]),
                },
                "performance": apollo_perf,
                "modeled_before_seconds": round(apollo_modeled_serial, 4),
                "modeled_improvement_seconds": round(max(apollo_modeled_serial - apollo_current_total, 0.0), 4),
                "modeled_improvement_percent": round(((apollo_modeled_serial - apollo_current_total) / apollo_modeled_serial) * 100.0, 2) if apollo_modeled_serial > 0 else 0.0,
                "status": apollo_payload.get("status") or apollo_precheck.get("apollo_status"),
            },
        }

        print(json.dumps(report, indent=2))
        return 0
    finally:
        temp_dir.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())