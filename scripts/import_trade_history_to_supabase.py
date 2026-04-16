"""Import Delphi 4.3 SQLite trade history into the hosted Supabase trade tables."""

from __future__ import annotations

import argparse
from pathlib import Path

from services.runtime.supabase_integration import SupabaseConfig, SupabaseProjectIntegration
from services.trade_history_migration import (
    filter_trade_history_snapshot,
    load_trade_history_snapshot,
    sync_trade_history_snapshot,
    verify_trade_history_target,
)


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env", help="Environment file containing Supabase URL and keys.")
    parser.add_argument("--source-db", default="instance/horme_trades.db", help="SQLite database containing Delphi 4.3 trade history.")
    parser.add_argument("--trade-id", type=int, action="append", default=[], help="Import only the specified source trade id. Repeat for multiple ids.")
    parser.add_argument("--trade-number", type=int, action="append", default=[], help="Import only the specified source trade number. Repeat for multiple trade numbers.")
    parser.add_argument("--dry-run", action="store_true", help="Load the source history and verify the hosted schema without writing rows.")
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    env_path = Path(args.env_file)
    env_values = read_env(env_path)
    snapshot = load_trade_history_snapshot(args.source_db)
    snapshot = filter_trade_history_snapshot(
        snapshot,
        trade_ids=args.trade_id,
        trade_numbers=args.trade_number,
    )
    integration = SupabaseProjectIntegration(
        SupabaseConfig(
            url=env_values.get("SUPABASE_URL", ""),
            publishable_key=env_values.get("SUPABASE_PUBLISHABLE_KEY", ""),
            secret_key=env_values.get("SUPABASE_SECRET_KEY", ""),
        )
    )
    gateway = integration.create_table_gateway()

    if args.dry_run:
        verify_trade_history_target(gateway)
        print(
            f"Dry run verified hosted schema. Source trades={len(snapshot.trades)}, close_events={len(snapshot.close_events)}."
        )
        return 0

    result = sync_trade_history_snapshot(gateway, snapshot)
    print(f"Imported trade history from {snapshot.source_path}.")
    print(
        "Trades: "
        f"source={result.source_trade_count}, inserted={result.inserted_trade_count}, updated={result.updated_trade_count}"
    )
    print(
        "Close events: "
        f"source={result.source_close_event_count}, inserted={result.inserted_close_event_count}, updated={result.updated_close_event_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())