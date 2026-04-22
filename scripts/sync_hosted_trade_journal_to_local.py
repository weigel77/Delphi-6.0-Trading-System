"""Reconcile hosted Supabase and local Delphi 7.0 journal trades."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.local_trade_sync_service import SYNCABLE_TRADE_MODES, reconcile_trade_journal
from services.repositories.trade_repository import SQLiteTradeRepository, SupabaseTradeRepository
from services.runtime.supabase_integration import SupabaseConfig, SupabaseProjectIntegration
from services.trade_store import TradeStore


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
    parser.add_argument("--local-db", default="instance/horme_trades.db", help="SQLite database to update for local Delphi 7.0 journal views.")
    parser.add_argument(
        "--mode",
        action="append",
        choices=list(SYNCABLE_TRADE_MODES),
        default=[],
        help="Trade mode to sync. Repeat for multiple modes. Defaults to real and simulated.",
    )
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Skipped hosted journal sync: env file not found at {env_path}.")
        return 0

    env_values = read_env(env_path)
    supabase_config = SupabaseConfig(
        url=env_values.get("SUPABASE_URL", ""),
        publishable_key=env_values.get("SUPABASE_PUBLISHABLE_KEY", ""),
        secret_key=env_values.get("SUPABASE_SECRET_KEY", ""),
    )
    if not supabase_config.is_configured:
        print("Skipped hosted journal sync: Supabase environment is not configured in the env file.")
        return 0

    integration = SupabaseProjectIntegration(supabase_config)
    hosted_repository = SupabaseTradeRepository(
        context=integration.initialize_context(),
        gateway=integration.create_table_gateway(),
        database_path="supabase://journal_trades",
    )
    local_backend = TradeStore(Path(args.local_db))
    local_repository = SQLiteTradeRepository(local_backend)
    local_repository.initialize()

    trade_modes = tuple(args.mode or SYNCABLE_TRADE_MODES)
    result = reconcile_trade_journal(
        hosted_repository=hosted_repository,
        local_repository=local_repository,
        trade_modes=trade_modes,
    )
    for mode_result in result.hosted_to_local.mode_results:
        print(
            f"direction=hosted_to_local mode={mode_result.mode} hosted={mode_result.hosted_count} local_before={mode_result.local_before_count} "
            f"inserted={mode_result.inserted_count} duplicate_skips={mode_result.duplicate_skip_count} "
            f"local_after={mode_result.local_after_count} imported_trade_numbers={list(mode_result.imported_trade_numbers)} "
            f"renumbered_trade_numbers={list(mode_result.renumbered_trade_numbers)}"
        )
    for mode_result in result.local_to_hosted.mode_results:
        print(
            f"direction=local_to_hosted mode={mode_result.mode} local={mode_result.local_count} hosted_before={mode_result.hosted_before_count} "
            f"inserted={mode_result.inserted_count} duplicate_skips={mode_result.duplicate_skip_count} "
            f"hosted_after={mode_result.hosted_after_count} exported_trade_numbers={list(mode_result.exported_trade_numbers)} "
            f"renumbered_trade_numbers={list(mode_result.renumbered_trade_numbers)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())