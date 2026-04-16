"""Developer-only incremental sync from local Delphi 4.3 sources into hosted Delphi 5.4 Supabase."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

from services.kairos_scenario_repository import KairosScenarioRepository
from services.repositories.scenario_repository import FileSystemKairosScenarioRepository, SupabaseKairosScenarioRepository
from services.runtime.supabase_integration import SupabaseTableGateway
from services.trade_history_migration import (
    TARGET_CLOSE_EVENTS_TABLE,
    TARGET_TRADES_TABLE,
    TradeHistorySnapshot,
    load_trade_history_snapshot,
    verify_trade_history_target,
)


TARGET_KAIROS_TAPES_TABLE = "kairos_simulation_tapes"
SYNC_STATUS_OBJECT_KEY = "latest"
SYNC_STATUS_OBJECT_TYPE = "delphi4_sync_status"
LIVE_TAPE_SOURCE_TYPE = "live_spx_tape"


class RuntimeStateStore(Protocol):
    def load_object(self, object_key: str) -> dict[str, Any] | None:
        ...

    def save_object(self, object_key: str, payload: dict[str, Any], *, expires_at: str | None = None) -> None:
        ...


@dataclass(frozen=True)
class Delphi4SourcePaths:
    source_root: Path
    trade_database_path: Path
    kairos_replay_dir: Path


@dataclass(frozen=True)
class IncrementalTradePlan:
    missing_trades: list[dict[str, Any]]
    missing_close_events: list[dict[str, Any]]
    missing_trade_numbers: list[int]


@dataclass(frozen=True)
class IncrementalKairosPlan:
    missing_scenario_keys: list[str]
    missing_session_dates: list[str]
    missing_bundles: list[tuple[str, dict[str, Any]]]


@dataclass(frozen=True)
class Delphi4SyncSummary:
    dry_run: bool
    source_paths: Delphi4SourcePaths
    new_trade_numbers: list[int]
    new_close_event_count: int
    new_tape_session_dates: list[str]
    new_tape_scenario_keys: list[str]
    had_changes: bool
    completed_at: str
    last_successful_sync_at: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_paths"] = {
            "source_root": str(self.source_paths.source_root),
            "trade_database_path": str(self.source_paths.trade_database_path),
            "kairos_replay_dir": str(self.source_paths.kairos_replay_dir),
        }
        return payload


def resolve_delphi4_source_paths(*, repo_root: Path | None = None) -> Delphi4SourcePaths:
    workspace_root = repo_root or Path(__file__).resolve().parents[1]
    explicit_root = _read_path_env("DELPHI43_SOURCE_ROOT")
    source_root = explicit_root or _find_delphi4_source_root(workspace_root)
    if source_root is None:
        raise FileNotFoundError(
            "Delphi 4.3 source root is unavailable. Set DELPHI43_SOURCE_ROOT or ensure a sibling Delphi-4.3-Production/Legacy repo exists."
        )

    trade_database_path = _read_path_env("DELPHI43_TRADE_SOURCE_DB") or (source_root / "instance" / "horme_trades.db")
    kairos_replay_dir = _read_path_env("DELPHI43_KAIROS_SOURCE_DIR") or (source_root / "instance" / "kairos_replays")
    return Delphi4SourcePaths(
        source_root=source_root,
        trade_database_path=trade_database_path,
        kairos_replay_dir=kairos_replay_dir,
    )


def run_incremental_delphi4_sync(
    *,
    gateway: SupabaseTableGateway,
    kairos_repository: SupabaseKairosScenarioRepository,
    status_store: RuntimeStateStore | None = None,
    source_paths: Delphi4SourcePaths,
    dry_run: bool = False,
) -> Delphi4SyncSummary:
    verify_trade_history_target(gateway)
    kairos_repository.gateway.select(kairos_repository.TABLE, limit=1, columns="scenario_key")
    _verify_delphi4_source_paths(source_paths)

    trade_snapshot = load_trade_history_snapshot(source_paths.trade_database_path)
    trade_plan = build_incremental_trade_plan(gateway, trade_snapshot)
    kairos_plan = build_incremental_kairos_plan(kairos_repository, source_paths.kairos_replay_dir)
    completed_at = datetime.now(timezone.utc).isoformat()
    last_successful_sync_at = load_last_successful_sync_timestamp(status_store)

    if not dry_run:
        _insert_missing_trade_rows(gateway, trade_plan)
        _insert_missing_kairos_bundles(kairos_repository, kairos_plan)
        if status_store is not None:
            status_store.save_object(
                SYNC_STATUS_OBJECT_KEY,
                {
                    "last_successful_sync_at": completed_at,
                    "new_trade_numbers": trade_plan.missing_trade_numbers,
                    "new_close_event_count": len(trade_plan.missing_close_events),
                    "new_tape_session_dates": kairos_plan.missing_session_dates,
                    "new_tape_scenario_keys": kairos_plan.missing_scenario_keys,
                },
            )
        last_successful_sync_at = completed_at

    return Delphi4SyncSummary(
        dry_run=dry_run,
        source_paths=source_paths,
        new_trade_numbers=trade_plan.missing_trade_numbers,
        new_close_event_count=len(trade_plan.missing_close_events),
        new_tape_session_dates=kairos_plan.missing_session_dates,
        new_tape_scenario_keys=kairos_plan.missing_scenario_keys,
        had_changes=bool(trade_plan.missing_trade_numbers or trade_plan.missing_close_events or kairos_plan.missing_scenario_keys),
        completed_at=completed_at,
        last_successful_sync_at=last_successful_sync_at,
    )


def load_last_successful_sync_timestamp(status_store: RuntimeStateStore | None) -> str | None:
    if status_store is None:
        return None
    payload = status_store.load_object(SYNC_STATUS_OBJECT_KEY) or {}
    return str(payload.get("last_successful_sync_at") or "").strip() or None


def build_incremental_trade_plan(gateway: SupabaseTableGateway, snapshot: TradeHistorySnapshot) -> IncrementalTradePlan:
    existing_trade_ids = _select_existing_ids(gateway, TARGET_TRADES_TABLE)
    existing_close_event_ids = _select_existing_ids(gateway, TARGET_CLOSE_EVENTS_TABLE)
    missing_trades = [dict(trade) for trade in snapshot.trades if int(trade.get("id") or 0) not in existing_trade_ids]
    missing_close_events = [
        dict(close_event)
        for close_event in snapshot.close_events
        if int(close_event.get("id") or 0) not in existing_close_event_ids
    ]
    missing_trade_numbers = [
        int(trade.get("trade_number"))
        for trade in missing_trades
        if trade.get("trade_number") not in {None, ""}
    ]
    return IncrementalTradePlan(
        missing_trades=missing_trades,
        missing_close_events=missing_close_events,
        missing_trade_numbers=missing_trade_numbers,
    )


def build_incremental_kairos_plan(
    repository: SupabaseKairosScenarioRepository,
    source_dir: str | Path,
) -> IncrementalKairosPlan:
    source_repository = FileSystemKairosScenarioRepository(KairosScenarioRepository(source_dir))
    source_entries = source_repository.list_catalog_entries()
    target_entries = repository.list_catalog_entries()
    existing_keys = {str(item.get("scenario_key") or "").strip() for item in target_entries if item.get("scenario_key")}

    missing_bundles: list[tuple[str, dict[str, Any]]] = []
    missing_scenario_keys: list[str] = []
    missing_session_dates: list[str] = []
    seen_session_dates: set[str] = set()
    for entry in source_entries:
        scenario_key = str(entry.get("scenario_key") or "").strip()
        if not scenario_key or scenario_key in existing_keys:
            continue
        if str(entry.get("source_type") or "").strip() != LIVE_TAPE_SOURCE_TYPE:
            continue
        payload = source_repository.load_bundle_payload(scenario_key)
        if payload is None:
            continue
        missing_bundles.append((scenario_key, payload))
        missing_scenario_keys.append(scenario_key)
        session_date = str(entry.get("session_date") or "").strip()
        if session_date and session_date not in seen_session_dates:
            seen_session_dates.add(session_date)
            missing_session_dates.append(session_date)

    return IncrementalKairosPlan(
        missing_scenario_keys=missing_scenario_keys,
        missing_session_dates=missing_session_dates,
        missing_bundles=missing_bundles,
    )


def _insert_missing_trade_rows(gateway: SupabaseTableGateway, plan: IncrementalTradePlan) -> None:
    for trade in plan.missing_trades:
        gateway.insert(TARGET_TRADES_TABLE, trade)
    for close_event in plan.missing_close_events:
        gateway.insert(TARGET_CLOSE_EVENTS_TABLE, close_event)


def _insert_missing_kairos_bundles(repository: SupabaseKairosScenarioRepository, plan: IncrementalKairosPlan) -> None:
    for scenario_key, payload in plan.missing_bundles:
        repository.save_bundle(scenario_key, payload)


def _select_existing_ids(gateway: SupabaseTableGateway, table_name: str) -> set[int]:
    rows = gateway.select(table_name, columns="id")
    return {int(row["id"]) for row in rows if row.get("id") is not None}


def _find_delphi4_source_root(workspace_root: Path) -> Path | None:
    parent = workspace_root.parent
    for directory_name in ("Delphi-4.3-Production", "Delphi-4.3-Legacy-Repo"):
        candidate = parent / directory_name
        if candidate.exists():
            return candidate
    return None


def _read_path_env(name: str) -> Path | None:
    import os

    value = str(os.getenv(name) or "").strip()
    if not value:
        return None
    return Path(value)


def _verify_delphi4_source_paths(source_paths: Delphi4SourcePaths) -> None:
    if not source_paths.trade_database_path.exists():
        raise FileNotFoundError(f"Delphi 4.3 trade history source is unavailable: {source_paths.trade_database_path}")
    if not source_paths.kairos_replay_dir.exists():
        raise FileNotFoundError(f"Delphi 4.3 Kairos replay source is unavailable: {source_paths.kairos_replay_dir}")
