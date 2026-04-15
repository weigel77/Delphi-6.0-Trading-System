"""Trade-history migration helpers for moving Delphi 4.3 SQLite journal data into hosted storage."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from services.runtime.supabase_integration import SupabaseRequestError, SupabaseTableGateway
from services.trade_store import resolve_trade_candidate_profile


SOURCE_TRADES_TABLE = "trades"
SOURCE_CLOSE_EVENTS_TABLE = "trade_close_events"
TARGET_TRADES_TABLE = "journal_trades"
TARGET_CLOSE_EVENTS_TABLE = "journal_trade_close_events"


@dataclass(frozen=True)
class TradeHistorySnapshot:
    source_path: Path
    trades: list[dict[str, Any]]
    close_events: list[dict[str, Any]]


@dataclass(frozen=True)
class TradeHistorySyncResult:
    source_trade_count: int
    source_close_event_count: int
    inserted_trade_count: int
    updated_trade_count: int
    inserted_close_event_count: int
    updated_close_event_count: int


def load_trade_history_snapshot(source_path: str | Path) -> TradeHistorySnapshot:
    database_path = Path(source_path)
    if not database_path.exists():
        raise FileNotFoundError(f"SQLite trade history source does not exist: {database_path}")

    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        trade_cursor = connection.execute(f"SELECT * FROM {SOURCE_TRADES_TABLE} ORDER BY id ASC")
        close_event_cursor = connection.execute(f"SELECT * FROM {SOURCE_CLOSE_EVENTS_TABLE} ORDER BY id ASC")
        trades = [_normalize_trade_row(dict(row)) for row in trade_cursor.fetchall()]
        close_events = [dict(row) for row in close_event_cursor.fetchall()]
        trade_cursor.close()
        close_event_cursor.close()

    return TradeHistorySnapshot(source_path=database_path, trades=trades, close_events=close_events)


def filter_trade_history_snapshot(
    snapshot: TradeHistorySnapshot,
    *,
    trade_ids: Iterable[int] | None = None,
    trade_numbers: Iterable[int] | None = None,
) -> TradeHistorySnapshot:
    selected_trade_ids = {int(value) for value in (trade_ids or [])}
    selected_trade_numbers = {int(value) for value in (trade_numbers or [])}
    if not selected_trade_ids and not selected_trade_numbers:
        return snapshot

    filtered_trades = [
        dict(trade)
        for trade in snapshot.trades
        if (selected_trade_ids and int(trade.get("id") or 0) in selected_trade_ids)
        or (selected_trade_numbers and int(trade.get("trade_number") or 0) in selected_trade_numbers)
    ]
    retained_ids = {int(trade.get("id") or 0) for trade in filtered_trades}
    filtered_close_events = [
        dict(close_event)
        for close_event in snapshot.close_events
        if int(close_event.get("trade_id") or 0) in retained_ids
    ]
    return TradeHistorySnapshot(
        source_path=snapshot.source_path,
        trades=filtered_trades,
        close_events=filtered_close_events,
    )


def sync_trade_history_snapshot(
    gateway: SupabaseTableGateway,
    snapshot: TradeHistorySnapshot,
) -> TradeHistorySyncResult:
    verify_trade_history_target(gateway)
    existing_trade_ids = _select_existing_ids(gateway, TARGET_TRADES_TABLE)
    existing_close_event_ids = _select_existing_ids(gateway, TARGET_CLOSE_EVENTS_TABLE)

    inserted_trade_count = 0
    updated_trade_count = 0
    inserted_close_event_count = 0
    updated_close_event_count = 0

    for trade in snapshot.trades:
        trade_id = int(trade["id"])
        if trade_id in existing_trade_ids:
            gateway.update(TARGET_TRADES_TABLE, trade, filters={"id": f"eq.{trade_id}"})
            updated_trade_count += 1
        else:
            gateway.insert(TARGET_TRADES_TABLE, trade)
            inserted_trade_count += 1

    for close_event in snapshot.close_events:
        close_event_id = int(close_event["id"])
        if close_event_id in existing_close_event_ids:
            gateway.update(TARGET_CLOSE_EVENTS_TABLE, close_event, filters={"id": f"eq.{close_event_id}"})
            updated_close_event_count += 1
        else:
            gateway.insert(TARGET_CLOSE_EVENTS_TABLE, close_event)
            inserted_close_event_count += 1

    return TradeHistorySyncResult(
        source_trade_count=len(snapshot.trades),
        source_close_event_count=len(snapshot.close_events),
        inserted_trade_count=inserted_trade_count,
        updated_trade_count=updated_trade_count,
        inserted_close_event_count=inserted_close_event_count,
        updated_close_event_count=updated_close_event_count,
    )


def _normalize_trade_row(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["candidate_profile"] = resolve_trade_candidate_profile(payload)
    return payload


def _select_existing_ids(gateway: SupabaseTableGateway, table_name: str) -> set[int]:
    rows = gateway.select(table_name, columns="id")
    return {int(row["id"]) for row in rows if row.get("id") is not None}


def verify_trade_history_target(gateway: SupabaseTableGateway) -> None:
    try:
        gateway.select(TARGET_TRADES_TABLE, limit=1, columns="id")
        gateway.select(TARGET_CLOSE_EVENTS_TABLE, limit=1, columns="id")
    except SupabaseRequestError as exc:
        detail = str(exc)
        if "PGRST205" in detail:
            raise RuntimeError(
                "Hosted Supabase trade tables are not available. Apply supabase/migrations/20260412_phase3_trade_persistence.sql before importing trade history."
            ) from exc
        raise