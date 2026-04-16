"""Trade journal repository abstraction and adapters."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Protocol, runtime_checkable

from services.trade_store import TradeStore
from services.trade_store import (
    build_real_trade_outcome_profile,
    build_trade_duplicate_signature,
    current_timestamp,
    default_close_reason,
    derive_close_event_type,
    normalize_close_method,
    normalize_trade_mode,
    normalize_trade_payload,
    parse_datetime_value,
    summarize_trade_close_events,
    timestamp_seconds_ago,
    to_float,
    to_int,
)
from services.runtime.supabase_integration import SupabaseRequestError, SupabaseRuntimeContext, SupabaseTableGateway

LOGGER = logging.getLogger(__name__)


@runtime_checkable
class TradeRepository(Protocol):
    """Abstract trade journal persistence contract."""

    @property
    def database_path(self) -> str:
        ...

    def initialize(self) -> None:
        ...

    def next_trade_number(self) -> int:
        ...

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        ...

    def create_trade(self, values: Dict[str, Any]) -> int:
        ...

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        ...

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def delete_trade(self, trade_id: int) -> None:
        ...

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        ...

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        ...

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        ...

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        ...


class SQLiteTradeRepository:
    """Adapter that exposes the trade-store API through a repository seam."""

    def __init__(self, store: TradeStore) -> None:
        self._store = store

    @property
    def database_path(self) -> str:
        return str(self._store.database_path)

    def initialize(self) -> None:
        self._store.initialize()

    def next_trade_number(self) -> int:
        return self._store.next_trade_number()

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        return self._store.find_recent_duplicate(values, window_seconds=window_seconds)

    def create_trade(self, values: Dict[str, Any]) -> int:
        return self._store.create_trade(values)

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        return self._store.get_trade(trade_id)

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        self._store.update_trade(trade_id, values)

    def delete_trade(self, trade_id: int) -> None:
        self._store.delete_trade(trade_id)

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        self._store.reduce_trade(trade_id, values)

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        self._store.expire_trade(trade_id, values)

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        return self._store.find_duplicate_trade(values)

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        return self._store.list_trades(trade_mode)

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        return self._store.summarize(trade_mode)

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        return self._store.build_real_trade_outcome_profile()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)
class SupabaseTradeRepository:
    """Supabase-backed trade repository for hosted Delphi runtime composition."""

    TRADES_TABLE = "journal_trades"
    CLOSE_EVENTS_TABLE = "journal_trade_close_events"
    TRADE_IDENTITY_REPAIR_RPC = "sync_journal_trade_identity_sequence"
    CLOSE_EVENT_IDENTITY_REPAIR_RPC = "sync_journal_trade_close_event_identity_sequence"

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext,
        gateway: SupabaseTableGateway,
        database_path: str | Path,
        market_data_service: Any | None = None,
    ) -> None:
        self.context = context
        self.gateway = gateway
        self._database_path = str(database_path)
        self.market_data_service = market_data_service
        self._trade_storage_available: bool | None = None

    @property
    def database_path(self) -> str:
        return self._database_path

    def initialize(self) -> None:
        return None

    def is_trade_storage_available(self) -> bool:
        if self._trade_storage_available is not None:
            return self._trade_storage_available
        try:
            self.gateway.select(self.TRADES_TABLE, limit=1, columns="id")
            self.gateway.select(self.CLOSE_EVENTS_TABLE, limit=1, columns="id")
        except SupabaseRequestError as exc:
            if self._is_missing_table_error(exc, self.TRADES_TABLE) or self._is_missing_table_error(exc, self.CLOSE_EVENTS_TABLE):
                self._trade_storage_available = False
                return False
            raise
        self._trade_storage_available = True
        return True

    def next_trade_number(self) -> int:
        rows = self._select_rows(self.TRADES_TABLE, order="trade_number.desc,id.desc", limit=1)
        trade_number = to_int(rows[0].get("trade_number")) if rows else None
        return (trade_number or 0) + 1

    def find_recent_duplicate(self, values: Dict[str, Any], window_seconds: int = 15) -> Dict[str, Any] | None:
        normalized = normalize_trade_payload(values)
        threshold = parse_datetime_value(timestamp_seconds_ago(window_seconds))
        rows = self._select_rows(
            self.TRADES_TABLE,
            filters={"trade_mode": f"eq.{normalized.get('trade_mode')}"},
            order="id.desc",
            limit=25,
        )
        for row in rows:
            created_at = parse_datetime_value(row.get("created_at"))
            if threshold is not None and (created_at is None or created_at < threshold):
                continue
            if not self._matches_recent_duplicate_candidate(normalized, row):
                continue
            return dict(row)
        return None

    def create_trade(self, values: Dict[str, Any]) -> int:
        normalized = normalize_trade_payload(values)
        timestamp = current_timestamp()
        normalized["created_at"] = timestamp
        normalized["updated_at"] = timestamp
        trade_number = normalized.get("trade_number")
        if trade_number is None:
            normalized["trade_number"] = self.next_trade_number()
        else:
            self._ensure_trade_number_available(int(trade_number))
        inserted = self._insert_trade_row(normalized)
        if not inserted:
            raise ValueError("Supabase trade insert did not return a row.")
        return int(inserted[0]["id"])

    def _insert_trade_row(self, normalized: Dict[str, Any]) -> list[dict[str, Any]]:
        serialized = self._serialize_payload(normalized)
        return self._insert_with_identity_recovery(
            table=self.TRADES_TABLE,
            payload=serialized,
            repair_rpc=self.TRADE_IDENTITY_REPAIR_RPC,
            resolve_next_id=self._resolve_next_trade_id,
        )

    def _insert_with_identity_recovery(
        self,
        *,
        table: str,
        payload: Dict[str, Any],
        repair_rpc: str,
        resolve_next_id,
    ) -> list[dict[str, Any]]:
        try:
            return self.gateway.insert(table, payload)
        except SupabaseRequestError as exc:
            if not self._is_duplicate_primary_key_error(exc, table=table):
                raise
            LOGGER.warning("Supabase %s identity sequence appears out of sync. Attempting repair RPC.", table)
            try:
                self.gateway.rpc(repair_rpc, {})
                return self.gateway.insert(table, payload)
            except SupabaseRequestError as repair_exc:
                LOGGER.warning(
                    "Supabase %s repair RPC was unavailable. Falling back to an explicit id insert. Details: %s",
                    table,
                    repair_exc,
                )
                fallback_payload = dict(payload)
                fallback_payload["id"] = resolve_next_id()
                return self.gateway.insert(table, fallback_payload)

    def _resolve_next_trade_id(self) -> int:
        rows = self._select_rows(self.TRADES_TABLE, order="id.desc", limit=1, columns="id")
        current_max = to_int(rows[0].get("id")) if rows else None
        return (current_max or 0) + 1

    def _resolve_next_close_event_id(self) -> int:
        rows = self._select_rows(self.CLOSE_EVENTS_TABLE, order="id.desc", limit=1, columns="id")
        current_max = to_int(rows[0].get("id")) if rows else None
        return (current_max or 0) + 1

    def get_trade(self, trade_id: int) -> Dict[str, Any] | None:
        rows = self._select_rows(self.TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"}, limit=1)
        return self._attach_trade_state(dict(rows[0])) if rows else None

    def update_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        existing = self.get_trade(trade_id)
        if not existing:
            raise ValueError("Trade not found.")
        close_events_payload = values.get("close_events") if "close_events" in values else None
        normalized = normalize_trade_payload(values, existing=existing)
        normalized["updated_at"] = current_timestamp()
        if close_events_payload is not None:
            normalized.update(
                {
                    "status": "cancelled" if normalized.get("status") == "cancelled" else "open",
                    "exit_datetime": None,
                    "spx_at_exit": None,
                    "actual_exit_value": None,
                    "close_method": None,
                }
            )
        trade_number = normalized.get("trade_number") or existing.get("trade_number")
        if trade_number is None:
            trade_number = self.next_trade_number()
        self._ensure_trade_number_available(int(trade_number), exclude_id=int(trade_id))
        normalized["trade_number"] = int(trade_number)
        self.gateway.update(
            self.TRADES_TABLE,
            self._serialize_payload(normalized),
            filters={"id": f"eq.{int(trade_id)}"},
        )
        if close_events_payload is not None:
            synchronized_events = self._normalize_close_events_payload(close_events_payload, trade_contracts=normalized.get("contracts"))
            self._replace_close_events(int(trade_id), synchronized_events)
            self._refresh_trade_after_close_events(int(trade_id))

    def delete_trade(self, trade_id: int) -> None:
        self.gateway.delete(self.TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"})

    def reduce_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        trade = self.get_trade(trade_id)
        if not trade:
            raise ValueError("Trade not found.")
        if trade.get("derived_status_raw") in {"closed", "expired", "cancelled"}:
            raise ValueError("Only open trades can be reduced.")
        remaining_contracts = int(trade.get("remaining_contracts") or 0)
        contracts_closed = to_int(values.get("contracts_closed"))
        actual_exit_value = to_float(values.get("actual_exit_value"))
        if contracts_closed is None or contracts_closed <= 0:
            raise ValueError("Reduce quantity must be at least 1 contract.")
        if remaining_contracts <= 0 or contracts_closed > remaining_contracts:
            raise ValueError("Reduce quantity cannot exceed the remaining open contracts.")
        if actual_exit_value is None or actual_exit_value < 0:
            raise ValueError("Reduce exit value must be zero or greater.")
        self._insert_close_event(
            trade_id=int(trade_id),
            event_type="reduce",
            contracts_closed=contracts_closed,
            actual_exit_value=actual_exit_value,
            event_datetime=values.get("event_datetime") or current_timestamp(),
            spx_at_exit=values.get("spx_at_exit"),
            close_method=values.get("close_method") or "Reduce",
            close_reason=values.get("close_reason") or f"Reduced {contracts_closed} contract{'s' if contracts_closed != 1 else ''}",
            notes_exit=values.get("notes_exit") or "",
        )
        self._refresh_trade_after_close_events(int(trade_id))

    def expire_trade(self, trade_id: int, values: Dict[str, Any]) -> None:
        payload = values or {}
        trade = self.get_trade(trade_id)
        if not trade:
            raise ValueError("Trade not found.")
        if trade.get("derived_status_raw") in {"closed", "expired", "cancelled"}:
            raise ValueError("Only open trades can be expired.")
        remaining_contracts = int(trade.get("remaining_contracts") or 0)
        if remaining_contracts <= 0:
            raise ValueError("Trade has no remaining open contracts to expire.")
        actual_exit_value = payload.get("actual_exit_value")
        if actual_exit_value in {None, ""}:
            actual_exit_value = 0.0
        actual_exit_value = to_float(actual_exit_value)
        if actual_exit_value is None or actual_exit_value < 0:
            raise ValueError("Expire exit value must be zero or greater.")
        self._insert_close_event(
            trade_id=int(trade_id),
            event_type="expire",
            contracts_closed=remaining_contracts,
            actual_exit_value=actual_exit_value,
            event_datetime=payload.get("event_datetime") or current_timestamp(),
            spx_at_exit=payload.get("spx_at_exit"),
            close_method=payload.get("close_method") or "Expire",
            close_reason=payload.get("close_reason") or "Expired Worthless",
            notes_exit=payload.get("notes_exit") or "",
        )
        self._refresh_trade_after_close_events(int(trade_id))

    def find_duplicate_trade(self, values: Dict[str, Any]) -> Dict[str, Any] | None:
        normalized = normalize_trade_payload(values)
        target_signature = build_trade_duplicate_signature(normalized, already_normalized=True)
        for row in self._select_rows(self.TRADES_TABLE, filters={"trade_mode": f"eq.{normalized['trade_mode']}"}, order="id.desc"):
            candidate = dict(row)
            if build_trade_duplicate_signature(candidate, already_normalized=True) == target_signature:
                return candidate
        return None

    def list_trades(self, trade_mode: str) -> List[Dict[str, Any]]:
        rows = self._select_rows(self.TRADES_TABLE, filters={"trade_mode": f"eq.{normalize_trade_mode(trade_mode)}"})
        close_events_by_trade_id = self._select_close_events_for_trade_ids(
            int(row.get("id") or 0)
            for row in rows
            if row.get("id") is not None
        )
        trades = [
            self._attach_trade_state(
                dict(row),
                close_events=close_events_by_trade_id.get(int(row.get("id") or 0), []),
            )
            for row in rows
        ]
        trades.sort(key=self._sort_key, reverse=True)
        return trades

    def summarize(self, trade_mode: str) -> Dict[str, Any]:
        rows = self.list_trades(trade_mode)
        total_pnl = sum(float(row.get("gross_pnl") or 0.0) for row in rows)
        pnl_values = [float(row.get("gross_pnl") or 0.0) for row in rows if row.get("gross_pnl") is not None]
        return {
            "total_trades": len(rows),
            "open_trades": sum(1 for row in rows if row.get("derived_status_raw") in {"open", "reduced"}),
            "closed_trades": sum(1 for row in rows if row.get("derived_status_raw") not in {"open", "reduced"}),
            "total_pnl": total_pnl,
            "average_pnl": (sum(pnl_values) / len(pnl_values)) if pnl_values else 0.0,
            "win_count": sum(1 for row in rows if row.get("win_loss_result") == "Win"),
            "loss_count": sum(1 for row in rows if row.get("win_loss_result") in {"Loss", "Black Swan"}),
        }

    def build_real_trade_outcome_profile(self) -> Dict[str, Any]:
        return build_real_trade_outcome_profile(self.list_trades("real"))

    def _attach_trade_state(self, trade: Dict[str, Any], *, close_events: list[Dict[str, Any]] | None = None) -> Dict[str, Any]:
        events = close_events if close_events is not None else (self._list_close_events(int(trade.get("id"))) if trade.get("id") is not None else [])
        summary = summarize_trade_close_events(trade, events)
        trade.update(summary)
        trade["close_events"] = events
        return trade

    def _list_close_events(self, trade_id: int) -> list[Dict[str, Any]]:
        rows = self._select_rows(
            self.CLOSE_EVENTS_TABLE,
            filters={"trade_id": f"eq.{int(trade_id)}"},
            order="event_datetime.asc.nullslast,id.asc",
        )
        return [dict(row) for row in rows]

    def _select_close_events_for_trade_ids(self, trade_ids: Any) -> dict[int, list[Dict[str, Any]]]:
        normalized_trade_ids = sorted({int(trade_id) for trade_id in trade_ids if int(trade_id) > 0})
        if not normalized_trade_ids:
            return {}
        rows = self._select_rows(
            self.CLOSE_EVENTS_TABLE,
            filters={"trade_id": f"in.({','.join(str(trade_id) for trade_id in normalized_trade_ids)})"},
            order="trade_id.asc,event_datetime.asc.nullslast,id.asc",
        )
        events_by_trade_id: dict[int, list[Dict[str, Any]]] = {trade_id: [] for trade_id in normalized_trade_ids}
        for row in rows:
            trade_id = int(row.get("trade_id") or 0)
            events_by_trade_id.setdefault(trade_id, []).append(dict(row))
        return events_by_trade_id

    def _select_rows(
        self,
        table: str,
        *,
        filters: dict[str, str] | None = None,
        order: str | None = None,
        limit: int | None = None,
        columns: str = "*",
    ) -> list[dict[str, Any]]:
        try:
            return self.gateway.select(table, filters=filters, order=order, limit=limit, columns=columns)
        except SupabaseRequestError as exc:
            if self._is_missing_table_error(exc, table):
                if table in {self.TRADES_TABLE, self.CLOSE_EVENTS_TABLE}:
                    self._trade_storage_available = False
                LOGGER.warning(
                    "Supabase table %s is unavailable; returning empty hosted trade data until migrations are applied.",
                    table,
                )
                return []
            raise

    @staticmethod
    def _is_missing_table_error(error: SupabaseRequestError, table: str) -> bool:
        detail = str(error)
        return "PGRST205" in detail and (f"'{table}'" in detail or f"'public.{table}'" in detail)

    def _insert_close_event(
        self,
        *,
        trade_id: int,
        event_type: str,
        contracts_closed: int,
        actual_exit_value: float,
        event_datetime: Any,
        spx_at_exit: Any,
        close_method: Any,
        close_reason: Any,
        notes_exit: Any,
    ) -> None:
        parsed_event_datetime = parse_datetime_value(event_datetime)
        self._insert_with_identity_recovery(
            table=self.CLOSE_EVENTS_TABLE,
            payload={
                "trade_id": int(trade_id),
                "created_at": current_timestamp(),
                "event_type": str(event_type or "").strip().lower(),
                "event_datetime": parsed_event_datetime.isoformat(timespec="minutes") if parsed_event_datetime else None,
                "contracts_closed": int(contracts_closed),
                "actual_exit_value": float(actual_exit_value),
                "spx_at_exit": to_float(spx_at_exit),
                "close_method": str(close_method or "").strip() or None,
                "close_reason": str(close_reason or "").strip() or None,
                "notes_exit": str(notes_exit or "").strip() or None,
            },
            repair_rpc=self.CLOSE_EVENT_IDENTITY_REPAIR_RPC,
            resolve_next_id=self._resolve_next_close_event_id,
        )

    def _normalize_close_events_payload(self, close_events: Any, *, trade_contracts: Any) -> list[Dict[str, Any]]:
        original_contracts = to_int(trade_contracts)
        if original_contracts is None or original_contracts <= 0:
            if close_events:
                raise ValueError("Contracts must be set before saving close events.")
            return []
        normalized_rows: list[Dict[str, Any]] = []
        total_closed = 0
        for index, row in enumerate(close_events or [], start=1):
            event_id = to_int((row or {}).get("id"))
            contracts_raw = (row or {}).get("contracts_closed")
            exit_value_raw = (row or {}).get("actual_exit_value")
            method_raw = (row or {}).get("close_method")
            if event_id is None and all(value in {None, ""} for value in (contracts_raw, exit_value_raw, method_raw)):
                continue
            contracts_closed = to_int(contracts_raw)
            if contracts_closed is None or contracts_closed <= 0:
                raise ValueError(f"Close event #{index}: Contracts closed must be a positive integer.")
            close_method = normalize_close_method(method_raw)
            actual_exit_value = 0.0 if close_method == "Expire" and exit_value_raw in {None, ""} else to_float(exit_value_raw)
            if actual_exit_value is None:
                raise ValueError(f"Close event #{index}: Close credit is required.")
            if actual_exit_value < 0:
                raise ValueError(f"Close event #{index}: Close credit must be zero or greater.")
            total_closed += contracts_closed
            normalized_rows.append(
                {
                    "id": event_id,
                    "contracts_closed": contracts_closed,
                    "actual_exit_value": actual_exit_value,
                    "event_datetime": (row or {}).get("event_datetime"),
                    "close_method": close_method,
                    "notes_exit": str((row or {}).get("notes_exit") or "").strip(),
                    "event_type": derive_close_event_type(close_method),
                }
            )
        if total_closed > original_contracts:
            raise ValueError("The total closed quantity cannot exceed the original contracts.")
        return normalized_rows

    def _replace_close_events(self, trade_id: int, close_events: list[Dict[str, Any]]) -> None:
        existing_events = {int(row["id"]): dict(row) for row in self._list_close_events(trade_id)}
        retained_ids: set[int] = set()
        for row in close_events:
            event_id = row.get("id")
            if event_id is not None:
                existing_row = existing_events.get(int(event_id))
                if not existing_row:
                    raise ValueError("One or more close events could not be matched to this trade.")
                retained_ids.add(int(event_id))
                parsed_event_datetime = parse_datetime_value(row.get("event_datetime"))
                self.gateway.update(
                    self.CLOSE_EVENTS_TABLE,
                    {
                        "event_type": row["event_type"],
                        "event_datetime": parsed_event_datetime.isoformat(timespec="minutes") if parsed_event_datetime else existing_row.get("event_datetime"),
                        "contracts_closed": row["contracts_closed"],
                        "actual_exit_value": row["actual_exit_value"],
                        "close_method": row["close_method"],
                        "close_reason": default_close_reason(row["close_method"], row["contracts_closed"]),
                        "notes_exit": row.get("notes_exit") or existing_row.get("notes_exit") or "",
                    },
                    filters={"id": f"eq.{int(event_id)}", "trade_id": f"eq.{int(trade_id)}"},
                )
                continue
            self._insert_close_event(
                trade_id=trade_id,
                event_type=row["event_type"],
                contracts_closed=row["contracts_closed"],
                actual_exit_value=row["actual_exit_value"],
                event_datetime=row.get("event_datetime") or current_timestamp(),
                spx_at_exit=None,
                close_method=row["close_method"],
                close_reason=default_close_reason(row["close_method"], row["contracts_closed"]),
                notes_exit=row.get("notes_exit") or "",
            )
        for event_id in existing_events:
            if event_id not in retained_ids:
                self.gateway.delete(self.CLOSE_EVENTS_TABLE, filters={"id": f"eq.{int(event_id)}", "trade_id": f"eq.{int(trade_id)}"})

    def _refresh_trade_after_close_events(self, trade_id: int) -> None:
        trade = self._get_trade_row(trade_id)
        if not trade:
            return
        events = self._list_close_events(trade_id)
        summary = summarize_trade_close_events(trade, events)
        self.gateway.update(
            self.TRADES_TABLE,
            {
                "updated_at": current_timestamp(),
                "status": summary.get("status"),
                "exit_datetime": summary.get("exit_datetime"),
                "spx_at_exit": summary.get("spx_at_exit"),
                "actual_exit_value": summary.get("actual_exit_value"),
                "close_method": summary.get("close_method"),
                "close_reason": summary.get("close_reason"),
                "notes_exit": summary.get("notes_exit"),
                "gross_pnl": summary.get("gross_pnl"),
                "max_risk": summary.get("max_risk"),
                "roi_on_risk": summary.get("roi_on_risk"),
                "hours_held": summary.get("hours_held"),
                "win_loss_result": summary.get("win_loss_result"),
            },
            filters={"id": f"eq.{int(trade_id)}"},
        )

    def _get_trade_row(self, trade_id: int) -> Dict[str, Any] | None:
        rows = self.gateway.select(self.TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"}, limit=1)
        return dict(rows[0]) if rows else None

    def _ensure_trade_number_available(self, trade_number: int, exclude_id: int | None = None) -> None:
        rows = self.gateway.select(self.TRADES_TABLE, filters={"trade_number": f"eq.{int(trade_number)}"})
        for row in rows:
            if exclude_id is not None and int(row.get("id") or 0) == int(exclude_id):
                continue
            raise ValueError("Trade number already exists.")

    @staticmethod
    def _serialize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {key: value for key, value in payload.items() if key not in {"id", "trade_id"}}

    @staticmethod
    def _is_duplicate_primary_key_error(error: SupabaseRequestError, *, table: str) -> bool:
        detail = str(error or "")
        return "23505" in detail and f"{table}_pkey" in detail

    @staticmethod
    def _filter_value(value: Any) -> str:
        return "" if value in {None, ""} else str(value)

    @staticmethod
    def _numeric_filter_value(value: Any) -> str:
        integer_value = to_int(value)
        if integer_value is not None and str(value).strip() not in {"", "None"}:
            if to_float(value) == float(integer_value):
                return str(integer_value)
        normalized = to_float(value)
        return "-1" if normalized is None else str(normalized)

    @staticmethod
    def _sort_key(trade: Dict[str, Any]) -> tuple[datetime, int]:
        sort_value = parse_datetime_value(trade.get("entry_datetime"))
        if sort_value is None:
            sort_value = parse_datetime_value(trade.get("trade_date"))
        if sort_value is None:
            sort_value = parse_datetime_value(trade.get("created_at"))
        if sort_value is None:
            sort_value = datetime.min.replace(tzinfo=timezone.utc)
        elif sort_value.tzinfo is None:
            sort_value = sort_value.replace(tzinfo=timezone.utc)
        return (sort_value, int(trade.get("id") or 0))

    @staticmethod
    def _matches_recent_duplicate_candidate(normalized: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
        return (
            str(candidate.get("trade_mode") or "") == str(normalized.get("trade_mode") or "")
            and str(candidate.get("system_name") or "") == str(normalized.get("system_name") or "")
            and str(candidate.get("journal_name") or "") == str(normalized.get("journal_name") or "")
            and str(candidate.get("candidate_profile") or "") == str(normalized.get("candidate_profile") or "")
            and str(candidate.get("underlying_symbol") or "") == str(normalized.get("underlying_symbol") or "")
            and to_float(candidate.get("short_strike")) == to_float(normalized.get("short_strike"))
            and to_float(candidate.get("long_strike")) == to_float(normalized.get("long_strike"))
            and to_int(candidate.get("contracts")) == to_int(normalized.get("contracts"))
            and str(candidate.get("status") or "") == str(normalized.get("status") or "")
        )