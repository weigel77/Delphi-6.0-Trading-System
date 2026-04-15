"""Open-trade management persistence abstractions and adapters."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Protocol, runtime_checkable

from services.runtime.supabase_integration import SupabaseRequestError, SupabaseTableGateway
from services.trade_store import current_timestamp


MANAGEMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS open_trade_management_state (
    trade_id INTEGER PRIMARY KEY,
    system_name TEXT NOT NULL,
    trade_mode TEXT NOT NULL,
    current_management_status TEXT,
    previous_management_status TEXT,
    current_thesis_status TEXT,
    previous_thesis_status TEXT,
    concise_reason TEXT,
    reason_code TEXT,
    next_trigger TEXT,
    trigger_source TEXT,
    last_evaluated_at TEXT,
    last_alert_sent_at TEXT,
    last_alert_type TEXT,
    last_alert_priority INTEGER,
    alert_count INTEGER NOT NULL DEFAULT 0,
    last_alert_reason_code TEXT,
    last_underlying_price REAL,
    last_distance_to_short REAL,
    last_distance_to_long REAL,
    last_buffer_remaining_percent REAL
);

CREATE TABLE IF NOT EXISTS open_trade_management_alert_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    system_name TEXT NOT NULL,
    trade_mode TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    alert_priority INTEGER NOT NULL,
    alert_priority_label TEXT NOT NULL,
    reason_code TEXT,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    sent_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS open_trade_management_runtime_settings (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    notifications_enabled INTEGER NOT NULL DEFAULT 1,
    last_morning_snapshot_date TEXT,
    last_eod_summary_date TEXT,
    last_background_run_at TEXT
);
"""

ACTIVE_TRADES_TABLE = "active_trades"
ACTIVE_TRADE_ALERT_LOG_TABLE = "active_trade_alert_log"
MANAGEMENT_RUNTIME_SETTINGS_TABLE = "management_runtime_settings"
JOURNAL_TRADES_TABLE = "journal_trades"


def build_management_state_payload(
    record: Dict[str, Any],
    previous_state: Dict[str, Any],
    alert_outcome: Dict[str, Any],
    evaluated_at: datetime,
) -> Dict[str, Any]:
    existing_alert_count = int(previous_state.get("alert_count") or 0)
    return {
        "trade_id": int(record["trade_id"]),
        "system_name": record["system_name"],
        "trade_mode": str(record["trade_mode"]).lower(),
        "current_management_status": record["status"],
        "previous_management_status": previous_state.get("current_management_status"),
        "current_thesis_status": record["thesis_status"],
        "previous_thesis_status": previous_state.get("current_thesis_status"),
        "concise_reason": record["reason"],
        "reason_code": record["reason_code"],
        "next_trigger": record["next_trigger"],
        "trigger_source": record["trigger_source"],
        "last_evaluated_at": evaluated_at.isoformat(),
        "last_alert_sent_at": alert_outcome.get("sent_at") or previous_state.get("last_alert_sent_at"),
        "last_alert_type": alert_outcome.get("alert_type") or previous_state.get("last_alert_type"),
        "last_alert_priority": (
            alert_outcome.get("priority")
            if alert_outcome.get("priority") is not None
            else previous_state.get("last_alert_priority")
        ),
        "alert_count": existing_alert_count + int(alert_outcome.get("sent_count") or (1 if alert_outcome.get("sent") else 0)),
        "last_alert_reason_code": record["reason_code"] if alert_outcome.get("sent") else previous_state.get("last_alert_reason_code"),
        "last_underlying_price": record.get("current_underlying_price"),
        "last_distance_to_short": record.get("distance_to_short"),
        "last_distance_to_long": record.get("distance_to_long"),
        "last_buffer_remaining_percent": record.get("percent_buffer_remaining"),
    }


@runtime_checkable
class OpenTradeManagementStateRepository(Protocol):
    """Abstract persistence contract for open-trade management state."""

    def initialize(self) -> None:
        ...

    def set_notifications_enabled(self, enabled: bool) -> None:
        ...

    def load_runtime_settings(self) -> Dict[str, Any]:
        ...

    def load_trade(self, trade_id: int) -> Dict[str, Any]:
        ...

    def load_management_states(self) -> Dict[int, Dict[str, Any]]:
        ...

    def load_management_state(self, trade_id: int) -> Dict[str, Any]:
        ...

    def upsert_management_state(
        self,
        record: Dict[str, Any],
        previous_state: Dict[str, Any],
        alert_outcome: Dict[str, Any],
        evaluated_at: datetime,
    ) -> None:
        ...

    def record_alert(
        self,
        *,
        trade_id: int,
        system_name: str,
        trade_mode: str,
        alert_type: str,
        alert_priority: int,
        alert_priority_label: str,
        reason_code: str | None,
        title: str,
        body: str,
        sent_at: str,
    ) -> None:
        ...

    def update_trade_notification_state(
        self,
        *,
        trade_id: int,
        last_status: str,
        last_action_sent: str,
        last_alert_timestamp: str | None,
    ) -> None:
        ...

    def stamp_trade_alert_timestamps(self, trade_ids: List[int], sent_at: str | None) -> None:
        ...

    def has_matching_alert(self, *, alert_type: str, sent_on: str, title: str, body: str) -> bool:
        ...

    def mark_runtime_setting(self, column_name: str, value: str) -> None:
        ...


class SQLiteOpenTradeManagementStateRepository:
    """Adapter that preserves the current SQLite management-state behavior."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.executescript(MANAGEMENT_SCHEMA)
            connection.execute(
                """
                INSERT INTO open_trade_management_runtime_settings (singleton_id, notifications_enabled)
                VALUES (1, 1)
                ON CONFLICT(singleton_id) DO NOTHING
                """
            )
            connection.commit()

    def set_notifications_enabled(self, enabled: bool) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                "UPDATE open_trade_management_runtime_settings SET notifications_enabled = ? WHERE singleton_id = 1",
                (1 if enabled else 0,),
            )
            connection.commit()

    def load_runtime_settings(self) -> Dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM open_trade_management_runtime_settings WHERE singleton_id = 1"
            ).fetchone()
            return dict(row) if row else {"notifications_enabled": True}

    def load_trade(self, trade_id: int) -> Dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            return dict(row) if row else {}

    def load_management_states(self) -> Dict[int, Dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM open_trade_management_state").fetchall()
            return {int(row["trade_id"]): dict(row) for row in rows}

    def load_management_state(self, trade_id: int) -> Dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM open_trade_management_state WHERE trade_id = ?",
                (trade_id,),
            ).fetchone()
            return dict(row) if row else {}

    def upsert_management_state(
        self,
        record: Dict[str, Any],
        previous_state: Dict[str, Any],
        alert_outcome: Dict[str, Any],
        evaluated_at: datetime,
    ) -> None:
        existing_alert_count = int(previous_state.get("alert_count") or 0)
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO open_trade_management_state (
                    trade_id, system_name, trade_mode, current_management_status, previous_management_status,
                    current_thesis_status, previous_thesis_status, concise_reason, reason_code, next_trigger,
                    trigger_source, last_evaluated_at, last_alert_sent_at, last_alert_type, last_alert_priority,
                    alert_count, last_alert_reason_code, last_underlying_price, last_distance_to_short,
                    last_distance_to_long, last_buffer_remaining_percent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    system_name = excluded.system_name,
                    trade_mode = excluded.trade_mode,
                    previous_management_status = open_trade_management_state.current_management_status,
                    current_management_status = excluded.current_management_status,
                    previous_thesis_status = open_trade_management_state.current_thesis_status,
                    current_thesis_status = excluded.current_thesis_status,
                    concise_reason = excluded.concise_reason,
                    reason_code = excluded.reason_code,
                    next_trigger = excluded.next_trigger,
                    trigger_source = excluded.trigger_source,
                    last_evaluated_at = excluded.last_evaluated_at,
                    last_alert_sent_at = COALESCE(excluded.last_alert_sent_at, open_trade_management_state.last_alert_sent_at),
                    last_alert_type = COALESCE(excluded.last_alert_type, open_trade_management_state.last_alert_type),
                    last_alert_priority = COALESCE(excluded.last_alert_priority, open_trade_management_state.last_alert_priority),
                    alert_count = excluded.alert_count,
                    last_alert_reason_code = COALESCE(excluded.last_alert_reason_code, open_trade_management_state.last_alert_reason_code),
                    last_underlying_price = excluded.last_underlying_price,
                    last_distance_to_short = excluded.last_distance_to_short,
                    last_distance_to_long = excluded.last_distance_to_long,
                    last_buffer_remaining_percent = excluded.last_buffer_remaining_percent
                """,
                (
                    record["trade_id"],
                    record["system_name"],
                    str(record["trade_mode"]).lower(),
                    record["status"],
                    previous_state.get("current_management_status"),
                    record["thesis_status"],
                    previous_state.get("current_thesis_status"),
                    record["reason"],
                    record["reason_code"],
                    record["next_trigger"],
                    record["trigger_source"],
                    evaluated_at.isoformat(),
                    alert_outcome.get("sent_at"),
                    alert_outcome.get("alert_type"),
                    alert_outcome.get("priority"),
                    existing_alert_count + int(alert_outcome.get("sent_count") or (1 if alert_outcome.get("sent") else 0)),
                    record["reason_code"] if alert_outcome.get("sent") else previous_state.get("last_alert_reason_code"),
                    record.get("current_underlying_price"),
                    record.get("distance_to_short"),
                    record.get("distance_to_long"),
                    record.get("percent_buffer_remaining"),
                ),
            )
            connection.commit()

    def record_alert(
        self,
        *,
        trade_id: int,
        system_name: str,
        trade_mode: str,
        alert_type: str,
        alert_priority: int,
        alert_priority_label: str,
        reason_code: str | None,
        title: str,
        body: str,
        sent_at: str,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO open_trade_management_alert_log (
                    trade_id, system_name, trade_mode, alert_type, alert_priority, alert_priority_label,
                    reason_code, title, body, sent_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    system_name,
                    trade_mode,
                    alert_type,
                    alert_priority,
                    alert_priority_label,
                    reason_code,
                    title,
                    body,
                    sent_at,
                ),
            )
            connection.commit()

    def update_trade_notification_state(
        self,
        *,
        trade_id: int,
        last_status: str,
        last_action_sent: str,
        last_alert_timestamp: str | None,
    ) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                """
                UPDATE trades
                SET last_status = ?,
                    last_action_sent = ?,
                    last_alert_timestamp = COALESCE(?, last_alert_timestamp),
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    last_status or None,
                    last_action_sent or None,
                    last_alert_timestamp,
                    current_timestamp(),
                    trade_id,
                ),
            )
            connection.commit()

    def stamp_trade_alert_timestamps(self, trade_ids: List[int], sent_at: str | None) -> None:
        if not sent_at:
            return
        with closing(self._connect()) as connection:
            for trade_id in [item for item in trade_ids if item > 0]:
                connection.execute(
                    "UPDATE trades SET last_alert_timestamp = ?, updated_at = ? WHERE id = ?",
                    (sent_at, current_timestamp(), trade_id),
                )
            connection.commit()

    def has_matching_alert(self, *, alert_type: str, sent_on: str, title: str, body: str) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT 1
                FROM open_trade_management_alert_log
                WHERE alert_type = ?
                  AND sent_at >= ?
                  AND sent_at < ?
                LIMIT 1
                """,
                                (alert_type, f"{sent_on}T00:00:00", f"{sent_on}T23:59:59.999999"),
            ).fetchone()
            return row is not None

    def mark_runtime_setting(self, column_name: str, value: str) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                f"UPDATE open_trade_management_runtime_settings SET {column_name} = ? WHERE singleton_id = 1",
                (value,),
            )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection


class SupabaseOpenTradeManagementStateRepository:
    """Supabase-backed adapter for hosted open-trade management state."""

    def __init__(self, gateway: SupabaseTableGateway) -> None:
        self.gateway = gateway

    def initialize(self) -> None:
        existing = self._load_runtime_settings_row()
        if existing is None:
            self._insert(
                MANAGEMENT_RUNTIME_SETTINGS_TABLE,
                {
                    "singleton_id": 1,
                    "notifications_enabled": True,
                    "last_morning_snapshot_date": None,
                    "last_eod_summary_date": None,
                    "last_background_run_at": None,
                },
            )

    def set_notifications_enabled(self, enabled: bool) -> None:
        self._upsert_runtime_settings({"notifications_enabled": bool(enabled)})

    def load_runtime_settings(self) -> Dict[str, Any]:
        row = self._load_runtime_settings_row()
        if row is None:
            return {"notifications_enabled": True}
        return dict(row)

    def load_trade(self, trade_id: int) -> Dict[str, Any]:
        rows = self._select(JOURNAL_TRADES_TABLE, filters={"id": f"eq.{int(trade_id)}"}, limit=1)
        return dict(rows[0]) if rows else {}

    def load_management_states(self) -> Dict[int, Dict[str, Any]]:
        rows = self._select(ACTIVE_TRADES_TABLE)
        return {int(row["trade_id"]): dict(row) for row in rows if row.get("trade_id") is not None}

    def load_management_state(self, trade_id: int) -> Dict[str, Any]:
        rows = self._select(ACTIVE_TRADES_TABLE, filters={"trade_id": f"eq.{int(trade_id)}"}, limit=1)
        return dict(rows[0]) if rows else {}

    def upsert_management_state(
        self,
        record: Dict[str, Any],
        previous_state: Dict[str, Any],
        alert_outcome: Dict[str, Any],
        evaluated_at: datetime,
    ) -> None:
        payload = build_management_state_payload(record, previous_state, alert_outcome, evaluated_at)
        existing = bool(previous_state)
        if existing:
            self._update(ACTIVE_TRADES_TABLE, payload, filters={"trade_id": f"eq.{int(record['trade_id'])}"})
            return
        self._insert(ACTIVE_TRADES_TABLE, payload)

    def record_alert(
        self,
        *,
        trade_id: int,
        system_name: str,
        trade_mode: str,
        alert_type: str,
        alert_priority: int,
        alert_priority_label: str,
        reason_code: str | None,
        title: str,
        body: str,
        sent_at: str,
    ) -> None:
        self._insert(
            ACTIVE_TRADE_ALERT_LOG_TABLE,
            {
                "trade_id": trade_id,
                "system_name": system_name,
                "trade_mode": trade_mode,
                "alert_type": alert_type,
                "alert_priority": alert_priority,
                "alert_priority_label": alert_priority_label,
                "reason_code": reason_code,
                "title": title,
                "body": body,
                "sent_at": sent_at,
            },
        )

    def update_trade_notification_state(
        self,
        *,
        trade_id: int,
        last_status: str,
        last_action_sent: str,
        last_alert_timestamp: str | None,
    ) -> None:
        self._update(
            JOURNAL_TRADES_TABLE,
            {
                "last_status": last_status or None,
                "last_action_sent": last_action_sent or None,
                "last_alert_timestamp": last_alert_timestamp,
                "updated_at": current_timestamp(),
            },
            filters={"id": f"eq.{int(trade_id)}"},
        )

    def stamp_trade_alert_timestamps(self, trade_ids: List[int], sent_at: str | None) -> None:
        if not sent_at:
            return
        for trade_id in [item for item in trade_ids if item > 0]:
            self._update(
                JOURNAL_TRADES_TABLE,
                {"last_alert_timestamp": sent_at, "updated_at": current_timestamp()},
                filters={"id": f"eq.{int(trade_id)}"},
            )

    def has_matching_alert(self, *, alert_type: str, sent_on: str, title: str, body: str) -> bool:
        rows = self._select(
            ACTIVE_TRADE_ALERT_LOG_TABLE,
            filters={
                "alert_type": f"eq.{alert_type}",
                "sent_at": f"gte.{sent_on}T00:00:00",
            },
            limit=20,
        )
        return any(
            str(row.get("sent_at") or "") < f"{sent_on}T23:59:59.999999"
            for row in rows
        )

    def mark_runtime_setting(self, column_name: str, value: str) -> None:
        self._upsert_runtime_settings({column_name: value})

    def _load_runtime_settings_row(self) -> Dict[str, Any] | None:
        rows = self._select(MANAGEMENT_RUNTIME_SETTINGS_TABLE, filters={"singleton_id": "eq.1"}, limit=1)
        return dict(rows[0]) if rows else None

    def _upsert_runtime_settings(self, values: Dict[str, Any]) -> None:
        existing = self._load_runtime_settings_row()
        if existing is None:
            self._insert(MANAGEMENT_RUNTIME_SETTINGS_TABLE, {"singleton_id": 1, **values})
            return
        self._update(MANAGEMENT_RUNTIME_SETTINGS_TABLE, values, filters={"singleton_id": "eq.1"})

    def _select(self, table: str, **kwargs: Any) -> List[Dict[str, Any]]:
        try:
            return self.gateway.select(table, **kwargs)
        except SupabaseRequestError as exc:
            return []

    def _insert(self, table: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            return self.gateway.insert(table, payload)
        except SupabaseRequestError as exc:
            return []

    def _update(self, table: str, payload: Dict[str, Any], *, filters: Dict[str, str]) -> List[Dict[str, Any]]:
        try:
            return self.gateway.update(table, payload, filters=filters)
        except SupabaseRequestError as exc:
            return []