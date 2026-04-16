"""Per-trade notification settings persistence."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Protocol, runtime_checkable

from services.repositories.hosted_runtime_state_repository import SupabaseHostedRuntimeStateRepository
from services.runtime.supabase_integration import SupabaseRuntimeContext, SupabaseTableGateway
from services.trade_notifications import normalize_trade_notifications
from services.trade_store import current_timestamp


TRADE_NOTIFICATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS trade_notification_settings (
    trade_id INTEGER PRIMARY KEY,
    notifications_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
"""


@runtime_checkable
class TradeNotificationRepository(Protocol):
    def initialize(self) -> None:
        ...

    def load_trade_notifications(self, trade_id: int) -> List[Dict[str, Any]]:
        ...

    def load_notifications_for_trade_ids(self, trade_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        ...

    def save_trade_notifications(self, trade_id: int, notifications: List[Dict[str, Any]]) -> None:
        ...


class SQLiteTradeNotificationRepository:
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.executescript(TRADE_NOTIFICATION_SCHEMA)
            connection.commit()

    def load_trade_notifications(self, trade_id: int) -> List[Dict[str, Any]]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT notifications_json FROM trade_notification_settings WHERE trade_id = ?",
                (int(trade_id),),
            ).fetchone()
            if not row:
                return normalize_trade_notifications([])
            return normalize_trade_notifications(row["notifications_json"])

    def load_notifications_for_trade_ids(self, trade_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        normalized_trade_ids = sorted({int(trade_id) for trade_id in trade_ids if int(trade_id) > 0})
        if not normalized_trade_ids:
            return {}
        placeholders = ", ".join("?" for _ in normalized_trade_ids)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"SELECT trade_id, notifications_json FROM trade_notification_settings WHERE trade_id IN ({placeholders})",
                normalized_trade_ids,
            ).fetchall()
        loaded = {int(row["trade_id"]): normalize_trade_notifications(row["notifications_json"]) for row in rows}
        return {trade_id: loaded.get(trade_id, normalize_trade_notifications([])) for trade_id in normalized_trade_ids}

    def save_trade_notifications(self, trade_id: int, notifications: List[Dict[str, Any]]) -> None:
        normalized = json.dumps(normalize_trade_notifications(notifications))
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO trade_notification_settings (trade_id, notifications_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    notifications_json = excluded.notifications_json,
                    updated_at = excluded.updated_at
                """,
                (int(trade_id), normalized, current_timestamp()),
            )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection


class SupabaseTradeNotificationRepository:
    def __init__(self, *, context: SupabaseRuntimeContext, gateway: SupabaseTableGateway) -> None:
        self._store = SupabaseHostedRuntimeStateRepository(
            context=context,
            gateway=gateway,
            object_type="trade_notification_settings",
        )

    def initialize(self) -> None:
        return None

    def load_trade_notifications(self, trade_id: int) -> List[Dict[str, Any]]:
        payload = self._store.load_object(self._key(trade_id)) or {}
        return normalize_trade_notifications(payload.get("notifications") or [])

    def load_notifications_for_trade_ids(self, trade_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        normalized_trade_ids = sorted({int(trade_id) for trade_id in trade_ids if int(trade_id) > 0})
        return {trade_id: self.load_trade_notifications(trade_id) for trade_id in normalized_trade_ids}

    def save_trade_notifications(self, trade_id: int, notifications: List[Dict[str, Any]]) -> None:
        self._store.save_object(
            self._key(trade_id),
            {
                "trade_id": int(trade_id),
                "notifications": normalize_trade_notifications(notifications),
                "updated_at": current_timestamp(),
            },
        )

    @staticmethod
    def _key(trade_id: int) -> str:
        return f"trade-{int(trade_id)}"