"""Global notification settings persistence for local and hosted Delphi runtimes."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Protocol, runtime_checkable

from services.repositories.hosted_runtime_state_repository import SupabaseHostedRuntimeStateRepository
from services.runtime.supabase_integration import SupabaseRuntimeContext, SupabaseTableGateway
from services.trade_notifications import normalize_global_notification_settings
from services.trade_store import current_timestamp


GLOBAL_NOTIFICATION_SETTINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS global_notification_settings (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    settings_json TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
"""


@runtime_checkable
class GlobalNotificationSettingsRepository(Protocol):
    def initialize(self) -> None:
        ...

    def load_settings(self) -> List[Dict[str, Any]]:
        ...

    def save_settings(self, settings: List[Dict[str, Any]]) -> None:
        ...


class SQLiteGlobalNotificationSettingsRepository:
    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.executescript(GLOBAL_NOTIFICATION_SETTINGS_SCHEMA)
            connection.execute(
                """
                INSERT INTO global_notification_settings (singleton_id, settings_json, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(singleton_id) DO NOTHING
                """,
                (json.dumps(normalize_global_notification_settings([])), current_timestamp()),
            )
            connection.commit()

    def load_settings(self) -> List[Dict[str, Any]]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT settings_json FROM global_notification_settings WHERE singleton_id = 1"
            ).fetchone()
            if not row:
                return normalize_global_notification_settings([])
            return normalize_global_notification_settings(row["settings_json"])

    def save_settings(self, settings: List[Dict[str, Any]]) -> None:
        normalized = json.dumps(normalize_global_notification_settings(settings))
        with closing(self._connect()) as connection:
            connection.execute(
                """
                INSERT INTO global_notification_settings (singleton_id, settings_json, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(singleton_id) DO UPDATE SET
                    settings_json = excluded.settings_json,
                    updated_at = excluded.updated_at
                """,
                (normalized, current_timestamp()),
            )
            connection.commit()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection


class SupabaseGlobalNotificationSettingsRepository:
    def __init__(self, *, context: SupabaseRuntimeContext, gateway: SupabaseTableGateway) -> None:
        self._store = SupabaseHostedRuntimeStateRepository(
            context=context,
            gateway=gateway,
            object_type="global_notification_settings",
        )

    def initialize(self) -> None:
        existing = self._store.load_object("global")
        if not existing:
            self.save_settings([])

    def load_settings(self) -> List[Dict[str, Any]]:
        payload = self._store.load_object("global") or {}
        return normalize_global_notification_settings(payload.get("settings") or [])

    def save_settings(self, settings: List[Dict[str, Any]]) -> None:
        self._store.save_object(
            "global",
            {
                "settings": normalize_global_notification_settings(settings),
                "updated_at": current_timestamp(),
            },
        )