"""Kairos last-run snapshot repository abstraction and adapters."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from services.runtime.supabase_integration import SupabaseRequestError, SupabaseRuntimeContext, SupabaseTableGateway


@runtime_checkable
class KairosSnapshotRepository(Protocol):
    """Abstract persistence contract for the latest hosted-safe Kairos payload."""

    def save_snapshot(self, payload: Dict[str, Any]) -> None:
        ...

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        ...


class JsonFileKairosSnapshotRepository:
    """Persist the latest Kairos hosted-safe payload to a JSON file."""

    def __init__(self, file_path: str | Path) -> None:
        self._file_path = Path(file_path)

    @property
    def file_path(self) -> Path:
        return self._file_path

    def save_snapshot(self, payload: Dict[str, Any]) -> None:
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._file_path.write_text(json.dumps(payload, default=str), encoding="utf-8")

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        if not self._file_path.exists():
            return None
        try:
            data = json.loads(self._file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None


class SupabaseKairosSnapshotRepository:
    """Persist the latest Kairos hosted-safe payload to Supabase."""

    TABLE = "kairos_snapshots"
    SNAPSHOT_KEY = "latest"

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext,
        gateway: SupabaseTableGateway,
        snapshot_key: str = SNAPSHOT_KEY,
    ) -> None:
        self.context = context
        self.gateway = gateway
        self.snapshot_key = snapshot_key
        self._storage_available: bool | None = None

    def save_snapshot(self, payload: Dict[str, Any]) -> None:
        if not self._ensure_storage_available():
            return
        row_payload = {
            "snapshot_key": self.snapshot_key,
            "payload": self._normalize_payload(payload),
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        existing = self._select_rows(filters={"snapshot_key": f"eq.{self.snapshot_key}"}, limit=1)
        if existing:
            self.gateway.update(
                self.TABLE,
                row_payload,
                filters={"snapshot_key": f"eq.{self.snapshot_key}"},
            )
            return
        self.gateway.insert(self.TABLE, row_payload)

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        if not self._ensure_storage_available():
            return None
        rows = self._select_rows(filters={"snapshot_key": f"eq.{self.snapshot_key}"}, limit=1)
        if not rows:
            return None
        payload = rows[0].get("payload")
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                return None
            return decoded if isinstance(decoded, dict) else None
        return None

    def _ensure_storage_available(self) -> bool:
        if self._storage_available is not None:
            return self._storage_available
        try:
            self.gateway.select(self.TABLE, limit=1, columns="snapshot_key")
        except SupabaseRequestError as exc:
            self._storage_available = False
            return False
        self._storage_available = True
        return True

    def _select_rows(
        self,
        *,
        filters: Dict[str, str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        try:
            return self.gateway.select(self.TABLE, filters=filters, limit=limit)
        except SupabaseRequestError as exc:
            self._storage_available = False
            return []

    @staticmethod
    def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = json.loads(json.dumps(payload, default=str))
        return normalized if isinstance(normalized, dict) else {}

    @classmethod
    def _is_missing_table_error(cls, error: SupabaseRequestError) -> bool:
        message = str(error)
        return cls.TABLE in message and "schema cache" in message