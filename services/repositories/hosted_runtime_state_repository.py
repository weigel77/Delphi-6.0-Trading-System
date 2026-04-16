"""Supabase-backed hosted runtime state repositories."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from services.repositories.apollo_snapshot_repository import ApolloSnapshotRepository
from services.repositories.import_preview_repository import ImportPreviewRepository
from services.repositories.token_repository import TokenRepository
from services.runtime.supabase_integration import SupabaseRequestError, SupabaseRuntimeContext, SupabaseTableGateway


class SupabaseHostedRuntimeStateRepository:
    """Small JSON object store for hosted-only runtime state."""

    TABLE = "hosted_runtime_state"

    def __init__(
        self,
        *,
        context: SupabaseRuntimeContext,
        gateway: SupabaseTableGateway,
        object_type: str,
    ) -> None:
        self.context = context
        self.gateway = gateway
        self.object_type = str(object_type or "").strip().lower()
        self._storage_available: bool | None = None

    def load_object(self, object_key: str) -> Optional[Dict[str, Any]]:
        if not self._ensure_storage_available():
            return None
        normalized_key = self._normalize_key(object_key)
        if not normalized_key:
            return None
        rows = self._select_rows(filters=self._build_filters(normalized_key), limit=1)
        if not rows:
            return None
        payload = rows[0].get("payload")
        if isinstance(payload, dict):
            return dict(payload)
        if isinstance(payload, str):
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                return None
            return decoded if isinstance(decoded, dict) else None
        return None

    def save_object(self, object_key: str, payload: Dict[str, Any], *, expires_at: str | None = None) -> None:
        if not self._ensure_storage_available():
            return
        normalized_key = self._normalize_key(object_key)
        if not normalized_key:
            return
        timestamp = datetime.now(timezone.utc).isoformat()
        row_payload = {
            "object_type": self.object_type,
            "object_key": normalized_key,
            "payload": self._normalize_payload(payload),
            "updated_at": timestamp,
            "expires_at": expires_at,
        }
        existing = self._select_rows(filters=self._build_filters(normalized_key), limit=1)
        if existing:
            self.gateway.update(self.TABLE, row_payload, filters=self._build_filters(normalized_key))
            return
        row_payload["created_at"] = timestamp
        self.gateway.insert(self.TABLE, row_payload)

    def delete_object(self, object_key: str) -> None:
        if not self._ensure_storage_available():
            return
        normalized_key = self._normalize_key(object_key)
        if not normalized_key:
            return
        self.gateway.delete(self.TABLE, filters=self._build_filters(normalized_key))

    def _ensure_storage_available(self) -> bool:
        if self._storage_available is not None:
            return self._storage_available
        try:
            self.gateway.select(self.TABLE, limit=1, columns="object_type")
        except SupabaseRequestError:
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
        except SupabaseRequestError:
            self._storage_available = False
            return []

    def _build_filters(self, object_key: str) -> Dict[str, str]:
        return {
            "object_type": f"eq.{self.object_type}",
            "object_key": f"eq.{object_key}",
        }

    @staticmethod
    def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = json.loads(json.dumps(payload, default=str))
        return normalized if isinstance(normalized, dict) else {}

    @staticmethod
    def _normalize_key(value: Any) -> str:
        return str(value or "").strip().lower()


class SupabaseApolloSnapshotRepository(ApolloSnapshotRepository):
    """Persist Apollo hosted snapshots in Supabase."""

    SNAPSHOT_KEY = "latest"

    def __init__(self, *, context: SupabaseRuntimeContext, gateway: SupabaseTableGateway) -> None:
        self._store = SupabaseHostedRuntimeStateRepository(
            context=context,
            gateway=gateway,
            object_type="apollo_snapshot",
        )

    def save_snapshot(self, payload: Dict[str, Any]) -> None:
        self._store.save_object(self.SNAPSHOT_KEY, payload)

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        return self._store.load_object(self.SNAPSHOT_KEY)


class SupabaseImportPreviewRepository(ImportPreviewRepository):
    """Persist hosted trade import previews in Supabase."""

    def __init__(self, *, context: SupabaseRuntimeContext, gateway: SupabaseTableGateway) -> None:
        self._store = SupabaseHostedRuntimeStateRepository(
            context=context,
            gateway=gateway,
            object_type="trade_import_preview",
        )

    def store_preview(self, trade_mode: str, preview_payload: Dict[str, Any]) -> str:
        token = uuid4().hex
        payload = {"trade_mode": str(trade_mode or "").strip().lower(), **preview_payload}
        self._store.save_object(token, payload)
        return token

    def load_preview(self, token: str) -> Optional[Dict[str, Any]]:
        return self._store.load_object(token)

    def delete_preview(self, token: str) -> None:
        self._store.delete_object(token)


class SupabaseTokenRepository(TokenRepository):
    """Persist hosted Schwab OAuth tokens in Supabase."""

    TOKEN_KEY = "default"

    def __init__(self, *, context: SupabaseRuntimeContext, gateway: SupabaseTableGateway) -> None:
        self._store = SupabaseHostedRuntimeStateRepository(
            context=context,
            gateway=gateway,
            object_type="schwab_oauth_token",
        )
        self._file_path = Path("supabase") / self._store.TABLE / f"{self._store.object_type}-{self.TOKEN_KEY}.json"

    @property
    def file_path(self) -> Path:
        return self._file_path

    def load(self) -> Optional[Dict[str, Any]]:
        return self._store.load_object(self.TOKEN_KEY)

    def save(self, token_payload: Dict[str, Any]) -> None:
        self._store.save_object(self.TOKEN_KEY, token_payload)

    def clear(self) -> None:
        self._store.delete_object(self.TOKEN_KEY)