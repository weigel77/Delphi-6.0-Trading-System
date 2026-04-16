"""Import-preview repository abstraction and filesystem adapter."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4
from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class ImportPreviewRepository(Protocol):
    """Abstract persistence contract for trade import preview payloads."""

    def store_preview(self, trade_mode: str, preview_payload: Dict[str, Any]) -> str:
        ...

    def load_preview(self, token: str) -> Optional[Dict[str, Any]]:
        ...

    def delete_preview(self, token: str) -> None:
        ...


class FileSystemImportPreviewRepository:
    """Persist import previews to the existing instance-path JSON store."""

    def __init__(self, root_path: str | Path, *, directory_name: str = "trade_import_previews") -> None:
        self._root_path = Path(root_path)
        self._directory_name = directory_name

    def store_preview(self, trade_mode: str, preview_payload: Dict[str, Any]) -> str:
        token = uuid4().hex
        payload = {"trade_mode": str(trade_mode or "").strip().lower(), **preview_payload}
        preview_path = self._storage_dir() / f"{token}.json"
        preview_path.write_text(json.dumps(payload), encoding="utf-8")
        return token

    def load_preview(self, token: str) -> Optional[Dict[str, Any]]:
        preview_path = self._storage_dir() / f"{self._normalize_token(token)}.json"
        if not preview_path.exists():
            return None
        try:
            return json.loads(preview_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def delete_preview(self, token: str) -> None:
        preview_path = self._storage_dir() / f"{self._normalize_token(token)}.json"
        if preview_path.exists():
            preview_path.unlink()

    def _storage_dir(self) -> Path:
        directory = self._root_path / self._directory_name
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    @staticmethod
    def _normalize_token(token: str) -> str:
        return "".join(character for character in str(token or "") if character.isalnum())