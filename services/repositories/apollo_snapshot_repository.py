"""Apollo last-run snapshot repository abstraction and filesystem adapter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class ApolloSnapshotRepository(Protocol):
    """Abstract persistence contract for the latest Apollo template payload."""

    def save_snapshot(self, payload: Dict[str, Any]) -> None:
        ...

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        ...


class JsonFileApolloSnapshotRepository:
    """Persist the latest Apollo hosted-safe payload to a JSON file."""

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