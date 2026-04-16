"""Token persistence abstraction and JSON-file adapter."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from services.token_store import JsonTokenStore


@runtime_checkable
class TokenRepository(Protocol):
    """Abstract token persistence contract for OAuth flows."""

    @property
    def file_path(self) -> Path:
        ...

    def load(self) -> Optional[Dict[str, Any]]:
        ...

    def save(self, token_payload: Dict[str, Any]) -> None:
        ...

    def clear(self) -> None:
        ...


class JsonFileTokenRepository:
    """Adapter that preserves the current JSON token file behavior."""

    def __init__(self, file_path: str | Path, store: JsonTokenStore | None = None) -> None:
        self._store = store or JsonTokenStore(str(file_path))

    @property
    def file_path(self) -> Path:
        return self._store.file_path

    def load(self) -> Optional[Dict[str, Any]]:
        return self._store.load()

    def save(self, token_payload: Dict[str, Any]) -> None:
        self._store.save(token_payload)

    def clear(self) -> None:
        self._store.clear()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)