"""Workflow/session-state abstraction and Flask-session adapter."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional, Protocol, runtime_checkable

from flask import session


@runtime_checkable
class WorkflowStateStore(Protocol):
    """Abstract contract for transient request-scoped workflow state."""

    def put(self, key: str, value: Any) -> None:
        ...

    def get(self, key: str, default: Any = None) -> Any:
        ...

    def pop(self, key: str, default: Any = None) -> Any:
        ...

    def set_status_message(self, message: str, level: str = "info") -> None:
        ...

    def pop_status_message(self) -> Optional[Dict[str, str]]:
        ...

    def store_trade_prefill(self, trade_mode: str, values: Dict[str, Any]) -> None:
        ...

    def get_trade_prefill(self, trade_mode: str) -> Optional[Dict[str, Any]]:
        ...

    def clear_trade_prefill(self, trade_mode: Optional[str] = None) -> None:
        ...

    def store_trade_close_prefill(self, trade_id: int, values: Dict[str, Any]) -> None:
        ...

    def get_trade_close_prefill(self, trade_id: int) -> Optional[Dict[str, Any]]:
        ...

    def clear_trade_close_prefill(self, trade_id: Optional[int] = None) -> None:
        ...


class FlaskSessionWorkflowState:
    """Adapter that preserves the current Flask session-backed workflow state."""

    def __init__(
        self,
        *,
        status_message_key: str = "status_message",
        trade_prefill_key: str,
        trade_close_prefill_key: str,
        trade_form_fields: Iterable[str],
        trade_mode_resolver: Callable[[str], str],
    ) -> None:
        self._status_message_key = status_message_key
        self._trade_prefill_key = trade_prefill_key
        self._trade_close_prefill_key = trade_close_prefill_key
        self._trade_form_fields = tuple(trade_form_fields)
        self._trade_mode_resolver = trade_mode_resolver

    def put(self, key: str, value: Any) -> None:
        session[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return session.get(key, default)

    def pop(self, key: str, default: Any = None) -> Any:
        return session.pop(key, default)

    def set_status_message(self, message: str, level: str = "info") -> None:
        session[self._status_message_key] = {"text": message, "level": level}

    def pop_status_message(self) -> Optional[Dict[str, str]]:
        return session.pop(self._status_message_key, None)

    def store_trade_prefill(self, trade_mode: str, values: Dict[str, Any]) -> None:
        drafts = dict(session.get(self._trade_prefill_key, {}))
        drafts[self._trade_mode_resolver(trade_mode)] = {key: values.get(key, "") for key in self._trade_form_fields}
        session[self._trade_prefill_key] = drafts

    def get_trade_prefill(self, trade_mode: str) -> Optional[Dict[str, Any]]:
        drafts = session.get(self._trade_prefill_key, {}) or {}
        draft = drafts.get(self._trade_mode_resolver(trade_mode))
        return dict(draft) if isinstance(draft, dict) else None

    def clear_trade_prefill(self, trade_mode: Optional[str] = None) -> None:
        if trade_mode is None:
            session.pop(self._trade_prefill_key, None)
            return

        drafts = dict(session.get(self._trade_prefill_key, {}))
        drafts.pop(self._trade_mode_resolver(trade_mode), None)
        if drafts:
            session[self._trade_prefill_key] = drafts
        else:
            session.pop(self._trade_prefill_key, None)

    def store_trade_close_prefill(self, trade_id: int, values: Dict[str, Any]) -> None:
        drafts = dict(session.get(self._trade_close_prefill_key, {}))
        drafts[str(int(trade_id))] = dict(values)
        session[self._trade_close_prefill_key] = drafts

    def get_trade_close_prefill(self, trade_id: int) -> Optional[Dict[str, Any]]:
        drafts = session.get(self._trade_close_prefill_key, {}) or {}
        draft = drafts.get(str(int(trade_id)))
        return dict(draft) if isinstance(draft, dict) else None

    def clear_trade_close_prefill(self, trade_id: Optional[int] = None) -> None:
        if trade_id is None:
            session.pop(self._trade_close_prefill_key, None)
            return

        drafts = dict(session.get(self._trade_close_prefill_key, {}))
        drafts.pop(str(int(trade_id)), None)
        if drafts:
            session[self._trade_close_prefill_key] = drafts
        else:
            session.pop(self._trade_close_prefill_key, None)