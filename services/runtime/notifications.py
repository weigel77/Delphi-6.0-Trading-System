"""Notification delivery abstraction and local Pushover adapter."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Protocol, runtime_checkable

from services.pushover_service import PushoverService


@runtime_checkable
class NotificationDelivery(Protocol):
    """Abstract delivery contract for Delphi operator notifications."""

    def send_notification(
        self,
        *,
        title: str,
        message: str,
        priority: int = 0,
        sound: str | None = None,
        url: str | None = None,
        url_title: str | None = None,
        retry: int | None = None,
        expire: int | None = None,
    ) -> Dict[str, Any]:
        ...

    def send_kairos_window_open_alert(
        self,
        *,
        scan_result: Any,
        best_trade_payload: Dict[str, Any],
        generated_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        ...


class PushoverNotificationDelivery:
    """Adapter that preserves the current Pushover-backed delivery behavior."""

    def __init__(self, service: PushoverService) -> None:
        self._service = service

    def send_notification(
        self,
        *,
        title: str,
        message: str,
        priority: int = 0,
        sound: str | None = None,
        url: str | None = None,
        url_title: str | None = None,
        retry: int | None = None,
        expire: int | None = None,
    ) -> Dict[str, Any]:
        return self._service.send_notification(
            title=title,
            message=message,
            priority=priority,
            sound=sound,
            url=url,
            url_title=url_title,
            retry=retry,
            expire=expire,
        )

    def send_kairos_window_open_alert(
        self,
        *,
        scan_result: Any,
        best_trade_payload: Dict[str, Any],
        generated_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        return self._service.send_kairos_window_open_alert(
            scan_result=scan_result,
            best_trade_payload=best_trade_payload,
            generated_at=generated_at,
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._service, name)