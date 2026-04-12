"""Pushover notification helpers for Delphi 4.3 Dev."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import AppConfig, get_app_config


LOGGER = logging.getLogger(__name__)


class PushoverService:
    """Send Delphi notifications through the Pushover message API."""

    API_URL = "https://api.pushover.net/1/messages.json"

    def __init__(
        self,
        config: AppConfig | None = None,
        *,
        request_sender: Callable[[str, Dict[str, Any]], Dict[str, Any]] | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.user_key = self.config.pushover_user_key
        self.api_token = self.config.pushover_api_token
        self._request_sender = request_sender or self._send_http_request

    def is_configured(self) -> bool:
        """Return True when the required Pushover credentials are present."""
        return bool(self.user_key and self.api_token)

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
        """Send a basic Pushover notification."""
        if not self.is_configured():
            return {
                "ok": False,
                "error": "Pushover is not configured. Add PUSHOVER_USER_KEY and PUSHOVER_API_TOKEN to the Delphi 4.3 Dev .env file.",
                "status_code": 503,
            }

        payload: Dict[str, Any] = {
            "token": self.api_token,
            "user": self.user_key,
            "title": str(title or "").strip() or "Delphi 4.3 Alert",
            "message": str(message or "").strip() or "Delphi 4.3 test notification.",
            "priority": int(priority),
        }
        if sound:
            payload["sound"] = str(sound).strip()
        if url:
            payload["url"] = str(url).strip()
        if url_title:
            payload["url_title"] = str(url_title).strip()
        if retry is not None:
            payload["retry"] = int(retry)
        if expire is not None:
            payload["expire"] = int(expire)

        try:
            response_payload = self._request_sender(self.API_URL, payload)
        except HTTPError as exc:
            response_body = self._read_error_body(exc)
            diagnostic = self._extract_error_message(response_body) or f"HTTP {exc.code}"
            LOGGER.warning("Pushover send failed status=%s reason=%s", exc.code, diagnostic)
            return {
                "ok": False,
                "error": f"Pushover notification failed: {diagnostic}",
                "status_code": int(exc.code or 502),
            }
        except URLError as exc:
            LOGGER.warning("Pushover send failed reason=%s", exc.reason)
            return {
                "ok": False,
                "error": "Pushover notification failed: network error while contacting the Pushover API.",
                "status_code": 502,
            }
        except Exception as exc:  # pragma: no cover - defensive transport handling
            LOGGER.warning("Pushover send failed reason=%s", type(exc).__name__)
            return {
                "ok": False,
                "error": "Pushover notification failed unexpectedly. Check the Delphi 4.3 Dev log for details.",
                "status_code": 502,
            }

        request_id = str(response_payload.get("request") or "").strip()
        LOGGER.info("Pushover notification sent request_id=%s", request_id or "pending")
        return {
            "ok": True,
            "message": "Pushover test alert sent.",
            "request_id": request_id,
            "status_code": 200,
        }

    def send_kairos_window_open_alert(
        self,
        *,
        scan_result: Any,
        best_trade_payload: Dict[str, Any],
        generated_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Keep Kairos live alert plumbing working through Pushover."""
        timestamp = self._format_generated_time(generated_at)
        candidate = best_trade_payload.get("candidate") or {}
        profile_label = self._coerce_text(candidate.get("mode_label"), fallback="Kairos")
        strike_pair = self._coerce_text(candidate.get("strike_pair"), fallback="—")
        width_label = self._coerce_text(candidate.get("spread_width_display"), fallback="—")
        credit_label = self._coerce_text(candidate.get("premium_received_display") or candidate.get("credit_estimate_display"), fallback="—")
        contracts_label = self._coerce_text(candidate.get("recommended_contracts_display"), fallback="—")
        spx_value = self._coerce_text(getattr(scan_result, "spx_value", None), fallback="—")
        vix_value = self._coerce_text(getattr(scan_result, "vix_value", None), fallback="—")
        message = (
            f"Kairos window open {timestamp}.\n"
            f"SPX: {spx_value}\n"
            f"VIX: {vix_value}\n"
            f"Trade: {profile_label} {strike_pair} put spread\n"
            f"Width: {width_label}\n"
            f"Premium: {credit_label}\n"
            f"Contracts: {contracts_label}"
        )
        return self.send_notification(title="Delphi 4.3 Kairos Alert", message=message, priority=0)

    def _send_http_request(self, api_url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        encoded_payload = urlencode({key: value for key, value in payload.items() if value not in {None, ""}}).encode("utf-8")
        request = Request(api_url, data=encoded_payload, method="POST")
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urlopen(request, timeout=15) as response:
            return json.loads(response.read().decode("utf-8"))

    @staticmethod
    def _read_error_body(error: HTTPError) -> str:
        try:
            return error.read().decode("utf-8", errors="replace")
        except Exception:
            return ""

    @staticmethod
    def _extract_error_message(response_body: str) -> str:
        if not response_body:
            return ""
        try:
            payload = json.loads(response_body)
        except (TypeError, ValueError):
            return response_body.strip()
        errors = payload.get("errors") or []
        if isinstance(errors, list) and errors:
            return "; ".join(str(item) for item in errors)
        return str(payload.get("error") or "").strip()

    @staticmethod
    def _coerce_text(value: Any, *, fallback: str) -> str:
        text = str(value or "").strip()
        return text or fallback

    @staticmethod
    def _format_generated_time(generated_at: Optional[datetime]) -> str:
        if generated_at is None:
            return ""
        return generated_at.strftime("%I:%M %p %Z").lstrip("0")