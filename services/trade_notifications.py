"""Per-trade notification configuration and trigger evaluation."""

from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional


NOTIFICATION_TYPE_SHORT_STRIKE_PROXIMITY = "SHORT_STRIKE_PROXIMITY"
NOTIFICATION_TYPE_LONG_STRIKE_TOUCH = "LONG_STRIKE_TOUCH"
NOTIFICATION_TYPE_VWAP_BREAK = "VWAP_BREAK"
NOTIFICATION_TYPE_STRUCTURE_BREAK = "STRUCTURE_BREAK"
NOTIFICATION_TYPE_TIME_WINDOW = "TIME_WINDOW"

SUPPORTED_NOTIFICATION_TYPES = (
    NOTIFICATION_TYPE_SHORT_STRIKE_PROXIMITY,
    NOTIFICATION_TYPE_LONG_STRIKE_TOUCH,
    NOTIFICATION_TYPE_VWAP_BREAK,
    NOTIFICATION_TYPE_STRUCTURE_BREAK,
    NOTIFICATION_TYPE_TIME_WINDOW,
)

DEFAULT_NOTIFICATION_DEFINITIONS = {
    NOTIFICATION_TYPE_SHORT_STRIKE_PROXIMITY: {
        "threshold": 10.0,
        "description": "Alert when the underlying is within 10 points of the short strike.",
        "threshold_label": "Points to short strike",
    },
    NOTIFICATION_TYPE_LONG_STRIKE_TOUCH: {
        "threshold": None,
        "description": "Alert when price touches or breaches the long strike.",
        "threshold_label": "",
    },
    NOTIFICATION_TYPE_VWAP_BREAK: {
        "threshold": None,
        "description": "Alert when the trade breaks below VWAP context.",
        "threshold_label": "",
    },
    NOTIFICATION_TYPE_STRUCTURE_BREAK: {
        "threshold": None,
        "description": "Alert when the active structure regime breaks.",
        "threshold_label": "",
    },
    NOTIFICATION_TYPE_TIME_WINDOW: {
        "threshold": 2.0,
        "description": "Alert during the final 2 hours before expiration.",
        "threshold_label": "Hours remaining",
    },
}


def default_trade_notifications() -> List[Dict[str, Any]]:
    return [
        {
            "type": notification_type,
            "enabled": False,
            "threshold": definition["threshold"],
            "description": definition["description"],
            "threshold_label": definition["threshold_label"],
        }
        for notification_type, definition in DEFAULT_NOTIFICATION_DEFINITIONS.items()
    ]


def normalize_trade_notifications(payload: Any) -> List[Dict[str, Any]]:
    loaded = payload
    if isinstance(payload, str):
        try:
            loaded = json.loads(payload)
        except json.JSONDecodeError:
            loaded = []
    rows = loaded if isinstance(loaded, list) else []
    configured: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        notification_type = str(row.get("type") or "").strip().upper()
        if notification_type not in SUPPORTED_NOTIFICATION_TYPES:
            continue
        definition = DEFAULT_NOTIFICATION_DEFINITIONS[notification_type]
        configured[notification_type] = {
            "type": notification_type,
            "enabled": bool(row.get("enabled")),
            "threshold": _to_float(row.get("threshold"), fallback=definition["threshold"]),
            "description": str(row.get("description") or definition["description"]).strip() or definition["description"],
            "threshold_label": definition["threshold_label"],
        }
    normalized = []
    for default_row in default_trade_notifications():
        normalized.append(configured.get(default_row["type"], dict(default_row)))
    return normalized


def evaluate_trade_notifications(trade: Dict[str, Any], market_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    notifications = normalize_trade_notifications(trade.get("notifications"))
    distance_to_short = _to_float(market_data.get("distance_to_short"))
    distance_to_long = _to_float(market_data.get("distance_to_long"))
    time_remaining_seconds = _to_float(market_data.get("time_remaining_to_expiration"))
    time_remaining_hours = None if time_remaining_seconds is None else (time_remaining_seconds / 3600.0)
    triggered: List[Dict[str, Any]] = []

    for notification in notifications:
        if not notification.get("enabled"):
            continue
        notification_type = notification["type"]
        threshold = _to_float(notification.get("threshold"))
        reason = None
        if notification_type == NOTIFICATION_TYPE_SHORT_STRIKE_PROXIMITY:
            active_threshold = threshold if threshold is not None else DEFAULT_NOTIFICATION_DEFINITIONS[notification_type]["threshold"]
            if distance_to_short is not None and active_threshold is not None and distance_to_short <= active_threshold:
                reason = f"Short strike is within {distance_to_short:.2f} points (threshold {active_threshold:.2f})."
        elif notification_type == NOTIFICATION_TYPE_LONG_STRIKE_TOUCH:
            if bool(market_data.get("long_proximity_trigger_fired")) or (distance_to_long is not None and distance_to_long <= 0):
                reason = "Long strike has been touched or breached."
        elif notification_type == NOTIFICATION_TYPE_VWAP_BREAK:
            if bool(market_data.get("vwap_trigger_fired")):
                reason = "VWAP break condition is active."
        elif notification_type == NOTIFICATION_TYPE_STRUCTURE_BREAK:
            if bool(market_data.get("structure_trigger_fired")) or str(market_data.get("thesis_status") or "").strip().lower() in {"bearish", "broken"}:
                reason = "Structure break condition is active."
        elif notification_type == NOTIFICATION_TYPE_TIME_WINDOW:
            active_threshold = threshold if threshold is not None else DEFAULT_NOTIFICATION_DEFINITIONS[notification_type]["threshold"]
            if time_remaining_hours is not None and active_threshold is not None and time_remaining_hours <= active_threshold:
                reason = f"Only {time_remaining_hours:.2f} hours remain to expiration (threshold {active_threshold:.2f})."
        if reason:
            triggered.append(
                {
                    "type": notification_type,
                    "description": notification.get("description") or DEFAULT_NOTIFICATION_DEFINITIONS[notification_type]["description"],
                    "threshold": threshold,
                    "reason": reason,
                }
            )
    return triggered


def serialize_trade_notifications(notifications: Iterable[Dict[str, Any]]) -> str:
    return json.dumps(normalize_trade_notifications(list(notifications)))


def _to_float(value: Any, *, fallback: Optional[float] = None) -> Optional[float]:
    if value in {None, ""}:
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback