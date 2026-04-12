"""Open-trade management, scheduled notifications, and deduped alerts for Delphi 4.3 Dev."""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from datetime import date, datetime, time, timedelta
from pathlib import Path
from threading import RLock, Timer
from typing import Any, Callable, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from config import AppConfig, get_app_config

from .apollo_service import ApolloService
from .market_calendar_service import MarketCalendarService
from .market_data import MarketDataError, MarketDataService
from .options_chain_service import OptionsChainService
from .pushover_service import PushoverService
from .trade_store import (
    TradeStore,
    current_timestamp,
    normalize_candidate_profile,
    normalize_system_name,
    parse_date_value,
    parse_datetime_value,
    resolve_trade_credit_model,
    to_float,
)


LOGGER = logging.getLogger(__name__)

MANAGEMENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS open_trade_management_state (
    trade_id INTEGER PRIMARY KEY,
    system_name TEXT NOT NULL,
    trade_mode TEXT NOT NULL,
    current_management_status TEXT,
    previous_management_status TEXT,
    current_thesis_status TEXT,
    previous_thesis_status TEXT,
    concise_reason TEXT,
    reason_code TEXT,
    next_trigger TEXT,
    trigger_source TEXT,
    last_evaluated_at TEXT,
    last_alert_sent_at TEXT,
    last_alert_type TEXT,
    last_alert_priority INTEGER,
    alert_count INTEGER NOT NULL DEFAULT 0,
    last_alert_reason_code TEXT,
    last_underlying_price REAL,
    last_distance_to_short REAL,
    last_distance_to_long REAL,
    last_buffer_remaining_percent REAL
);

CREATE TABLE IF NOT EXISTS open_trade_management_alert_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id INTEGER NOT NULL,
    system_name TEXT NOT NULL,
    trade_mode TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    alert_priority INTEGER NOT NULL,
    alert_priority_label TEXT NOT NULL,
    reason_code TEXT,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    sent_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS open_trade_management_runtime_settings (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    notifications_enabled INTEGER NOT NULL DEFAULT 1,
    last_morning_snapshot_date TEXT,
    last_eod_summary_date TEXT,
    last_background_run_at TEXT
);
"""


class OpenTradeManager:
    """Classify open trades and send low-noise management alerts."""

    STATUS_SEVERITY = {
        "Closed": 0,
        "Healthy": 0,
        "Watch": 1,
        "Exit Approaching": 2,
        "Exit Partial": 3,
        "Exit Now": 4,
    }
    ALERT_COOLDOWN_MINUTES = {
        "Watch": 45,
        "Exit Approaching": 20,
        "Exit Partial": 20,
        "Exit Now": 5,
    }
    MARKET_OPEN_HOUR = 8
    MARKET_OPEN_MINUTE = 30
    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 0
    MORNING_SNAPSHOT_DELAY_MINUTES = 2
    EOD_SUMMARY_DELAY_MINUTES = 10
    BACKGROUND_INTERVAL_SECONDS = 60
    APOLLO_HEALTHY_EM_THRESHOLD = 2.0
    APOLLO_WATCH_EM_THRESHOLD = 1.5
    KAIROS_HEALTHY_EM_THRESHOLD = 3.0
    KAIROS_WATCH_EM_THRESHOLD = 1.5

    def __init__(
        self,
        *,
        trade_store: TradeStore,
        market_data_service: MarketDataService,
        apollo_service: ApolloService,
        options_chain_service: OptionsChainService,
        pushover_service: PushoverService,
        kairos_service: Any,
        market_calendar_service: MarketCalendarService | None = None,
        config: AppConfig | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.trade_store = trade_store
        self.market_data_service = market_data_service
        self.apollo_service = apollo_service
        self.options_chain_service = options_chain_service
        self.pushover_service = pushover_service
        self.kairos_service = kairos_service
        self.market_calendar_service = market_calendar_service or MarketCalendarService(self.config)
        self.database_path = Path(self.trade_store.database_path)
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self.now_provider = now_provider or (lambda: datetime.now(self.display_timezone))
        self._monitor_lock = RLock()
        self._monitor_timer: Timer | None = None
        self._monitor_running = False

    def initialize(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            connection.executescript(MANAGEMENT_SCHEMA)
            connection.execute(
                """
                INSERT INTO open_trade_management_runtime_settings (singleton_id, notifications_enabled)
                VALUES (1, 1)
                ON CONFLICT(singleton_id) DO NOTHING
                """
            )
            connection.commit()

    def start_background_monitoring(self) -> None:
        with self._monitor_lock:
            if self._monitor_running:
                return
            self._monitor_running = True
            self._schedule_background_monitor()

    def shutdown(self) -> None:
        with self._monitor_lock:
            self._monitor_running = False
            if self._monitor_timer is not None:
                self._monitor_timer.cancel()
                self._monitor_timer = None

    def set_notifications_enabled(self, enabled: bool) -> None:
        with closing(self._connect()) as connection:
            connection.execute(
                "UPDATE open_trade_management_runtime_settings SET notifications_enabled = ? WHERE singleton_id = 1",
                (1 if enabled else 0,),
            )
            connection.commit()

    def notifications_enabled(self) -> bool:
        return bool(self._load_runtime_settings().get("notifications_enabled", True))

    def _schedule_background_monitor(self) -> None:
        if not self._monitor_running:
            return
        self._monitor_timer = Timer(self.BACKGROUND_INTERVAL_SECONDS, self._background_monitor_tick)
        self._monitor_timer.daemon = True
        self._monitor_timer.start()

    def _background_monitor_tick(self) -> None:
        try:
            self.run_background_monitor_cycle()
        except Exception as exc:  # pragma: no cover - defensive scheduler guard
            LOGGER.warning("Open trade background monitor failed: %s", exc)
        finally:
            with self._monitor_lock:
                if self._monitor_running:
                    self._schedule_background_monitor()

    def run_background_monitor_cycle(self) -> Dict[str, Any]:
        now = self._now()
        if not self._is_monitor_window(now):
            return {"ran": False, "reason": "outside-monitor-window", "evaluated_at": now.isoformat()}
        payload = self.evaluate_open_trades(send_alerts=True)
        with closing(self._connect()) as connection:
            connection.execute(
                "UPDATE open_trade_management_runtime_settings SET last_background_run_at = ? WHERE singleton_id = 1",
                (now.isoformat(),),
            )
            connection.commit()
        return {"ran": True, **payload}

    def evaluate_open_trades(self, *, send_alerts: bool = False) -> Dict[str, Any]:
        now = self._now()
        open_trades = self._load_open_trades()
        alert_eligible_trades = [trade for trade in open_trades if str(trade.get("trade_mode") or "").strip().lower() == "real"]
        states_by_trade = self._load_management_states()
        runtime_settings = self._load_runtime_settings()
        shared_context = self._build_shared_context(open_trades, now)
        alerts_sent = 0
        alert_failures: List[str] = []
        records: List[Dict[str, Any]] = []

        with closing(self._connect()) as connection:
            daily_outcomes: list[Dict[str, Any]] = []
            for trade in open_trades:
                previous_state = states_by_trade.get(int(trade.get("id") or 0), {})
                record = self._evaluate_trade(trade, shared_context, now)
                alert_outcome = {"sent": False, "error": "", "priority": None, "alert_type": None, "sent_at": None}
                record["trade_notification_state"] = {
                    "last_status": self._coerce_text(trade.get("last_status"), fallback="—"),
                    "last_action_sent": self._coerce_text(trade.get("last_action_sent"), fallback="—"),
                    "last_alert_timestamp": self._format_datetime(parse_datetime_value(trade.get("last_alert_timestamp"))),
                }
                self._upsert_management_state(connection, record, previous_state, alert_outcome, now)
                persisted_state = self._load_management_state(connection, int(record["trade_id"]))
                record["alert_state"] = self._build_alert_state_payload(persisted_state, trade)
                records.append(record)

            if send_alerts and runtime_settings.get("notifications_enabled", True):
                real_records = [
                    item for item in records if str(item.get("trade_mode") or "").strip().lower() == "real"
                ]
                daily_outcomes = self._process_daily_notifications(connection, real_records, now, runtime_settings)
                for outcome in daily_outcomes:
                    if outcome.get("sent"):
                        alerts_sent += 1
                    elif outcome.get("error"):
                        alert_failures.append(str(outcome["error"]))
                for trade in alert_eligible_trades:
                    record = next((item for item in records if int(item.get("trade_id") or 0) == int(trade.get("id") or 0)), None)
                    if record is None:
                        continue
                    previous_state = states_by_trade.get(int(trade.get("id") or 0), {})
                    alert_outcome = self._process_trade_notifications(connection, trade, record, previous_state, now)
                    if alert_outcome.get("sent"):
                        alerts_sent += int(alert_outcome.get("sent_count") or 1)
                    elif alert_outcome.get("error"):
                        alert_failures.append(f"Trade #{record['trade_number']}: {alert_outcome['error']}")
                    self._upsert_management_state(connection, record, previous_state, alert_outcome, now)
                    persisted_state = self._load_management_state(connection, int(record["trade_id"]))
                    record["alert_state"] = self._build_alert_state_payload(persisted_state, self._load_trade(connection, int(record["trade_id"])))
            connection.commit()

        records.sort(key=lambda item: (-int(item.get("status_severity") or 0), str(item.get("system_name") or ""), -int(item.get("trade_number") or 0)))
        return {
            "evaluated_at": now.isoformat(),
            "evaluated_at_display": self._format_datetime(now),
            "open_trade_count": len(records),
            "alerts_sent": alerts_sent,
            "alert_failures": alert_failures,
            "notifications_enabled": bool(runtime_settings.get("notifications_enabled", True)),
            "last_morning_snapshot_date": runtime_settings.get("last_morning_snapshot_date") or "",
            "last_eod_summary_date": runtime_settings.get("last_eod_summary_date") or "",
            "status_counts": self._build_status_counts(records),
            "records": records,
        }

    def send_manual_status_update(self, *, trade_mode: str) -> Dict[str, Any]:
        normalized_trade_mode = str(trade_mode or "").strip().lower()
        if normalized_trade_mode not in {"real", "simulated"}:
            raise ValueError(f"Unsupported trade mode for manual status update: {trade_mode}")

        payload = self.evaluate_open_trades(send_alerts=False)
        selected_records = [
            item for item in payload["records"] if str(item.get("trade_mode") or "").strip().lower() == normalized_trade_mode
        ]
        now = self._now()

        if not selected_records:
            return {
                "sent": False,
                "error": "",
                "trade_mode": normalized_trade_mode,
                "record_count": 0,
                "priority": 0,
                "alert_type": f"manual-{normalized_trade_mode}-status-update",
                "sent_at": None,
            }

        snapshot_payload = self._build_open_positions_snapshot_payload(
            selected_records,
            alert_type=f"manual-{normalized_trade_mode}-status-update",
            reason_code=f"manual-{normalized_trade_mode}-status-update",
        )
        with closing(self._connect()) as connection:
            outcome = self._send_management_alert(
                connection,
                None,
                {"trade_id": 0, "trade_mode": normalized_trade_mode, "system_name": "Delphi"},
                snapshot_payload,
                now,
            )
            if outcome.get("sent"):
                self._stamp_trade_alert_timestamps(
                    connection,
                    [int(item.get("trade_id") or 0) for item in selected_records],
                    outcome.get("sent_at"),
                )
            connection.commit()

        return {
            "sent": bool(outcome.get("sent")),
            "error": str(outcome.get("error") or ""),
            "trade_mode": normalized_trade_mode,
            "record_count": len(selected_records),
            "priority": int(outcome.get("priority") or 0),
            "alert_type": str(outcome.get("alert_type") or f"manual-{normalized_trade_mode}-status-update"),
            "sent_at": outcome.get("sent_at"),
        }

    def _build_shared_context(self, open_trades: List[Dict[str, Any]], now: datetime) -> Dict[str, Any]:
        spx_snapshot = self._safe_snapshot("^GSPC", query_type="open_trade_management_spx")
        vix_snapshot = self._safe_snapshot("^VIX", query_type="open_trade_management_vix")
        expirations = {
            parsed_date
            for parsed_date in (self._coerce_trade_expiration(item) for item in open_trades)
            if parsed_date is not None
        }
        option_chains = {
            expiration_date.isoformat(): self.options_chain_service.get_spx_option_chain_summary(expiration_date)
            for expiration_date in sorted(expirations)
        }

        apollo_context: Dict[str, Any] = {}
        if any(str(item.get("system_name") or "").strip().lower() == "apollo" for item in open_trades):
            try:
                apollo_precheck = self.apollo_service.run_precheck()
            except Exception as exc:  # pragma: no cover - defensive provider handling
                LOGGER.warning("Apollo management context unavailable: %s", exc)
                apollo_precheck = {}
            apollo_context = {
                "current_structure_grade": self._coerce_text(((apollo_precheck.get("structure") or {}).get("final_grade") or (apollo_precheck.get("structure") or {}).get("grade")), fallback="Not available"),
                "current_macro_grade": self._coerce_text(((apollo_precheck.get("macro") or {}).get("grade")), fallback="Not available"),
                "precheck": apollo_precheck,
            }

        kairos_context: Dict[str, Any] = self._build_kairos_context(now)
        return {
            "now": now,
            "spx_snapshot": spx_snapshot,
            "vix_snapshot": vix_snapshot,
            "option_chains": option_chains,
            "apollo": apollo_context,
            "kairos": kairos_context,
            "current_spx": self._coerce_float((spx_snapshot or {}).get("Latest Value")),
            "current_vix": self._coerce_float((vix_snapshot or {}).get("Latest Value")),
        }

    def _build_kairos_context(self, now: datetime) -> Dict[str, Any]:
        dashboard = {}
        try:
            dashboard = self.kairos_service.get_dashboard_payload() if self.kairos_service is not None else {}
        except Exception as exc:  # pragma: no cover - defensive provider handling
            LOGGER.warning("Kairos management context unavailable: %s", exc)

        latest_scan = dashboard.get("latest_scan") or {}
        intraday = self._build_intraday_kairos_context(now)
        return {
            "dashboard": dashboard,
            "current_structure_status": self._coerce_text(latest_scan.get("structure_status") or intraday.get("structure_status"), fallback="Developing"),
            "current_momentum_status": self._coerce_text(latest_scan.get("momentum_status") or intraday.get("momentum_status"), fallback="Steady"),
            "current_vwap": intraday.get("current_vwap"),
            "current_session_posture": intraday.get("session_posture") or "Awaiting tape",
            "below_vwap": intraday.get("below_vwap", False),
        }

    def _build_intraday_kairos_context(self, now: datetime) -> Dict[str, Any]:
        try:
            frame = self.market_data_service.get_same_day_intraday_candles("^GSPC", interval_minutes=1, query_type="open_trade_management_kairos_intraday")
        except MarketDataError:
            return {}
        if frame is None or frame.empty:
            return {}

        working = frame.copy()
        working["Datetime"] = pd.to_datetime(working["Datetime"], errors="coerce")
        working = working.dropna(subset=["Datetime", "Open", "High", "Low", "Close"])
        if working.empty:
            return {}

        latest = working.iloc[-1]
        current_close = self._coerce_float(latest.get("Close"), fallback=0.0)
        session_open = self._coerce_float(working.iloc[0].get("Open"), fallback=current_close)
        volume_series = pd.to_numeric(working.get("Volume"), errors="coerce").fillna(0)
        typical_price = (pd.to_numeric(working["High"], errors="coerce") + pd.to_numeric(working["Low"], errors="coerce") + pd.to_numeric(working["Close"], errors="coerce")) / 3.0
        cumulative_volume = float(volume_series.sum())
        current_vwap = float(((typical_price * volume_series).sum() / cumulative_volume) if cumulative_volume > 0 else current_close)
        recent_close = self._coerce_float(working.iloc[max(0, len(working) - 6)].get("Close"), fallback=current_close)
        net_change_percent = self._percent_change(current_close, session_open)
        close_vs_vwap_percent = self._percent_change(current_close, current_vwap)
        recent_change_percent = self._percent_change(current_close, recent_close)
        session_posture = self._derive_session_posture(
            net_change_percent=net_change_percent,
            close_vs_vwap_percent=close_vs_vwap_percent,
            recent_change_percent=recent_change_percent,
        )
        if current_close < current_vwap and net_change_percent <= -0.18:
            structure_status = "Failed"
            momentum_status = "Expired"
        elif current_close < current_vwap:
            structure_status = "Weakening"
            momentum_status = "Weakening"
        elif current_close > current_vwap and net_change_percent >= 0.08:
            structure_status = "Bullish Confirmation"
            momentum_status = "Improving"
        else:
            structure_status = "Developing"
            momentum_status = "Steady"
        return {
            "current_vwap": round(current_vwap, 2),
            "below_vwap": bool(current_close < current_vwap),
            "session_posture": session_posture,
            "structure_status": structure_status,
            "momentum_status": momentum_status,
        }

    def _evaluate_trade(self, trade: Dict[str, Any], shared_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        system_name = normalize_system_name(trade.get("system_name"))
        trade_mode = str(trade.get("trade_mode") or "").strip().title() or "Unknown"
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        current_underlying = self._coerce_float(shared_context.get("current_spx"), fallback=self._coerce_float(trade.get("spx_at_entry"), fallback=0.0))
        current_vix = self._coerce_float(shared_context.get("current_vix"))
        metrics = self._build_live_metrics(trade, current_underlying=current_underlying, current_vix=current_vix, shared_context=shared_context, now=now)

        if system_name == "Kairos":
            classification = self._classify_kairos_trade(trade, metrics, shared_context.get("kairos") or {}, now)
            profile_label = self._coerce_text(trade.get("pass_type") or trade.get("candidate_profile"), fallback="Kairos")
        else:
            classification = self._classify_apollo_trade(trade, metrics, shared_context.get("apollo") or {}, now)
            profile_label = normalize_candidate_profile(trade.get("candidate_profile"))

        return {
            "trade_id": int(trade.get("id") or 0),
            "trade_number": int(trade.get("trade_number") or 0),
            "system_name": system_name,
            "trade_mode": trade_mode,
            "profile_label": profile_label,
            "pass_type": self._coerce_text(trade.get("pass_type"), fallback="—"),
            "entry_timestamp": self._format_datetime(parse_datetime_value(trade.get("entry_datetime")) or now),
            "expiration": self._format_date(self._coerce_trade_expiration(trade)),
            "underlying_symbol": self._coerce_text(trade.get("underlying_symbol"), fallback="SPX"),
            "option_type": option_type,
            "net_credit_per_contract": metrics["net_credit_per_contract"],
            "net_credit_per_contract_display": self._format_number(metrics["net_credit_per_contract"], decimals=4),
            "contracts": metrics["remaining_contracts"],
            "contracts_display": str(metrics["remaining_contracts"]),
            "total_premium": metrics["total_premium"],
            "total_premium_display": self._format_currency(metrics["total_premium"]),
            "max_loss": metrics["max_loss"],
            "max_loss_display": self._format_currency(metrics["max_loss"]),
            "current_underlying_price": metrics["current_underlying_price"],
            "current_underlying_price_display": self._format_number(metrics["current_underlying_price"]),
            "distance_to_short": metrics["distance_to_short"],
            "distance_to_short_display": self._format_distance_to_strike(metrics["distance_to_short"]),
            "distance_to_long": metrics["distance_to_long"],
            "distance_to_long_display": self._format_distance_to_strike(metrics["distance_to_long"]),
            "entry_expected_move": metrics["entry_expected_move"],
            "entry_expected_move_display": self._format_number(metrics["entry_expected_move"]),
            "entry_em_multiple": metrics["entry_em_multiple"],
            "entry_em_multiple_display": self._format_number(metrics["entry_em_multiple"]),
            "current_live_expected_move": metrics["current_live_expected_move"],
            "current_live_expected_move_display": self._format_number(metrics["current_live_expected_move"]),
            "current_live_expected_move_source_label": metrics["current_live_expected_move_source_label"],
            "current_live_expected_move_formula_label": metrics["current_live_expected_move_formula_label"],
            "current_live_expected_move_contracts_label": metrics["current_live_expected_move_contracts_label"],
            "current_em_multiple": classification["status_em_multiple"],
            "current_em_multiple_display": self._format_number(classification["status_em_multiple"]),
            "status_em_multiple": classification["status_em_multiple"],
            "status_em_multiple_display": self._format_number(classification["status_em_multiple"]),
            "current_vix": metrics["current_vix"],
            "current_vix_display": self._format_number(metrics["current_vix"]),
            "current_structure_grade": classification["current_structure_grade"],
            "time_remaining_to_expiration": metrics["time_remaining_to_expiration"],
            "time_remaining_to_expiration_display": metrics["time_remaining_display"],
            "current_spread_mark": metrics["current_spread_mark"],
            "current_spread_mark_display": self._format_number(metrics["current_spread_mark"], decimals=4),
            "current_total_close_cost": metrics["current_total_close_cost"],
            "current_total_close_cost_display": self._format_currency(metrics["current_total_close_cost"]),
            "current_close_price": metrics["current_close_price"],
            "current_close_price_display": self._format_number(metrics["current_close_price"], decimals=4),
            "unrealized_pnl": metrics["unrealized_pnl"],
            "unrealized_pnl_display": self._format_currency(metrics["unrealized_pnl"]),
            "percent_credit_captured": metrics["percent_credit_captured"],
            "percent_credit_captured_display": self._format_signed_percent(metrics["percent_credit_captured"]),
            "current_pl": metrics["current_pl"],
            "current_pl_display": self._format_currency(metrics["current_pl"]),
            "status": classification["status"],
            "status_severity": int(self.STATUS_SEVERITY.get(classification["status"], 0)),
            "status_key": self._slugify(classification["status"]),
            "thesis_status": classification["thesis_status"],
            "thesis_status_key": self._slugify(classification["thesis_status"]),
            "reason": classification["reason"],
            "reason_code": classification["status_reason_code"],
            "status_reason_code": classification["status_reason_code"],
            "thesis_reason_code": classification["thesis_reason_code"],
            "next_trigger": classification["next_trigger"],
            "trigger_source": classification["trigger_source"],
            "structure_trigger_fired": classification["structure_trigger_fired"],
            "vwap_trigger_fired": classification["vwap_trigger_fired"],
            "short_proximity_trigger_fired": classification["short_proximity_trigger_fired"],
            "long_proximity_trigger_fired": classification["long_proximity_trigger_fired"],
            "final_stop_trigger_fired": classification["final_stop_trigger_fired"],
            "invalid_strike_order": metrics["invalid_strike_order"],
            "distance_warning": metrics["distance_warning"],
            "action_recommendation": classification["action_recommendation"],
            "action_type": classification["action_type"],
            "contracts_to_close": classification["contracts_to_close"],
            "contracts_to_close_display": str(classification["contracts_to_close"]),
            "pl_after_close": classification["pl_after_close"],
            "pl_after_close_display": self._format_currency(classification["pl_after_close"]),
            "remaining_risk": classification["remaining_risk"],
            "remaining_risk_display": self._format_currency(classification["remaining_risk"]),
            "critical_alert": classification["critical_alert"],
            "send_close_to_journal_enabled": bool(metrics["current_spread_mark"] is not None and metrics["remaining_contracts"] > 0),
            "evaluated_at": now.isoformat(),
            "evaluated_at_display": self._format_datetime(now),
        }

    def _build_live_metrics(self, trade: Dict[str, Any], *, current_underlying: float, current_vix: float | None, shared_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        short_strike = self._coerce_float(trade.get("short_strike"), fallback=0.0)
        long_strike = self._coerce_float(trade.get("long_strike"), fallback=0.0)
        entry_underlying = self._coerce_float(trade.get("spx_at_entry") or trade.get("spx_entry"), fallback=0.0)
        remaining_contracts = int(trade.get("remaining_contracts") or trade.get("contracts") or 0)
        credit_model = resolve_trade_credit_model(trade)
        net_credit_per_contract = self._coerce_float(credit_model.get("net_credit_per_contract"), fallback=0.0)
        total_premium = self._coerce_float(credit_model.get("total_premium"))
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        is_call = "call" in option_type.lower()
        distance_snapshot = self._build_distance_snapshot(
            current_price=current_underlying,
            short_strike=short_strike,
            long_strike=long_strike,
            option_type=option_type,
        )
        original_buffer = self._resolve_original_buffer(trade, entry_underlying=entry_underlying, short_strike=short_strike, is_call=is_call)
        distance_to_short = distance_snapshot["distance_to_short"]
        distance_to_long = distance_snapshot["distance_to_long"]
        current_spread_mark = self._resolve_current_spread_mark(trade, shared_context)
        current_total_close_cost = None
        unrealized_pnl = None
        if current_spread_mark is not None and remaining_contracts > 0:
            current_total_close_cost = round(current_spread_mark * 100.0 * remaining_contracts, 2)
        if total_premium is not None and current_total_close_cost is not None:
            unrealized_pnl = round(total_premium - current_total_close_cost, 2)
        current_close_price = current_spread_mark if current_spread_mark is not None else net_credit_per_contract
        percent_credit_captured = None
        current_pl = None
        if net_credit_per_contract not in {None, 0}:
            percent_credit_captured = round(((net_credit_per_contract - current_close_price) / net_credit_per_contract) * 100.0, 2)
        if current_close_price is not None and remaining_contracts > 0:
            current_pl = round((net_credit_per_contract - current_close_price) * remaining_contracts * 100.0, 2)

        expiration_date = self._coerce_trade_expiration(trade)
        chain_summary = (shared_context.get("option_chains") or {}).get(expiration_date.isoformat()) if expiration_date is not None else None
        current_live_expected_move_details = self._estimate_live_expected_move_from_chain(
            chain_summary=chain_summary,
            spot=current_underlying,
            system_name=normalize_system_name(trade.get("system_name")),
            expiration_date=expiration_date,
            evaluation_date=now.date(),
        )
        entry_expected_move = self._coerce_float(trade.get("expected_move_used") or trade.get("expected_move"))
        current_live_expected_move = self._coerce_float(current_live_expected_move_details.get("expected_move"))
        if current_live_expected_move in {None, 0.0}:
            current_live_expected_move = entry_expected_move
            current_live_expected_move_details = {
                "expected_move": current_live_expected_move,
                "source_label": "Entry expected move fallback",
                "formula_label": "Fallback to the stored trade entry expected move because the live chain EM was unavailable",
                "contracts_label": "—",
            }
        entry_distance_to_short = self._coerce_float(trade.get("actual_distance_to_short"))
        if entry_distance_to_short is None:
            entry_distance_to_short = original_buffer
        entry_em_multiple = self._safe_divide(entry_distance_to_short, entry_expected_move)
        time_remaining = self._build_time_remaining(expiration_date, now)
        return {
            "entry_underlying": entry_underlying,
            "net_credit_per_contract": net_credit_per_contract,
            "total_premium": total_premium,
            "max_loss": self._coerce_float(credit_model.get("max_theoretical_risk")),
            "current_underlying_price": current_underlying,
            "distance_to_short": distance_to_short,
            "distance_to_long": distance_to_long,
            "entry_expected_move": entry_expected_move,
            "entry_em_multiple": entry_em_multiple,
            "current_live_expected_move": current_live_expected_move,
            "current_live_expected_move_source_label": self._coerce_text(current_live_expected_move_details.get("source_label"), fallback="Unavailable"),
            "current_live_expected_move_formula_label": self._coerce_text(current_live_expected_move_details.get("formula_label"), fallback="Unavailable"),
            "current_live_expected_move_contracts_label": self._coerce_text(current_live_expected_move_details.get("contracts_label"), fallback="—"),
            "current_vix": current_vix,
            "current_spread_mark": current_spread_mark,
            "current_total_close_cost": current_total_close_cost,
            "current_close_price": current_close_price,
            "unrealized_pnl": unrealized_pnl,
            "percent_credit_captured": percent_credit_captured,
            "current_pl": current_pl,
            "remaining_contracts": remaining_contracts,
            "time_remaining_to_expiration": time_remaining["seconds"],
            "time_remaining_display": time_remaining["display"],
            "entry_structure_grade": self._coerce_text(trade.get("structure_grade"), fallback="Not available"),
            "entry_macro_grade": self._coerce_text(trade.get("macro_grade"), fallback="Not available"),
            "entry_vix": self._coerce_float(trade.get("vix_at_entry") or trade.get("vix_entry")),
            "expected_move_used": entry_expected_move,
            "actual_em_multiple": self._coerce_float(trade.get("actual_em_multiple")),
            "fallback_used": self._coerce_text(trade.get("fallback_used"), fallback="no"),
            "fallback_rule_name": self._coerce_text(trade.get("fallback_rule_name"), fallback="—"),
            "notes_entry": self._coerce_text(trade.get("notes_entry"), fallback="—"),
            "original_buffer": original_buffer,
            "invalid_strike_order": distance_snapshot["invalid_strike_order"],
            "distance_warning": distance_snapshot["distance_warning"],
        }

    def _classify_apollo_trade(self, trade: Dict[str, Any], metrics: Dict[str, Any], apollo_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        distance_to_short = float(metrics.get("distance_to_short") or 0.0)
        distance_to_long = float(metrics.get("distance_to_long") or 0.0)
        short_breach = self._is_strike_breached(distance_to_short)
        long_breach = self._is_long_stop_breached(distance_to_short=distance_to_short, distance_to_long=distance_to_long)
        current_structure = self._coerce_text(apollo_context.get("current_structure_grade"), fallback="Not used for status")
        current_live_expected_move = self._coerce_float(metrics.get("current_live_expected_move"))
        em_multiple = self._safe_divide(distance_to_short, current_live_expected_move)

        if long_breach or short_breach or em_multiple < 1.0:
            status = "Exit Now"
            status_reason_code = "apollo-em-exit-now"
            reason = "Apollo spread has breached its short or long stop and should be fully exited."
        elif em_multiple < 1.2:
            status = "Exit Partial"
            status_reason_code = "apollo-em-exit-partial"
            reason = f"Apollo spread is down to {em_multiple:.2f}x current live expected-move clearance and needs a partial exit."
        elif em_multiple >= self.APOLLO_HEALTHY_EM_THRESHOLD:
            status = "Healthy"
            status_reason_code = "apollo-em-healthy"
            reason = f"Apollo spread retains {em_multiple:.2f}x current live expected-move clearance to the short strike."
        elif em_multiple >= self.APOLLO_WATCH_EM_THRESHOLD:
            status = "Watch"
            status_reason_code = "apollo-em-watch"
            reason = f"Apollo spread has compressed to {em_multiple:.2f}x current live expected-move clearance and should be watched."
        else:
            status = "Exit Approaching"
            status_reason_code = "apollo-em-exit-approaching"
            if current_live_expected_move is None or current_live_expected_move <= 0:
                reason = "Apollo has exhausted its current live expected-move cushion and is approaching exit territory."
            else:
                reason = f"Apollo spread is down to {em_multiple:.2f}x current live expected-move clearance and is approaching exit territory."

        action_plan = self._build_action_plan(status, int(metrics.get("remaining_contracts") or 0), metrics)
        return {
            "status": status,
            "thesis_status": "neutral",
            "reason": reason,
            "status_reason_code": status_reason_code,
            "thesis_reason_code": "apollo-thesis-neutral",
            "next_trigger": self._build_apollo_next_trigger(status),
            "trigger_source": "current-live-expected-move",
            "current_structure_grade": current_structure,
            "status_em_multiple": em_multiple,
            "structure_trigger_fired": False,
            "vwap_trigger_fired": False,
            "short_proximity_trigger_fired": bool(status != "Healthy" or short_breach),
            "long_proximity_trigger_fired": long_breach,
            "final_stop_trigger_fired": bool(short_breach or long_breach),
            **action_plan,
        }

    def _classify_kairos_trade(self, trade: Dict[str, Any], metrics: Dict[str, Any], kairos_context: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        distance_to_short = float(metrics.get("distance_to_short") or 0.0)
        distance_to_long = float(metrics.get("distance_to_long") or 0.0)
        current_structure = self._coerce_text(kairos_context.get("current_structure_status"), fallback="Developing")
        below_vwap = bool(kairos_context.get("below_vwap"))
        current_live_expected_move = self._coerce_float(metrics.get("current_live_expected_move"))
        em_multiple = self._safe_divide(distance_to_short, current_live_expected_move)
        short_watch_zone = em_multiple < self.KAIROS_HEALTHY_EM_THRESHOLD
        short_trim_zone = em_multiple < self.KAIROS_WATCH_EM_THRESHOLD
        short_hard_stop = self._is_strike_breached(distance_to_short)
        long_breach = self._is_long_stop_breached(distance_to_short=distance_to_short, distance_to_long=distance_to_long)
        thesis_status, thesis_reason_code = self._derive_kairos_thesis_status(current_structure, below_vwap)
        structure_trigger_fired = thesis_status in {"bearish", "broken"}
        vwap_trigger_fired = below_vwap
        if long_breach or short_hard_stop or em_multiple < 1.0:
            status = "Exit Now"
            status_reason_code = "kairos-em-exit-now"
            reason = "Kairos spread has breached its short or long stop and should be fully exited."
        elif em_multiple < 1.2:
            status = "Exit Partial"
            status_reason_code = "kairos-em-exit-partial"
            reason = f"Kairos spread is down to {em_multiple:.2f}x current same-day expected-move clearance and needs a partial exit."
        elif em_multiple >= self.KAIROS_HEALTHY_EM_THRESHOLD:
            status = "Healthy"
            status_reason_code = "kairos-em-healthy"
            reason = f"Kairos spread retains {em_multiple:.2f}x current same-day expected-move clearance to the short strike."
        elif em_multiple >= self.KAIROS_WATCH_EM_THRESHOLD:
            status = "Watch"
            status_reason_code = "kairos-em-watch"
            reason = f"Kairos spread has compressed to {em_multiple:.2f}x current same-day expected-move clearance and should be watched."
        else:
            status = "Exit Approaching"
            status_reason_code = "kairos-em-exit-approaching"
            reason = f"Kairos spread is down to {em_multiple:.2f}x current same-day expected-move clearance and is approaching exit territory."

        action_plan = self._build_action_plan(status, int(metrics.get("remaining_contracts") or 0), metrics)
        return {
            "status": status,
            "thesis_status": thesis_status,
            "reason": reason,
            "status_reason_code": status_reason_code,
            "thesis_reason_code": thesis_reason_code,
            "next_trigger": self._build_kairos_next_trigger(status),
            "trigger_source": "current-live-expected-move",
            "current_structure_grade": current_structure,
            "status_em_multiple": em_multiple,
            "structure_trigger_fired": structure_trigger_fired,
            "vwap_trigger_fired": vwap_trigger_fired,
            "short_proximity_trigger_fired": short_watch_zone,
            "long_proximity_trigger_fired": long_breach,
            "final_stop_trigger_fired": bool(short_hard_stop or long_breach),
            **action_plan,
        }

    def _process_daily_notifications(
        self,
        connection: sqlite3.Connection,
        records: List[Dict[str, Any]],
        now: datetime,
        runtime_settings: Dict[str, Any],
    ) -> list[Dict[str, Any]]:
        outcomes: list[Dict[str, Any]] = []
        morning_outcome = self._send_morning_snapshot_if_due(connection, records, now, runtime_settings)
        if morning_outcome is not None:
            outcomes.append(morning_outcome)
        eod_outcome = self._send_eod_summary_if_due(connection, records, now, runtime_settings)
        if eod_outcome is not None:
            outcomes.append(eod_outcome)
        return outcomes

    def _process_trade_notifications(
        self,
        connection: sqlite3.Connection,
        trade: Dict[str, Any],
        record: Dict[str, Any],
        previous_state: Dict[str, Any],
        now: datetime,
    ) -> Dict[str, Any]:
        sent_count = 0
        last_priority = None
        last_alert_type = None
        sent_at = None

        status_change = self._build_status_change_alert(trade, record)
        if status_change is not None:
            outcome = self._send_management_alert(connection, trade, record, status_change, now)
            if not outcome.get("sent") and outcome.get("error"):
                return outcome
            if outcome.get("sent"):
                sent_count += 1
                last_priority = outcome.get("priority")
                last_alert_type = outcome.get("alert_type")
                sent_at = outcome.get("sent_at")

        action_alert = self._build_action_alert(trade, record)
        if action_alert is not None:
            outcome = self._send_management_alert(connection, trade, record, action_alert, now)
            if not outcome.get("sent") and outcome.get("error"):
                return outcome
            if outcome.get("sent"):
                sent_count += 1
                last_priority = outcome.get("priority")
                last_alert_type = outcome.get("alert_type")
                sent_at = outcome.get("sent_at")

        self._update_trade_notification_state(
            connection,
            trade_id=int(trade.get("id") or 0),
            last_status=str(record.get("status") or ""),
            last_action_sent=str(record.get("action_type") or ""),
            last_alert_timestamp=sent_at,
        )
        return {
            "sent": sent_count > 0,
            "sent_count": sent_count,
            "error": "",
            "priority": last_priority,
            "alert_type": last_alert_type,
            "sent_at": sent_at,
        }

    def _upsert_management_state(
        self,
        connection: sqlite3.Connection,
        record: Dict[str, Any],
        previous_state: Dict[str, Any],
        alert_outcome: Dict[str, Any],
        now: datetime,
    ) -> None:
        existing_alert_count = int(previous_state.get("alert_count") or 0)
        connection.execute(
            """
            INSERT INTO open_trade_management_state (
                trade_id, system_name, trade_mode, current_management_status, previous_management_status,
                current_thesis_status, previous_thesis_status, concise_reason, reason_code, next_trigger,
                trigger_source, last_evaluated_at, last_alert_sent_at, last_alert_type, last_alert_priority,
                alert_count, last_alert_reason_code, last_underlying_price, last_distance_to_short,
                last_distance_to_long, last_buffer_remaining_percent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
                system_name = excluded.system_name,
                trade_mode = excluded.trade_mode,
                previous_management_status = open_trade_management_state.current_management_status,
                current_management_status = excluded.current_management_status,
                previous_thesis_status = open_trade_management_state.current_thesis_status,
                current_thesis_status = excluded.current_thesis_status,
                concise_reason = excluded.concise_reason,
                reason_code = excluded.reason_code,
                next_trigger = excluded.next_trigger,
                trigger_source = excluded.trigger_source,
                last_evaluated_at = excluded.last_evaluated_at,
                last_alert_sent_at = COALESCE(excluded.last_alert_sent_at, open_trade_management_state.last_alert_sent_at),
                last_alert_type = COALESCE(excluded.last_alert_type, open_trade_management_state.last_alert_type),
                last_alert_priority = COALESCE(excluded.last_alert_priority, open_trade_management_state.last_alert_priority),
                alert_count = excluded.alert_count,
                last_alert_reason_code = COALESCE(excluded.last_alert_reason_code, open_trade_management_state.last_alert_reason_code),
                last_underlying_price = excluded.last_underlying_price,
                last_distance_to_short = excluded.last_distance_to_short,
                last_distance_to_long = excluded.last_distance_to_long,
                last_buffer_remaining_percent = excluded.last_buffer_remaining_percent
            """,
            (
                record["trade_id"],
                record["system_name"],
                str(record["trade_mode"]).lower(),
                record["status"],
                previous_state.get("current_management_status"),
                record["thesis_status"],
                previous_state.get("current_thesis_status"),
                record["reason"],
                record["reason_code"],
                record["next_trigger"],
                record["trigger_source"],
                now.isoformat(),
                alert_outcome.get("sent_at"),
                alert_outcome.get("alert_type"),
                alert_outcome.get("priority"),
                existing_alert_count + int(alert_outcome.get("sent_count") or (1 if alert_outcome.get("sent") else 0)),
                record["reason_code"] if alert_outcome.get("sent") else previous_state.get("last_alert_reason_code"),
                record.get("current_underlying_price"),
                record.get("distance_to_short"),
                record.get("distance_to_long"),
                record.get("percent_buffer_remaining"),
            ),
        )

    def _load_open_trades(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated"):
            rows.extend(
                trade
                for trade in self.trade_store.list_trades(trade_mode)
                if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
            )
        return rows

    def _load_runtime_settings(self) -> Dict[str, Any]:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM open_trade_management_runtime_settings WHERE singleton_id = 1"
            ).fetchone()
            return dict(row) if row else {"notifications_enabled": True}

    def _load_trade(self, connection: sqlite3.Connection, trade_id: int) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
        return dict(row) if row else {}

    def _load_management_states(self) -> Dict[int, Dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM open_trade_management_state").fetchall()
            return {int(row["trade_id"]): dict(row) for row in rows}

    def _load_management_state(self, connection: sqlite3.Connection, trade_id: int) -> Dict[str, Any]:
        row = connection.execute("SELECT * FROM open_trade_management_state WHERE trade_id = ?", (trade_id,)).fetchone()
        return dict(row) if row else {}

    def _build_alert_state_payload(self, state: Dict[str, Any], trade: Dict[str, Any]) -> Dict[str, Any]:
        last_alert_sent_at = parse_datetime_value(state.get("last_alert_sent_at")) if state else None
        last_alert_priority = state.get("last_alert_priority") if state else None
        return {
            "last_alert_sent_at": self._format_datetime(last_alert_sent_at),
            "last_alert_type": self._coerce_text((state or {}).get("last_alert_type"), fallback="—"),
            "last_alert_priority": "High" if last_alert_priority == 1 else ("Normal" if last_alert_priority == 0 else "—"),
            "alert_count": int((state or {}).get("alert_count") or 0),
            "last_status": self._coerce_text((trade or {}).get("last_status"), fallback="—"),
            "last_action_sent": self._coerce_text((trade or {}).get("last_action_sent"), fallback="—"),
            "last_trade_alert_timestamp": self._format_datetime(parse_datetime_value((trade or {}).get("last_alert_timestamp"))),
        }

    def _build_status_counts(self, records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        counts: Dict[str, int] = {label: 0 for label in ("Healthy", "Watch", "Exit Approaching", "Exit Partial", "Exit Now")}
        for record in records:
            label = str(record.get("status") or "Healthy")
            if label in counts:
                counts[label] += 1
        return [{"label": label, "count": counts[label], "key": self._slugify(label)} for label in counts]

    def _build_apollo_next_trigger(self, status: str) -> str:
        if status == "Healthy":
            return "Watch below 2.00x current live EM; prep if short distance compresses or long strike nears."
        if status == "Watch":
            return "Exit Approaching below 1.50x current live EM; monitor short distance and long-strike proximity."
        if status == "Exit Partial":
            return "Full exit if live EM falls further, the short strike breaches, or the long strike comes into play."
        return "Next trigger is short-strike compression, long-strike proximity, or live EM exhaustion."

    def _build_kairos_next_trigger(self, status: str) -> str:
        if status == "Healthy":
            return "Watch below 3.00x current live EM; also monitor distance-to-short and structure degradation."
        if status == "Watch":
            return "Exit Approaching below 1.50x current live EM; also monitor long-strike proximity and structure breaks."
        if status == "Exit Partial":
            return "Full exit if same-day EM compresses further, the short strike breaches, or the long strike is threatened."
        return "Next trigger is long-strike proximity, structure break, or further live EM compression."

    @staticmethod
    def _derive_kairos_thesis_status(current_structure: str, below_vwap: bool) -> tuple[str, str]:
        normalized = str(current_structure or "").strip().lower()
        if normalized == "failed":
            return "broken", "kairos-structure-broken"
        if normalized in {"weakening", "expired"} or below_vwap:
            return "bearish", "kairos-structure-bearish"
        if "bullish" in normalized or normalized == "improving":
            return "bullish", "kairos-structure-bullish"
        return "chop", "kairos-structure-chop"

    def _resolve_current_spread_mark(self, trade: Dict[str, Any], shared_context: Dict[str, Any]) -> float | None:
        expiration = self._coerce_trade_expiration(trade)
        if expiration is None:
            return None
        chain_summary = (shared_context.get("option_chains") or {}).get(expiration.isoformat()) or {}
        if not chain_summary.get("success"):
            return None
        option_type = self._coerce_text(trade.get("option_type"), fallback="Put Credit Spread")
        contracts = chain_summary.get("calls") if "call" in option_type.lower() else chain_summary.get("puts")
        if not isinstance(contracts, list):
            return None
        strike_lookup = {round(float(item.get("strike") or 0.0), 2): item for item in contracts if self._coerce_float(item.get("strike")) is not None}
        short_leg = strike_lookup.get(round(float(trade.get("short_strike") or 0.0), 2))
        long_leg = strike_lookup.get(round(float(trade.get("long_strike") or 0.0), 2))
        if not short_leg or not long_leg:
            return None
        short_mark = self._option_mark(short_leg)
        long_mark = self._option_mark(long_leg)
        if short_mark is None or long_mark is None:
            return None
        return round(max(short_mark - long_mark, 0.0), 2)

    def _estimate_live_expected_move_from_chain(
        self,
        *,
        chain_summary: Dict[str, Any] | None,
        spot: float,
        system_name: str,
        expiration_date: date | None,
        evaluation_date: date,
    ) -> Dict[str, Any]:
        if not chain_summary or not chain_summary.get("success"):
            return {
                "expected_move": None,
                "source_label": "Unavailable",
                "formula_label": "Live option-chain expected move unavailable",
                "contracts_label": "—",
            }

        normalized_puts = self._normalize_live_contracts(chain_summary.get("puts") or [])
        normalized_calls = self._normalize_live_contracts(chain_summary.get("calls") or [])
        selected_put = self._find_live_contract_at_or_below_strike(normalized_puts, spot) or self._find_nearest_live_contract(normalized_puts, spot)
        selected_call = self._find_live_contract_at_or_above_strike(normalized_calls, spot) or self._find_nearest_live_contract(normalized_calls, spot)
        put_anchor = self._option_mark(selected_put or {}) if selected_put is not None else 0.0
        call_anchor = self._option_mark(selected_call or {}) if selected_call is not None else 0.0

        expected_move = 0.0
        if put_anchor and call_anchor:
            expected_move = put_anchor + call_anchor
            contracts_label = f"{int(selected_put['strike'])}P {put_anchor:.2f} + {int(selected_call['strike'])}C {call_anchor:.2f}"
            formula_label = "ATM put premium + ATM call premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        elif put_anchor:
            expected_move = put_anchor * 2.0
            contracts_label = f"2 × {int(selected_put['strike'])}P {put_anchor:.2f}"
            formula_label = "2 × ATM put premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        elif call_anchor:
            expected_move = call_anchor * 2.0
            contracts_label = f"2 × {int(selected_call['strike'])}C {call_anchor:.2f}"
            formula_label = "2 × ATM call premium using midpoint when bid/ask are both available, else mark/bid/ask/last"
        else:
            contracts_label = "—"
            formula_label = "Live option-chain expected move unavailable"

        source_label = "Current trade-horizon ATM straddle"
        if expiration_date is not None and expiration_date == evaluation_date:
            source_label = "Same-day ATM straddle"
        if system_name == "Kairos":
            source_label = "Current same-day ATM straddle"

        return {
            "expected_move": round(expected_move, 2) if expected_move > 0 else None,
            "source_label": source_label if expected_move > 0 else "Unavailable",
            "formula_label": formula_label,
            "contracts_label": contracts_label,
        }

    def _normalize_live_contracts(self, contracts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for item in contracts:
            strike = self._coerce_float(item.get("strike"))
            if strike is None:
                continue
            normalized.append({**item, "strike": strike})
        return sorted(normalized, key=lambda item: item["strike"])

    @staticmethod
    def _find_live_contract_at_or_below_strike(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        eligible = [item for item in contracts if float(item.get("strike") or 0.0) <= strike]
        return eligible[-1] if eligible else None

    @staticmethod
    def _find_live_contract_at_or_above_strike(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        eligible = [item for item in contracts if float(item.get("strike") or 0.0) >= strike]
        return eligible[0] if eligible else None

    @staticmethod
    def _find_nearest_live_contract(contracts: List[Dict[str, Any]], strike: float) -> Dict[str, Any] | None:
        if not contracts:
            return None
        return min(contracts, key=lambda item: abs(float(item.get("strike") or 0.0) - strike))

    @staticmethod
    def _option_mark(contract: Dict[str, Any]) -> float | None:
        mark = to_float(contract.get("mark"))
        if mark is not None:
            return mark
        bid = to_float(contract.get("bid"))
        ask = to_float(contract.get("ask"))
        if bid is not None and ask is not None:
            return round((bid + ask) / 2.0, 2)
        return to_float(contract.get("last"))

    @staticmethod
    def _resolve_original_buffer(trade: Dict[str, Any], *, entry_underlying: float, short_strike: float, is_call: bool) -> float | None:
        stored = to_float(trade.get("actual_distance_to_short") or trade.get("distance_to_short"))
        if stored not in {None, 0}:
            return abs(float(stored))
        if entry_underlying in {None, 0} or short_strike in {None, 0}:
            return None
        return abs((short_strike - entry_underlying) if is_call else (entry_underlying - short_strike))

    @staticmethod
    def _compute_distance(current_price: float, *, strike: float, is_call: bool) -> float:
        if is_call:
            return round(strike - current_price, 2)
        return round(current_price - strike, 2)

    def _build_distance_snapshot(self, *, current_price: float, short_strike: float, long_strike: float, option_type: str) -> Dict[str, Any]:
        is_call = "call" in option_type.lower()
        distance_to_short = self._compute_distance(current_price, strike=short_strike, is_call=is_call)
        distance_to_long = self._compute_distance(current_price, strike=long_strike, is_call=is_call)
        invalid_strike_order = False
        distance_warning = ""
        if short_strike and long_strike:
            if is_call:
                invalid_strike_order = long_strike <= short_strike
                if invalid_strike_order:
                    distance_warning = "Stored call-spread strikes are inverted; alerts ignore long-stop breaches unless the short strike is also breached."
            else:
                invalid_strike_order = long_strike >= short_strike
                if invalid_strike_order:
                    distance_warning = "Stored put-spread strikes are inverted; alerts ignore long-stop breaches unless the short strike is also breached."
        return {
            "distance_to_short": distance_to_short,
            "distance_to_long": distance_to_long,
            "invalid_strike_order": invalid_strike_order,
            "distance_warning": distance_warning,
        }

    @staticmethod
    def _is_strike_breached(distance_to_strike: float) -> bool:
        return distance_to_strike <= 0

    def _is_long_stop_breached(self, *, distance_to_short: float, distance_to_long: float) -> bool:
        return self._is_strike_breached(distance_to_long) and self._is_strike_breached(distance_to_short)

    @staticmethod
    def _build_time_remaining(expiration_date: date | None, now: datetime) -> Dict[str, Any]:
        if expiration_date is None:
            return {"seconds": None, "display": "—"}
        expiration_dt = datetime.combine(expiration_date, time(15, 0), tzinfo=now.tzinfo)
        delta = expiration_dt - now
        seconds = int(delta.total_seconds())
        if seconds <= 0:
            return {"seconds": 0, "display": "Expired"}
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes = remainder // 60
        return {"seconds": seconds, "display": f"{days}d {hours}h {minutes}m"}

    def _build_action_plan(self, status: str, contracts: int, metrics: Dict[str, Any]) -> Dict[str, Any]:
        contracts = max(int(contracts or 0), 0)
        current_close_price = self._coerce_float(metrics.get("current_close_price"), fallback=0.0) or 0.0
        entry_credit = self._coerce_float(metrics.get("net_credit_per_contract"), fallback=0.0) or 0.0
        max_loss = self._coerce_float(metrics.get("max_loss"), fallback=0.0) or 0.0
        action_type = ""
        contracts_to_close = 0
        if status == "Exit Approaching" and contracts > 0:
            action_type = "Reduce"
            contracts_to_close = 1 if contracts == 1 else max(1, contracts // 2)
        elif status == "Exit Partial" and contracts > 0:
            action_type = "Partial Exit"
            contracts_to_close = max(1, (contracts + 1) // 2)
        elif status == "Exit Now" and contracts > 0:
            action_type = "Full Exit"
            contracts_to_close = contracts

        remaining_contracts = max(contracts - contracts_to_close, 0)
        closed_pl = round((entry_credit - current_close_price) * contracts_to_close * 100.0, 2)
        remaining_unrealized_pl = round((entry_credit - current_close_price) * remaining_contracts * 100.0, 2)
        pl_after_close = round(closed_pl + remaining_unrealized_pl, 2)
        remaining_risk = round(max(max_loss - closed_pl, 0.0), 2)
        action_recommendation = action_type or "Watch"
        critical_alert = bool((self._coerce_float(metrics.get("distance_to_short"), fallback=999.0) or 999.0) < 25.0 or (self._coerce_float(metrics.get("current_live_expected_move")) and self._safe_divide(metrics.get("distance_to_short"), metrics.get("current_live_expected_move")) < 1.2))
        return {
            "action_recommendation": action_recommendation,
            "action_type": action_type,
            "contracts_to_close": contracts_to_close,
            "pl_after_close": pl_after_close,
            "remaining_risk": remaining_risk,
            "critical_alert": critical_alert,
        }

    def _build_status_change_alert(self, trade: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any] | None:
        previous_status = str(trade.get("last_status") or "").strip()
        current_status = str(record.get("status") or "").strip()
        if not previous_status or previous_status == current_status:
            return None
        title = "⚠️ STATUS CHANGE"
        message = "\n".join(
            [
                f"{record['system_name']} | {record['profile_label']}",
                f"{previous_status} → {current_status}",
                "",
                f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}",
                f"Credit: {record['percent_credit_captured_display']}",
                f"Next Trigger: {record['next_trigger']}",
            ]
        )
        return {
            "title": title,
            "message": message,
            "priority": 1 if record.get("critical_alert") else 0,
            "alert_type": "status-change",
            "reason_code": record.get("reason_code"),
            "critical": bool(record.get("critical_alert")),
        }

    def _build_action_alert(self, trade: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any] | None:
        action_type = str(record.get("action_type") or "").strip()
        if not action_type:
            return None
        previous_action = str(trade.get("last_action_sent") or "").strip()
        if previous_action == action_type:
            return None
        title = "🚨 ACTION REQUIRED"
        message = "\n".join(
            [
                f"{record['system_name']} | {record['profile_label']}",
                f"Status: {record['status']}",
                "",
                f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}",
                "",
                f"Action: Close {record['contracts_to_close_display']} contracts",
                f"P/L Now: {record['current_pl_display']}",
                f"After Close: {record['pl_after_close_display']}",
                "",
                f"Remaining Risk: {record['remaining_risk_display']}",
                "",
                f"Reason: {record['reason']}",
                f"Next Trigger: {record['next_trigger']}",
            ]
        )
        return {
            "title": title,
            "message": message,
            "priority": 1 if record.get("critical_alert") else 0,
            "alert_type": "action-required",
            "reason_code": record.get("reason_code"),
            "critical": bool(record.get("critical_alert")),
        }

    def _send_morning_snapshot_if_due(
        self,
        connection: sqlite3.Connection,
        records: List[Dict[str, Any]],
        now: datetime,
        runtime_settings: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        if not self._is_morning_snapshot_due(now, runtime_settings):
            return None
        if not records:
            self._mark_runtime_setting(connection, "last_morning_snapshot_date", now.date().isoformat())
            return {"sent": False, "error": "", "priority": 0, "alert_type": "morning-snapshot", "sent_at": None}
        payload = self._build_open_positions_snapshot_payload(
            records,
            alert_type="morning-snapshot",
            reason_code="morning-snapshot",
        )
        outcome = self._send_management_alert(connection, None, {"trade_id": 0, "trade_mode": "real", "system_name": "Delphi"}, payload, now)
        if outcome.get("sent"):
            self._mark_runtime_setting(connection, "last_morning_snapshot_date", now.date().isoformat())
            self._stamp_trade_alert_timestamps(connection, [int(item.get("trade_id") or 0) for item in records], outcome.get("sent_at"))
        return outcome

    def _build_open_positions_snapshot_payload(
        self,
        records: List[Dict[str, Any]],
        *,
        alert_type: str,
        reason_code: str,
    ) -> Dict[str, Any]:
        body_blocks = []
        critical = False
        for record in records:
            critical = critical or bool(record.get("critical_alert"))
            body_blocks.append(
                "\n".join(
                    [
                        f"{record['system_name']} | {record['profile_label']} | {record['status']}",
                        f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}",
                        f"{record['percent_credit_captured_display']} credit",
                    ]
                )
            )
        return {
            "title": "DELPHI — OPEN POSITIONS",
            "message": "\n\n".join(body_blocks),
            "priority": 1 if critical else 0,
            "alert_type": alert_type,
            "reason_code": reason_code,
            "critical": critical,
        }

    def _send_eod_summary_if_due(
        self,
        connection: sqlite3.Connection,
        open_records: List[Dict[str, Any]],
        now: datetime,
        runtime_settings: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        if not self._is_eod_summary_due(now, runtime_settings):
            return None
        closed_today = self._load_closed_real_trades_for_date(now.date())
        if not closed_today and not open_records:
            self._mark_runtime_setting(connection, "last_eod_summary_date", now.date().isoformat())
            return {"sent": False, "error": "", "priority": 0, "alert_type": "eod-summary", "sent_at": None}
        lines = ["Closed Today:"]
        for trade in closed_today:
            lines.append(f"{trade['system_name']} #{trade['trade_number']} → {self._format_currency(trade.get('gross_pnl'))} ({trade.get('win_loss_result') or 'Closed'})")
        if not closed_today:
            lines.append("None")
        lines.append("")
        lines.append("Open (Next Day Risk):")
        critical = False
        for record in open_records:
            critical = critical or bool(record.get("critical_alert"))
            lines.append(f"{record['system_name']} | {record['profile_label']}")
            lines.append(f"Dist: {self._format_points(record.get('distance_to_short'))} | EM: {record['current_live_expected_move_display']}")
        if not open_records:
            lines.append("None")
        payload = {
            "title": "DELPHI — EOD SUMMARY",
            "message": "\n".join(lines),
            "priority": 1 if critical else 0,
            "alert_type": "eod-summary",
            "reason_code": "eod-summary",
            "critical": critical,
        }
        outcome = self._send_management_alert(connection, None, {"trade_id": 0, "trade_mode": "real", "system_name": "Delphi"}, payload, now)
        if outcome.get("sent"):
            self._mark_runtime_setting(connection, "last_eod_summary_date", now.date().isoformat())
            self._stamp_trade_alert_timestamps(connection, [int(item.get("trade_id") or 0) for item in open_records], outcome.get("sent_at"))
        return outcome

    def _send_management_alert(
        self,
        connection: sqlite3.Connection,
        trade: Dict[str, Any] | None,
        record: Dict[str, Any],
        payload: Dict[str, Any],
        now: datetime,
    ) -> Dict[str, Any]:
        title = str(payload.get("title") or "").strip()
        message = str(payload.get("message") or "").strip()
        priority = int(payload.get("priority") or 0)
        if payload.get("critical"):
            title = f"🔴 CRITICAL {title}"
        result = self.pushover_service.send_notification(title=title, message=message, priority=priority)
        if not result.get("ok"):
            return {
                "sent": False,
                "error": str(result.get("error") or "Unable to send Pushover alert."),
                "priority": priority,
                "alert_type": payload.get("alert_type"),
                "sent_at": None,
            }
        sent_at = now.isoformat()
        connection.execute(
            """
            INSERT INTO open_trade_management_alert_log (
                trade_id, system_name, trade_mode, alert_type, alert_priority, alert_priority_label,
                reason_code, title, body, sent_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int((trade or {}).get("id") or record.get("trade_id") or 0),
                str(record.get("system_name") or (trade or {}).get("system_name") or "Delphi"),
                str(record.get("trade_mode") or (trade or {}).get("trade_mode") or "real").lower(),
                str(payload.get("alert_type") or "management-alert"),
                priority,
                "High" if priority == 1 else "Normal",
                payload.get("reason_code"),
                title,
                message,
                sent_at,
            ),
        )
        return {
            "sent": True,
            "error": "",
            "priority": priority,
            "alert_type": payload.get("alert_type"),
            "sent_at": sent_at,
        }

    def _update_trade_notification_state(
        self,
        connection: sqlite3.Connection,
        *,
        trade_id: int,
        last_status: str,
        last_action_sent: str,
        last_alert_timestamp: str | None,
    ) -> None:
        connection.execute(
            """
            UPDATE trades
            SET last_status = ?,
                last_action_sent = ?,
                last_alert_timestamp = COALESCE(?, last_alert_timestamp),
                updated_at = ?
            WHERE id = ?
            """,
            (
                last_status or None,
                last_action_sent or None,
                last_alert_timestamp,
                current_timestamp(),
                trade_id,
            ),
        )

    def _stamp_trade_alert_timestamps(self, connection: sqlite3.Connection, trade_ids: List[int], sent_at: str | None) -> None:
        if not sent_at:
            return
        for trade_id in [item for item in trade_ids if item > 0]:
            connection.execute(
                "UPDATE trades SET last_alert_timestamp = ?, updated_at = ? WHERE id = ?",
                (sent_at, current_timestamp(), trade_id),
            )

    def _mark_runtime_setting(self, connection: sqlite3.Connection, column_name: str, value: str) -> None:
        connection.execute(
            f"UPDATE open_trade_management_runtime_settings SET {column_name} = ? WHERE singleton_id = 1",
            (value,),
        )

    def _load_closed_real_trades_for_date(self, target_date: date) -> List[Dict[str, Any]]:
        trades = self.trade_store.list_trades("real")
        results = []
        for trade in trades:
            close_events = trade.get("close_events") or []
            if str(trade.get("derived_status_raw") or "").lower() not in {"closed", "expired"}:
                continue
            if not close_events:
                continue
            last_event = close_events[-1]
            event_date = parse_datetime_value(last_event.get("event_datetime") or last_event.get("created_at"))
            if event_date is None or event_date.date() != target_date:
                continue
            results.append(trade)
        return results

    def _is_morning_snapshot_due(self, now: datetime, runtime_settings: Dict[str, Any]) -> bool:
        if not self._is_trading_day(now.date()):
            return False
        if str(runtime_settings.get("last_morning_snapshot_date") or "") == now.date().isoformat():
            return False
        snapshot_time = datetime.combine(now.date(), time(self.MARKET_OPEN_HOUR, self.MARKET_OPEN_MINUTE), tzinfo=self.display_timezone) + timedelta(minutes=self.MORNING_SNAPSHOT_DELAY_MINUTES)
        return now >= snapshot_time

    def _is_eod_summary_due(self, now: datetime, runtime_settings: Dict[str, Any]) -> bool:
        if not self._is_trading_day(now.date()):
            return False
        if str(runtime_settings.get("last_eod_summary_date") or "") == now.date().isoformat():
            return False
        eod_time = datetime.combine(now.date(), time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE), tzinfo=self.display_timezone) + timedelta(minutes=self.EOD_SUMMARY_DELAY_MINUTES)
        return now >= eod_time

    def _is_monitor_window(self, now: datetime) -> bool:
        if not self._is_trading_day(now.date()):
            return False
        market_open = datetime.combine(now.date(), time(self.MARKET_OPEN_HOUR, self.MARKET_OPEN_MINUTE), tzinfo=self.display_timezone)
        eod_cutoff = datetime.combine(now.date(), time(self.MARKET_CLOSE_HOUR, self.MARKET_CLOSE_MINUTE), tzinfo=self.display_timezone) + timedelta(minutes=self.EOD_SUMMARY_DELAY_MINUTES + 1)
        return market_open <= now <= eod_cutoff

    def _is_trading_day(self, target_date: date) -> bool:
        return self.market_calendar_service.is_tradable_market_day(target_date)

    @staticmethod
    def _safe_divide(numerator: float | None, denominator: float | None) -> float:
        if numerator is None or denominator in {None, 0}:
            return 0.0
        return round(float(numerator) / float(denominator), 2)

    @staticmethod
    def _derive_session_posture(*, net_change_percent: float, close_vs_vwap_percent: float, recent_change_percent: float) -> str:
        if net_change_percent >= 0.18 and close_vs_vwap_percent > 0.05 and recent_change_percent >= -0.02:
            return "Trend Up Above VWAP"
        if net_change_percent <= -0.18 and close_vs_vwap_percent < -0.05 and recent_change_percent <= 0.02:
            return "Trend Down Below VWAP"
        if abs(close_vs_vwap_percent) <= 0.05:
            return "Rotating Around VWAP"
        return "Mixed / Transition"

    def _safe_snapshot(self, ticker: str, *, query_type: str) -> Dict[str, Any]:
        try:
            return self.market_data_service.get_latest_snapshot(ticker, query_type=query_type)
        except MarketDataError:
            return {"status_unavailable": True}

    def _coerce_trade_expiration(self, trade: Dict[str, Any]) -> date | None:
        return parse_date_value(trade.get("expiration_date"))

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _now(self) -> datetime:
        value = self.now_provider()
        if value.tzinfo is None:
            return value.replace(tzinfo=self.display_timezone)
        return value.astimezone(self.display_timezone)

    @staticmethod
    def _coerce_text(value: Any, *, fallback: str) -> str:
        text = str(value or "").strip()
        return text or fallback

    @staticmethod
    def _coerce_float(value: Any, fallback: float | None = None) -> float | None:
        parsed = to_float(value)
        return fallback if parsed is None else parsed

    @staticmethod
    def _percent_change(current_value: float, reference_value: float) -> float:
        if reference_value in {None, 0}:
            return 0.0
        return ((current_value - reference_value) / reference_value) * 100.0

    @staticmethod
    def _slugify(value: str) -> str:
        return str(value or "").strip().lower().replace(" ", "-")

    def _format_date(self, value: date | None) -> str:
        return value.isoformat() if value is not None else "—"

    def _format_datetime(self, value: datetime | None) -> str:
        if value is None:
            return "—"
        return value.astimezone(self.display_timezone).strftime("%Y-%m-%d %I:%M %p %Z").replace(" 0", " ")

    @staticmethod
    def _format_number(value: float | None, decimals: int = 2) -> str:
        if value is None:
            return "—"
        return f"{float(value):,.{decimals}f}"

    def _format_points(self, value: float | None) -> str:
        if value is None:
            return "—"
        return f"{float(value):,.1f} pts"

    def _format_distance_to_strike(self, value: float | None) -> str:
        if value is None:
            return "—"
        if float(value) >= 0:
            return f"{abs(float(value)):,.1f} pts away"
        return f"Breached by {abs(float(value)):,.1f} pts"

    @staticmethod
    def _format_percent(value: float | None) -> str:
        if value is None:
            return "—"
        return f"{float(value):,.1f}%"

    @staticmethod
    def _format_signed_percent(value: float | None) -> str:
        if value is None:
            return "—"
        return f"{float(value):+,.1f}%"

    @staticmethod
    def _format_currency(value: float | None) -> str:
        if value is None:
            return "—"
        sign = "-" if value < 0 else ""
        return f"{sign}${abs(float(value)):,.2f}"