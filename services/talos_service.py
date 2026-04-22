"""Local-only Talos autonomous simulated trade orchestration."""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from config import AppConfig, get_app_config

from .apollo_service import ApolloService
from .kairos_service import KairosService
from .market_calendar_service import MarketCalendarService
from .market_data import MarketDataService
from .open_trade_manager import OpenTradeManager
from .options_chain_service import OptionsChainService
from .performance_dashboard_service import build_performance_record, calculate_expectancy, summarize_outcomes
from .repositories.scenario_repository import KairosBundleRepository
from .repositories.trade_repository import TradeRepository
from .runtime.scheduler import RuntimeJobHandle, RuntimeScheduler, ThreadingTimerScheduler
from .trade_store import JOURNAL_NAME_DEFAULT, current_timestamp, normalize_system_name, parse_date_value


LOGGER = logging.getLogger(__name__)


class NoopTalosService:
    def initialize(self) -> None:
        return None

    def start_background_monitoring(self) -> None:
        return None

    def shutdown(self) -> None:
        return None

    def get_dashboard_payload(self) -> Dict[str, Any]:
        return {
            "enabled": False,
            "account_balance": 135000.0,
            "account_balance_display": "$135,000.00",
            "settled_balance": 135000.0,
            "settled_balance_display": "$135,000.00",
            "unrealized_pnl": 0.0,
            "unrealized_pnl_display": "$0.00",
            "open_trade_count": 0,
            "open_records": [],
            "activity_log": [],
            "skip_log": [],
            "slot_statuses": [],
            "header_market_snapshots": {},
            "last_cycle_at": "",
            "last_cycle_at_display": "Not yet evaluated",
            "last_cycle_reason": "",
            "last_cycle_status": "disabled",
        }

    def run_cycle(self, *, trigger_reason: str = "manual") -> Dict[str, Any]:
        return self.get_dashboard_payload()

    def reset_state(self, *, new_starting_balance: float) -> Dict[str, Any]:
        return {"deleted_trade_count": 0}


class TalosService:
    STARTING_BALANCE = 135000.0
    BACKGROUND_INTERVAL_SECONDS = 60
    POST_CLOSE_FINAL_SCAN_MINUTES = 1
    MARKET_OPEN = time(8, 30)
    MARKET_CLOSE = time(15, 0)
    APOLLO_ENTRY_WINDOW_HOURS = 3
    NO_CLOSE_AFTER_OPEN_MINUTES = 15
    PROFIT_CAPTURE_ROTATION_THRESHOLD = 75.0
    ENTRY_SCORE_MINIMUM = 70.0
    MAX_ACTIVITY_ITEMS = 60
    MAX_SKIP_ITEMS = 60
    MAX_TOTAL_OPEN_TRADES = 2
    MAX_OPEN_TRADES_PER_SYSTEM = {"Apollo": 1, "Kairos": 1}
    SLOT_ORDER = ("Apollo", "Kairos")
    SCORE_BANDS = (
        (90.0, "Elite", "elite"),
        (80.0, "Strong", "strong"),
        (70.0, "Acceptable", "acceptable"),
        (60.0, "Weak", "weak"),
        (0.0, "Reject", "reject"),
    )

    def __init__(
        self,
        *,
        trade_store: TradeRepository,
        market_data_service: MarketDataService,
        options_chain_service: OptionsChainService,
        open_trade_manager: OpenTradeManager,
        config: AppConfig | None = None,
        scenario_repository: KairosBundleRepository | None = None,
        scheduler: RuntimeScheduler | None = None,
        state_path: str | Path | None = None,
        now_provider=None,
    ) -> None:
        self.config = config or get_app_config()
        self.trade_store = trade_store
        self.market_data_service = market_data_service
        self.options_chain_service = options_chain_service
        self.open_trade_manager = open_trade_manager
        self.scenario_repository = scenario_repository
        self.scheduler = scheduler or ThreadingTimerScheduler()
        self.display_timezone = ZoneInfo(self.config.app_timezone)
        self.market_calendar_service = MarketCalendarService(self.config)
        self.now_provider = now_provider or (lambda: datetime.now(self.display_timezone))
        self.state_path = Path(state_path) if state_path is not None else Path(self.trade_store.database_path).with_name("talos_state.json")
        self.archive_directory = self.state_path.with_name("talos_recovery")
        self._lock = RLock()
        self._monitor_running = False
        self._monitor_timer: RuntimeJobHandle | None = None

    def initialize(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.archive_directory.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if not self.state_path.exists():
                self._save_state(self._default_state())

    def start_background_monitoring(self) -> None:
        with self._lock:
            if self._monitor_running:
                return
            self._monitor_running = True
            self._schedule_background_monitor()

    def shutdown(self) -> None:
        with self._lock:
            self._monitor_running = False
            if self._monitor_timer is not None:
                self._monitor_timer.cancel()
                self._monitor_timer = None

    def get_dashboard_payload(self) -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            changed = False
            scan_engine_running = bool(self._monitor_running)
            if bool(state.get("scan_engine_running")) != scan_engine_running:
                state["scan_engine_running"] = scan_engine_running
                changed = True
            management_payload = self.open_trade_manager.evaluate_open_trades(send_alerts=False)
            open_records = self._filter_talos_records(management_payload)
            if self._reconcile_open_trade_governance(state, open_records, source_label="dashboard refresh"):
                management_payload = self.open_trade_manager.evaluate_open_trades(send_alerts=False)
                open_records = self._filter_talos_records(management_payload)
                changed = True
            if self._sync_trade_metadata(state, open_records):
                changed = True
            if changed:
                self._save_state(state)
            return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

    def run_cycle(self, *, trigger_reason: str = "manual") -> Dict[str, Any]:
        with self._lock:
            state = self._load_state()
            now = self._now()
            state["scan_engine_running"] = bool(self._monitor_running)
            if state.get("paused"):
                state["last_scan_at"] = now.isoformat()
                state["total_scan_count"] = int(state.get("total_scan_count") or 0) + 1
                state["last_cycle_at"] = now.isoformat()
                state["last_cycle_reason"] = trigger_reason
                state["last_cycle_status"] = "paused"
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "status",
                        "title": "Talos cycle skipped",
                        "detail": "Talos is paused, so no autonomous actions were taken.",
                    },
                )
                self._save_state(state)
                return self._build_dashboard_payload(state)

            management_payload = self.open_trade_manager.evaluate_open_trades(send_alerts=False)
            open_records = self._filter_talos_records(management_payload)
            if self._reconcile_open_trade_governance(state, open_records, source_label=f"{trigger_reason} cycle"):
                management_payload = self.open_trade_manager.evaluate_open_trades(send_alerts=False)
                open_records = self._filter_talos_records(management_payload)
            self._sync_trade_metadata(state, open_records)
            cycle_mode = self._resolve_cycle_mode(now)

            state["last_scan_at"] = now.isoformat()
            state["total_scan_count"] = int(state.get("total_scan_count") or 0) + 1

            if cycle_mode == "closed":
                state["last_cycle_at"] = now.isoformat()
                state["last_cycle_reason"] = trigger_reason
                state["last_cycle_status"] = "outside-market-hours"
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": "Talos",
                        "reason": "Market window is closed.",
                    },
                )
                self._save_state(state)
                return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

            management_changed = self._manage_open_trades(state, open_records)
            if management_changed:
                management_payload = self.open_trade_manager.evaluate_open_trades(send_alerts=False)
                open_records = self._filter_talos_records(management_payload)
                self._sync_trade_metadata(state, open_records)

            if cycle_mode == "post-close-final":
                state["last_cycle_at"] = now.isoformat()
                state["last_cycle_reason"] = trigger_reason
                state["last_cycle_status"] = "post-close-final-pass"
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "status",
                        "title": "Talos final post-close pass",
                        "detail": "Talos completed the final post-close management pass. No new Talos entries are allowed after the regular market close.",
                    },
                )
                self._save_state(state)
                return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

            balance = self._compute_balance_components(open_records)
            candidates = self._build_candidates(balance["settled_balance"], open_records)
            for system_name in self.SLOT_ORDER:
                action_taken = self._evaluate_slot(state, system_name, candidates.get(system_name), open_records)
                if action_taken:
                    management_payload = self.open_trade_manager.evaluate_open_trades(send_alerts=False)
                    open_records = self._filter_talos_records(management_payload)
                    self._sync_trade_metadata(state, open_records)

            state["last_cycle_at"] = now.isoformat()
            state["last_cycle_reason"] = trigger_reason
            state["last_cycle_status"] = "completed"
            self._save_state(state)
            return self._build_dashboard_payload(state, management_payload=management_payload, open_records=open_records)

    def reset_state(self, *, new_starting_balance: float) -> Dict[str, Any]:
        with self._lock:
            talos_trades = list(self.trade_store.list_trades("talos"))
            archive_path = self._archive_runtime_snapshot(self._load_state(), talos_trades)
            for trade in talos_trades:
                trade_id = int(trade.get("id") or 0)
                if trade_id > 0:
                    self.trade_store.delete_trade(trade_id)
            self._save_state(self._default_state(starting_balance=new_starting_balance, last_archive_path=str(archive_path)))
            return {
                "deleted_trade_count": len(talos_trades),
                "archive_path": str(archive_path),
                "new_starting_balance": float(new_starting_balance),
            }

    def _schedule_background_monitor(self) -> None:
        if not self._monitor_running:
            return
        next_run_at = self._resolve_next_background_run_at(self._now())
        if next_run_at is None:
            return
        delay_seconds = max((next_run_at - self._now()).total_seconds(), 1.0)
        state = self._load_state()
        state["scan_engine_running"] = True
        state["next_scan_at"] = next_run_at.isoformat()
        self._save_state(state)
        self._monitor_timer = self.scheduler.schedule(
            delay_seconds,
            self._background_monitor_tick,
            daemon=True,
        )

    def _background_monitor_tick(self) -> None:
        try:
            self.run_cycle(trigger_reason="background")
        except Exception as exc:  # pragma: no cover - defensive scheduler guard
            LOGGER.warning("Talos background cycle failed: %s", exc)
        finally:
            with self._lock:
                if self._monitor_running:
                    self._schedule_background_monitor()

    def _build_dashboard_payload(
        self,
        state: Dict[str, Any],
        *,
        management_payload: Dict[str, Any] | None = None,
        open_records: List[Dict[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        management_payload = management_payload or self.open_trade_manager.evaluate_open_trades(send_alerts=False)
        open_records = open_records if open_records is not None else self._filter_talos_records(management_payload)
        balance = self._compute_balance_components(open_records)
        metadata_map = state.get("trade_metadata") or {}
        decorated_open_records = [self._decorate_open_record(record, metadata_map.get(str(record.get("trade_id") or ""), {})) for record in open_records]
        skip_log = list(state.get("skip_log") or [])
        runtime_status = self._build_runtime_status_payload(state)
        return {
            "enabled": True,
            "account_balance": balance["settled_balance"],
            "account_balance_display": self._format_currency(balance["settled_balance"]),
            "settled_balance": balance["settled_balance"],
            "settled_balance_display": self._format_currency(balance["settled_balance"]),
            "current_account_value": balance["current_account_value"],
            "current_account_value_display": self._format_currency(balance["current_account_value"]),
            "realized_pnl": balance["realized_pnl"],
            "realized_pnl_display": self._format_currency(balance["realized_pnl"]),
            "unrealized_pnl": balance["unrealized_pnl"],
            "unrealized_pnl_display": self._format_currency(balance["unrealized_pnl"]),
            "open_trade_count": len(open_records),
            "open_records": decorated_open_records,
            "formula_chart": self._build_formula_chart_payload(),
            "current_commitment": self._build_current_commitment_payload(open_records, balance),
            "settled_equity_curve": self._build_settled_equity_curve_payload(state),
            "performance_metrics": self._build_performance_metrics_payload(),
            "activity_log": list(state.get("activity_log") or []),
            "skip_log": skip_log,
            "skip_log_summary": self._build_skip_log_summary(skip_log),
            "slot_statuses": self._build_slot_statuses(open_records, metadata_map),
            "decision_model": self._build_decision_model_payload(decorated_open_records),
            "header_market_snapshots": dict(management_payload.get("header_market_snapshots") or {}),
            "last_cycle_at": str(state.get("last_cycle_at") or ""),
            "last_cycle_at_display": self._format_datetime(state.get("last_cycle_at")),
            "last_cycle_reason": str(state.get("last_cycle_reason") or ""),
            "last_cycle_status": str(state.get("last_cycle_status") or "idle"),
            "last_scan_at_display": self._format_datetime(state.get("last_scan_at")),
            "next_scan_at_display": self._format_datetime(state.get("next_scan_at")),
            "scan_engine_running": bool(state.get("scan_engine_running")),
            "runtime_status": runtime_status,
            "runtime_status_display": runtime_status["label"],
            "runtime_status_key": runtime_status["key"],
            "total_scan_count": int(state.get("total_scan_count") or 0),
            "starting_balance": float(state.get("starting_balance") or self.STARTING_BALANCE),
            "starting_balance_display": self._format_currency(float(state.get("starting_balance") or self.STARTING_BALANCE)),
            "recovery_archive_dir": str(self.archive_directory),
            "last_archive_path": str(state.get("last_archive_path") or ""),
            "paused": bool(state.get("paused")),
        }

    def _decorate_open_record(self, record: Dict[str, Any], metadata: Dict[str, Any]) -> Dict[str, Any]:
        decorated = dict(record)
        trade = self.trade_store.get_trade(int(record.get("trade_id") or 0)) if int(record.get("trade_id") or 0) > 0 else None
        talos_metadata = dict(metadata or {})
        score_breakdown = self._score_open_position_record(decorated, trade, talos_metadata)
        candidate_score = self._coerce_float(score_breakdown.get("candidate_score"))
        score_band = self._score_band_for_value(candidate_score)
        decorated["talos_metadata"] = talos_metadata
        decorated["talos_candidate_score"] = candidate_score
        decorated["talos_candidate_score_display"] = f"{candidate_score:.1f}" if candidate_score is not None else "—"
        decorated["talos_score_band_label"] = score_band["label"]
        decorated["talos_score_band_key"] = score_band["key"]
        decorated["talos_entry_ready"] = bool(candidate_score is not None and candidate_score >= self.ENTRY_SCORE_MINIMUM)
        decorated["talos_entry_readiness_label"] = "Enterable" if decorated["talos_entry_ready"] else "Do Not Enter"
        decorated["talos_score_breakdown_items"] = self._build_score_breakdown_items(score_breakdown)
        decorated["talos_pricing_basis_label"] = talos_metadata.get("pricing_basis_label") or "Conservative executable fills"
        decorated["talos_pricing_math_display"] = talos_metadata.get("pricing_math_display") or "Entry uses short bid - long ask. Exit uses short ask - long bid."
        decorated["talos_entry_gate_label"] = talos_metadata.get("entry_gate_label") or "Talos accepted a qualified system candidate."
        decorated["talos_score_explanation_lines"] = self._build_score_explanation_lines(score_breakdown, talos_metadata)
        return decorated

    def _score_open_position_record(
        self,
        record: Dict[str, Any],
        trade: Dict[str, Any] | None,
        talos_metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        system_name = normalize_system_name((trade or {}).get("system_name") or record.get("system_name"))
        candidate_profile = str((trade or {}).get("candidate_profile") or record.get("profile_label") or "Standard")
        current_expected_move = self._coerce_float(record.get("current_live_expected_move"))
        distance_points = max(float(record.get("distance_to_short") or 0.0), 0.0)
        em_multiple = self._coerce_float(record.get("current_em_multiple"))
        if em_multiple is None:
            em_multiple = self._coerce_float(record.get("actual_em_multiple")) or self._coerce_float((trade or {}).get("actual_em_multiple"))
        short_delta = self._coerce_float(record.get("short_delta"))
        if short_delta is None:
            short_delta = self._coerce_float((trade or {}).get("short_delta"))
        conservative_open_credit = self._coerce_float((trade or {}).get("actual_entry_credit"))
        if conservative_open_credit is None:
            conservative_open_credit = self._coerce_float(record.get("net_credit_per_contract"))
        spread_width = self._coerce_float((trade or {}).get("spread_width"))
        if spread_width is None:
            spread_width = self._coerce_float((record.get("max_loss") or 0.0) / 100.0)
        contracts = int((trade or {}).get("contracts") or record.get("contracts") or 0)
        total_premium = self._coerce_float((trade or {}).get("total_premium"))
        if total_premium is None and conservative_open_credit is not None and contracts > 0:
            total_premium = conservative_open_credit * contracts * 100.0
        fit_component = self._fit_score(
            system_name=system_name,
            structure_context=record.get("current_structure_grade") or (trade or {}).get("structure_grade"),
            macro_context=record.get("thesis_status") or (trade or {}).get("macro_grade"),
        ) * 10.0
        history = self._build_history_profile(system_name, candidate_profile)
        safety_component = self._score_safety_component(
            system_name=system_name,
            em_multiple=em_multiple,
            distance_points=distance_points,
            expected_move=current_expected_move,
            short_delta=short_delta,
        )
        history_component = history["history_component"]
        credit_component = self._score_credit_efficiency_component(conservative_open_credit, spread_width)
        profit_component = self._score_profit_opportunity_component(total_premium)
        loss_penalty_component = history["loss_penalty_component"]
        candidate_score = max(
            0.0,
            min(
                100.0,
                round(
                    safety_component
                    + history_component
                    + credit_component
                    + profit_component
                    + fit_component
                    - loss_penalty_component,
                    2,
                ),
            ),
        )
        return {
            "candidate_score": candidate_score,
            "safety_component": round(safety_component, 2),
            "history_component": round(history_component, 2),
            "credit_component": round(credit_component, 2),
            "profit_component": round(profit_component, 2),
            "fit_component": round(fit_component, 2),
            "loss_penalty_component": round(loss_penalty_component, 2),
            "history_scope": history["scope"],
            "history_sample_count": history["sample_count"],
            "win_rate_component": round(history["win_rate_component"], 2),
            "expectancy_component": round(history["expectancy_component"], 2),
            "sample_confidence_component": round(history["sample_confidence_component"], 2),
            "distance_component": round(self._score_distance_component(system_name, distance_points, current_expected_move, em_multiple), 2),
            "em_multiple_component": round(self._score_em_multiple_component(em_multiple), 2),
            "short_delta_component": round(self._score_short_delta_component(short_delta), 2),
            "score_breakdown_display": (
                f"Score {candidate_score:.1f} = Safety {safety_component:.1f} + History {history_component:.1f} + "
                f"Credit {credit_component:.1f} + Profit {profit_component:.1f} + Fit {fit_component:.1f} - "
                f"Loss Penalty {loss_penalty_component:.1f}."
            ),
        }

    def _build_score_explanation_lines(self, score_breakdown: Dict[str, Any], talos_metadata: Dict[str, Any]) -> List[str]:
        lines: List[str] = []
        if talos_metadata.get("selection_basis"):
            lines.append(str(talos_metadata.get("selection_basis")))
        if talos_metadata.get("decision_note"):
            lines.append(str(talos_metadata.get("decision_note")))
        if talos_metadata.get("pricing_math_display"):
            lines.append(f"Pricing: {talos_metadata.get('pricing_math_display')}")
        if score_breakdown:
            lines.append(
                "Components: "
                f"Safety {float(score_breakdown.get('safety_component') or 0.0):.1f}, "
                f"History {float(score_breakdown.get('history_component') or 0.0):.1f}, "
                f"Credit {float(score_breakdown.get('credit_component') or 0.0):.1f}, "
                f"Profit {float(score_breakdown.get('profit_component') or 0.0):.1f}, "
                f"Fit {float(score_breakdown.get('fit_component') or 0.0):.1f}, "
                f"Loss Penalty {float(score_breakdown.get('loss_penalty_component') or 0.0):.1f}."
            )
        return lines

    def _build_score_breakdown_items(self, score_breakdown: Dict[str, Any]) -> List[Dict[str, str]]:
        component_specs = (
            ("Safety", "safety_component"),
            ("History", "history_component"),
            ("Credit", "credit_component"),
            ("Profit", "profit_component"),
            ("Fit", "fit_component"),
            ("Loss Penalty", "loss_penalty_component"),
        )
        items: List[Dict[str, str]] = []
        for label, key in component_specs:
            value = self._coerce_float(score_breakdown.get(key))
            if value is None:
                continue
            prefix = "-" if key == "loss_penalty_component" else "+"
            items.append({"label": label, "value": f"{prefix}{value:.1f}"})
        return items

    def _build_skip_log_summary(self, skip_log: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        summary: List[Dict[str, str]] = []
        for item in skip_log[:5]:
            repeat_count = max(int(item.get("repeat_count") or 1), 1)
            label = str(item.get("reason") or "Talos skip")
            if repeat_count > 1:
                label = f"{label} x{repeat_count}"
            summary.append(
                {
                    "system": str(item.get("system") or "Talos"),
                    "label": label,
                    "timestamp": self._format_datetime(item.get("timestamp") or item.get("latest_timestamp") or ""),
                }
            )
        return summary

    def _build_decision_model_payload(self, open_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        example_record = next((item for item in open_records if item.get("talos_score_breakdown_items")), None)
        return {
            "formula": "Score = safety 40 + history 25 + credit 15 + profit 10 + fit 10 - loss penalty 20.",
            "minimum_entry_score": self.ENTRY_SCORE_MINIMUM,
            "score_policy": "Talos scores every candidate on a 1-100 scale and only enters trades that score 70 or higher.",
            "bands": [
                {"minimum": minimum, "label": label, "key": key}
                for minimum, label, key in self.SCORE_BANDS
            ],
            "weights": [
                "Safety 40%: expected-move spacing plus raw distance to the short strike.",
                "History 25%: profile/system win rate, average P/L, and sample size.",
                "Credit 15%: credit earned per width of risk.",
                "Profit 10%: absolute premium opportunity.",
                "Fit 10%: current structure and timing alignment.",
                "Loss penalty 20%: average loss severity plus black-swan frequency.",
            ],
            "pricing_rule": "Simulated entry fill = short bid - long ask. Simulated exit fill = short ask - long bid.",
            "rotation_rule": "Talos only rotates after more than 75% of credit is captured, a replacement candidate is available, and the replacement score is equal or better.",
            "kairos_entry_rule": "Talos only opens Kairos when the Kairos best-trade override is Ready and the mode is Prime or Window Open.",
            "example_record": example_record,
        }

    def _build_runtime_status_payload(self, state: Dict[str, Any]) -> Dict[str, str]:
        if bool(state.get("paused")):
            return {"label": "Paused", "key": "paused"}
        if bool(state.get("scan_engine_running")):
            return {"label": "Live Monitor Running", "key": "running"}
        last_cycle_status = str(state.get("last_cycle_status") or "").strip().lower()
        if last_cycle_status == "post-close-final-pass":
            return {"label": "Final Post-Close Pass Complete", "key": "final-pass"}
        if last_cycle_status == "outside-market-hours":
            return {"label": "Waiting For Market Window", "key": "waiting"}
        if last_cycle_status == "completed":
            return {"label": "Ready For Next Scan", "key": "ready"}
        return {"label": "Idle", "key": "idle"}

    def _build_formula_chart_payload(self) -> Dict[str, Any]:
        segments = [
            {"label": "Safety", "weight": 40.0, "label_display": "Safety 40%", "color": "#ef4444", "shade_color": "#b91c1c"},
            {"label": "History", "weight": 25.0, "label_display": "History 25%", "color": "#f97316", "shade_color": "#c2410c"},
            {"label": "Credit", "weight": 15.0, "label_display": "Credit 15%", "color": "#eab308", "shade_color": "#a16207"},
            {"label": "Profit", "weight": 10.0, "label_display": "Profit 10%", "color": "#22c55e", "shade_color": "#15803d"},
            {"label": "Fit", "weight": 10.0, "label_display": "Fit 10%", "color": "#3b82f6", "shade_color": "#1d4ed8"},
        ]
        cursor = 0.0
        gradients: List[str] = []
        for item in segments:
            sweep = (item["weight"] / 100.0) * 360.0
            gradients.append(f"{item['color']} {cursor:.2f}deg {cursor + sweep:.2f}deg")
            cursor += sweep
        return {
            "chart_style": f"conic-gradient({', '.join(gradients)})",
            "segments": [
                {
                    "label": item["label"],
                    "label_display": item["label_display"],
                    "weight": item["weight"],
                    "weight_display": f"{item['weight']:.0f}%",
                    "color": item["color"],
                    "shade_color": item["shade_color"],
                }
                for item in segments
            ],
            "penalty_label": "Loss Penalty",
            "penalty_weight_display": "20% penalty",
            "penalty_note": "Loss penalty is applied separately in the decision model rather than folded into the 100% positive-weight pie.",
        }

    def _build_current_commitment_payload(self, open_records: List[Dict[str, Any]], balance: Dict[str, float]) -> Dict[str, Any]:
        commitment_value = 0.0
        for item in open_records:
            commitment_value += float(item.get("remaining_risk") or item.get("max_loss") or 0.0)
        gauge_max = max(float(balance.get("current_account_value") or 0.0), 1.0)
        ratio = max(0.0, min(commitment_value / gauge_max, 1.0))
        return {
            "value": round(commitment_value, 2),
            "value_display": self._format_currency(commitment_value),
            "gauge_max": round(gauge_max, 2),
            "gauge_max_display": self._format_currency(gauge_max),
            "ratio": round(ratio, 4),
        }

    def _build_performance_metrics_payload(self) -> Dict[str, Any]:
        talos_records = [build_performance_record(trade) for trade in self.trade_store.list_trades("talos")]
        outcomes = summarize_outcomes(talos_records)
        total_trades = len(talos_records)
        open_trades = len(outcomes.open_records)
        closed_trades = len(outcomes.closed_records)
        win_count = len(outcomes.wins)
        loss_count = len(outcomes.losses)
        black_swan_count = len(outcomes.black_swans)
        total_loss_outcomes = len(outcomes.loss_events)
        scored_outcomes = len(outcomes.scored_outcomes)
        win_rate_ratio = (win_count / scored_outcomes) if scored_outcomes else 0.0
        loss_rate_ratio = (total_loss_outcomes / scored_outcomes) if scored_outcomes else 0.0
        average_win = self._average(record.get("gross_pnl") for record in outcomes.wins)
        average_loss = abs(self._average(record.get("gross_pnl") for record in outcomes.losses))
        average_black_swan_loss = abs(self._average(record.get("gross_pnl") for record in outcomes.black_swans))
        expectancy_average_loss = abs(self._average(record.get("gross_pnl") for record in outcomes.loss_events))
        expectancy = calculate_expectancy(
            win_rate=win_rate_ratio,
            average_win=average_win,
            loss_rate=loss_rate_ratio,
            average_loss=expectancy_average_loss,
        )
        expectancy_scale = max(50.0, abs(expectancy) * 1.75, average_win, average_loss, average_black_swan_loss)
        net_pnl = sum(float(record.get("gross_pnl") or 0.0) for record in talos_records)
        closed_outcomes = win_count + loss_count + black_swan_count
        return {
            "totals": {
                "total_trades": total_trades,
                "open_trades": open_trades,
                "closed_trades": closed_trades,
            },
            "win_rate": {
                "value": round(win_rate_ratio * 100.0, 2),
                "ratio": round(win_rate_ratio, 4),
                "wins": win_count,
                "losses": total_loss_outcomes,
            },
            "expectancy": {
                "value": round(expectancy, 2),
                "scale": round(expectancy_scale, 2),
            },
            "net_pnl": {
                "value": round(net_pnl, 2),
            },
            "outcome_mix": {
                "win_percent": round((win_count / closed_outcomes) * 100.0, 2) if closed_outcomes else 0.0,
                "loss_percent": round((loss_count / closed_outcomes) * 100.0, 2) if closed_outcomes else 0.0,
                "black_swan_percent": round((black_swan_count / closed_outcomes) * 100.0, 2) if closed_outcomes else 0.0,
                "win_count": win_count,
                "loss_count": loss_count,
                "black_swan_count": black_swan_count,
                "closed_outcomes": closed_outcomes,
                "source": "Talos closed outcomes only",
            },
            "average_win_loss": {
                "average_win": round(average_win, 2),
                "average_loss": round(average_loss, 2),
                "average_black_swan_loss": round(average_black_swan_loss, 2),
                "ratio": round((average_win / average_loss), 2) if average_loss else None,
                "win_count": win_count,
                "loss_count": loss_count,
                "black_swan_count": black_swan_count,
            },
        }

    def _build_settled_equity_curve_payload(self, state: Dict[str, Any]) -> Dict[str, Any]:
        closed_trades = []
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status in {"open", "reduced"}:
                continue
            closed_trades.append(trade)
        closed_trades.sort(key=lambda trade: str(trade.get("exit_datetime") or trade.get("entry_datetime") or ""))
        starting_balance = float(state.get("starting_balance") or self.STARTING_BALANCE)
        values = [starting_balance]
        labels = ["Start"]
        cumulative = starting_balance
        for trade in closed_trades:
            cumulative += float(trade.get("gross_pnl") or 0.0)
            values.append(round(cumulative, 2))
            labels.append(str(trade.get("trade_number") or len(labels)))
        width = 640
        height = 220
        padding = 24
        if len(values) == 1:
            polyline_points = f"{padding:.2f},{height / 2.0:.2f} {width - padding:.2f},{height / 2.0:.2f}"
            return {
                "polyline_points": polyline_points,
                "area_points": f"{padding:.2f},{height - padding:.2f} {polyline_points} {width - padding:.2f},{height - padding:.2f}",
                "markers": [{"label": labels[0], "x": padding, "y": height / 2.0, "value_display": self._format_currency(values[0])}],
                "start_value_display": self._format_currency(values[0]),
                "end_value_display": self._format_currency(values[-1]),
                "closed_trade_count": len(closed_trades),
            }
        minimum = min(values)
        maximum = max(values)
        span = max(maximum - minimum, 1.0)
        x_step = (width - (padding * 2)) / max(len(values) - 1, 1)
        polyline_pairs: List[str] = []
        markers: List[Dict[str, Any]] = []
        for index, value in enumerate(values):
            x_value = padding + (x_step * index)
            y_value = height - padding - (((value - minimum) / span) * (height - (padding * 2)))
            polyline_pairs.append(f"{x_value:.2f},{y_value:.2f}")
            markers.append({"label": labels[index], "x": x_value, "y": y_value, "value_display": self._format_currency(value)})
        return {
            "polyline_points": " ".join(polyline_pairs),
            "area_points": f"{padding:.2f},{height - padding:.2f} {' '.join(polyline_pairs)} {width - padding:.2f},{height - padding:.2f}",
            "markers": markers,
            "start_value_display": self._format_currency(values[0]),
            "end_value_display": self._format_currency(values[-1]),
            "closed_trade_count": len(closed_trades),
        }

    def _score_band_for_value(self, value: float | None) -> Dict[str, Any]:
        numeric_value = float(value or 0.0)
        for minimum, label, key in self.SCORE_BANDS:
            if numeric_value >= minimum:
                return {"minimum": minimum, "label": label, "key": key}
        minimum, label, key = self.SCORE_BANDS[-1]
        return {"minimum": minimum, "label": label, "key": key}

    def _build_slot_statuses(self, open_records: List[Dict[str, Any]], metadata_map: Dict[str, Any]) -> List[Dict[str, Any]]:
        statuses: List[Dict[str, Any]] = []
        records_by_system: Dict[str, List[Dict[str, Any]]] = {system_name: [] for system_name in self.SLOT_ORDER}
        for item in open_records:
            records_by_system.setdefault(normalize_system_name(item.get("system_name")), []).append(item)
        for system_name in self.SLOT_ORDER:
            system_records = records_by_system.get(system_name) or []
            record = system_records[0] if system_records else None
            metadata = metadata_map.get(str((record or {}).get("trade_id") or ""), {}) if record else {}
            statuses.append(
                {
                    "system_name": system_name,
                    "occupied": record is not None,
                    "active_trade_count": len(system_records),
                    "trade_number": (record or {}).get("trade_number"),
                    "status": (record or {}).get("status") or "Idle",
                    "candidate_score": metadata.get("candidate_score"),
                    "score_breakdown_display": metadata.get("score_breakdown_display") or "",
                    "pricing_basis_label": metadata.get("pricing_basis_label") or "",
                    "decision_note": metadata.get("decision_note") or "No active Talos slot.",
                }
            )
        return statuses

    def _reconcile_open_trade_governance(self, state: Dict[str, Any], open_records: List[Dict[str, Any]], *, source_label: str) -> bool:
        records_by_system: Dict[str, List[Dict[str, Any]]] = {}
        for record in open_records:
            records_by_system.setdefault(normalize_system_name(record.get("system_name")), []).append(record)
        for system_name, records in records_by_system.items():
            allowed = int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1))
            if len(records) <= allowed:
                continue
            self._append_skip(
                state,
                {
                    "timestamp": self._now().isoformat(),
                    "system": system_name,
                    "reason": (
                        f"Talos governance detected {len(records)} open {system_name} positions during {source_label}, "
                        "but did not auto-close any trade because gate or rotation rules were not satisfied."
                    ),
                },
            )
        if len(open_records) > self.MAX_TOTAL_OPEN_TRADES:
            self._append_skip(
                state,
                {
                    "timestamp": self._now().isoformat(),
                    "system": "Talos",
                    "reason": (
                        f"Talos governance detected {len(open_records)} open positions during {source_label}, "
                        "but did not auto-close any trade because gate or rotation rules were not satisfied."
                    ),
                },
            )
        return False

    def _resolve_conservative_exit_value(self, record: Dict[str, Any]) -> float:
        current_spread_mark = self._coerce_float(record.get("current_spread_mark"))
        if current_spread_mark is not None:
            return round(max(current_spread_mark, 0.0), 4)
        return 0.0

    def _record_sort_key(self, record: Dict[str, Any]) -> tuple[float, int, int]:
        entry_timestamp = self._timestamp_sort_value(record.get("entry_datetime") or record.get("opened_at") or record.get("last_updated"))
        trade_number = int(record.get("trade_number") or 0)
        trade_id = int(record.get("trade_id") or record.get("id") or 0)
        return (entry_timestamp, trade_number, trade_id)

    def _timestamp_sort_value(self, value: Any) -> float:
        if not value:
            return 0.0
        try:
            stamp = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return 0.0
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=self.display_timezone)
        return stamp.timestamp()

    def _manage_open_trades(self, state: Dict[str, Any], open_records: List[Dict[str, Any]]) -> bool:
        now = self._now()
        action_taken = False
        for record in open_records:
            trade_id = int(record.get("trade_id") or 0)
            if trade_id <= 0:
                continue
            current_spread_mark = record.get("current_spread_mark")
            expiration_date = parse_date_value(record.get("expiration"))
            due_gate = self._first_due_exit_gate(record)
            if due_gate is not None:
                if self._is_close_restricted_window(now):
                    self._append_skip(
                        state,
                        {
                            "timestamp": now.isoformat(),
                            "system": str(record.get("system_name") or "Talos"),
                            "reason": (
                                f"Talos blocked close for Trade #{record.get('trade_number')} because no Talos close is allowed "
                                f"during the first {self.NO_CLOSE_AFTER_OPEN_MINUTES} minutes after the market opens."
                            ),
                        },
                    )
                    continue
                contracts_to_close = min(
                    int(due_gate.get("quantity_to_close") or 0),
                    int(record.get("contracts") or record.get("remaining_contracts") or 0),
                )
                if current_spread_mark in {None, ""} or contracts_to_close <= 0:
                    self._append_skip(
                        state,
                        {
                            "timestamp": now.isoformat(),
                            "system": str(record.get("system_name") or "Talos"),
                            "reason": (
                                f"Talos blocked close for Trade #{record.get('trade_number')} because gate {due_gate.get('label') or 'pending gate'} "
                                "was due but conservative exit pricing or gate quantity was unavailable."
                            ),
                        },
                    )
                    continue
                close_method = "Close" if contracts_to_close >= int(record.get("contracts") or 0) else "Reduce"
                actual_exit_value = self._resolve_conservative_exit_value(record)
                self.trade_store.reduce_trade(
                    trade_id,
                    {
                        "contracts_closed": contracts_to_close,
                        "actual_exit_value": actual_exit_value,
                        "event_datetime": current_timestamp(),
                        "close_method": close_method,
                        "close_reason": f"Talos gate-based close: {due_gate.get('label') or 'exit gate'}",
                        "notes_exit": (
                            f"Talos {close_method.lower()} executed the due exit gate {due_gate.get('label') or 'exit gate'} at "
                            f"{record.get('evaluated_at_display') or self._format_datetime(now)} for exactly {contracts_to_close} contract(s). "
                            f"Conservative simulated exit debit {actual_exit_value:.2f} per contract."
                        ),
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "gate-close",
                        "title": f"Talos gate close {record.get('system_name')}",
                        "detail": (
                            f"Trade #{record.get('trade_number')} closed {contracts_to_close} contract(s) from gate "
                            f"{due_gate.get('label') or 'exit gate'}."
                        ),
                    },
                )
                action_taken = True
                continue
            if expiration_date is not None and expiration_date <= now.date() and now.time() >= self.MARKET_CLOSE:
                self.trade_store.expire_trade(
                    trade_id,
                    {
                        "event_datetime": current_timestamp(),
                        "actual_exit_value": 0.0,
                        "close_reason": "Talos expiration sweep",
                        "notes_exit": "Talos expired the remaining contracts after the regular market session.",
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "expiration",
                        "title": "Talos expiration",
                        "detail": f"Trade #{record.get('trade_number')} {record.get('system_name')} expired during the Talos end-of-session sweep.",
                    },
                )
                action_taken = True
        return action_taken

    def _evaluate_slot(self, state: Dict[str, Any], system_name: str, candidate: Dict[str, Any] | None, open_records: List[Dict[str, Any]]) -> bool:
        now = self._now()
        governance = self._build_slot_governance(open_records)
        system_records = [item for item in open_records if normalize_system_name(item.get("system_name")) == system_name]
        active_record = system_records[0] if system_records else None
        allowed_per_system = int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1))
        if governance["system_counts"].get(system_name, 0) > allowed_per_system:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": f"Talos governance blocked new {system_name} entries because {governance['system_counts'].get(system_name, 0)} {system_name} Talos trades are already open.",
                },
            )
            return False
        if governance["total_open_count"] > self.MAX_TOTAL_OPEN_TRADES:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": f"Talos governance blocked new entries because {governance['total_open_count']} Talos trades are already open.",
                },
            )
            return False
        if candidate is None:
            if active_record is None and governance["total_open_count"] >= self.MAX_TOTAL_OPEN_TRADES:
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": system_name,
                        "reason": "Talos candidate evaluation was skipped because both Talos governance slots are already occupied.",
                    },
                )
                return False
            if active_record is None and governance["system_counts"].get(system_name, 0) >= allowed_per_system:
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": system_name,
                        "reason": f"Talos candidate evaluation was skipped because the {system_name} slot is already full.",
                    },
                )
                return False
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": f"No qualified {system_name} Talos candidate was available.",
                },
            )
            return False

        candidate_score = float(candidate.get("candidate_score") or 0.0)
        if candidate_score < self.ENTRY_SCORE_MINIMUM:
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": (
                        f"Talos rejected the {system_name} candidate because its score was {candidate_score:.1f}. "
                        f"Talos only enters candidates scoring {self.ENTRY_SCORE_MINIMUM:.0f} or higher."
                    ),
                },
            )
            return False

        if active_record is None:
            if system_name == "Apollo" and not self._is_apollo_entry_window(now):
                self._append_skip(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "system": system_name,
                        "reason": "Talos blocked Apollo entry because Apollo can only open during the final 3 hours of the market day.",
                    },
                )
                return False
            return self._create_trade_from_candidate(state, system_name, candidate)

        metadata = (state.get("trade_metadata") or {}).get(str(active_record.get("trade_id") or ""), {})
        previous_score = float(metadata.get("candidate_score") or 0.0)
        current_trade_score = float(active_record.get("talos_candidate_score") or 0.0)
        baseline_score = max(previous_score, current_trade_score)
        captured = float(active_record.get("percent_credit_captured") or 0.0)
        current_spread_mark = active_record.get("current_spread_mark")
        if self._is_close_restricted_window(now):
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": (
                        f"Talos blocked close for Trade #{active_record.get('trade_number')} because no Talos close is allowed during the first "
                        f"{self.NO_CLOSE_AFTER_OPEN_MINUTES} minutes after the market opens."
                    ),
                },
            )
            return False
        if system_name == "Apollo" and not self._is_apollo_entry_window(now):
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": "Talos blocked Apollo rotation because Apollo can only open during the final 3 hours of the market day.",
                },
            )
            return False
        if captured > self.PROFIT_CAPTURE_ROTATION_THRESHOLD and candidate_score >= baseline_score and current_spread_mark not in {None, ""}:
            contracts_to_close = int(active_record.get("contracts") or 0)
            if contracts_to_close > 0:
                trade_id = int(active_record.get("trade_id") or 0)
                actual_exit_value = self._resolve_conservative_exit_value(active_record)
                self.trade_store.reduce_trade(
                    trade_id,
                    {
                        "contracts_closed": contracts_to_close,
                        "actual_exit_value": actual_exit_value,
                        "event_datetime": current_timestamp(),
                        "close_method": "Close",
                        "close_reason": "Talos rotation-based close",
                        "notes_exit": (
                            f"Talos rotated out of this trade after {captured:.1f}% credit capture when an equal-or-better {system_name} candidate appeared. "
                            f"Conservative simulated exit debit {actual_exit_value:.2f} per contract."
                        ),
                    },
                )
                self._append_activity(
                    state,
                    {
                        "timestamp": now.isoformat(),
                        "category": "rotation",
                        "title": f"Talos rotated {system_name}",
                        "detail": f"Trade #{active_record.get('trade_number')} was closed after {captured:.1f}% credit capture for an equal-or-better {system_name} setup.",
                    },
                )
                return self._create_trade_from_candidate(state, system_name, candidate)

        self._append_skip(
            state,
            {
                "timestamp": now.isoformat(),
                "system": system_name,
                "reason": (
                    f"Talos blocked close for Trade #{active_record.get('trade_number')} because rotation requires more than "
                    f"{self.PROFIT_CAPTURE_ROTATION_THRESHOLD:.0f}% credit capture and a replacement score at least as strong as the current slot. "
                    f"Current capture {captured:.1f}%; replacement {candidate_score:.1f}; current slot {baseline_score:.1f}."
                ),
            },
        )
        return False

    def _first_due_exit_gate(self, record: Dict[str, Any]) -> Dict[str, Any] | None:
        for gate in record.get("exit_plan_gates") or []:
            if gate.get("due_now"):
                return gate
        return None

    def _is_close_restricted_window(self, now: datetime) -> bool:
        local_now = now.astimezone(self.display_timezone)
        market_open_at = self._combine_market_time(local_now.date(), self.MARKET_OPEN)
        restricted_until = market_open_at + timedelta(minutes=self.NO_CLOSE_AFTER_OPEN_MINUTES)
        return market_open_at <= local_now < restricted_until

    def _is_apollo_entry_window(self, now: datetime) -> bool:
        local_now = now.astimezone(self.display_timezone)
        session_close = self._combine_market_time(local_now.date(), self.MARKET_CLOSE)
        session_open = self._combine_market_time(local_now.date(), self.MARKET_OPEN)
        entry_window_open = session_close - timedelta(hours=self.APOLLO_ENTRY_WINDOW_HOURS)
        return session_open <= local_now <= session_close and local_now >= entry_window_open

    def _create_trade_from_candidate(self, state: Dict[str, Any], system_name: str, candidate: Dict[str, Any]) -> bool:
        now = self._now()
        payload = dict(candidate.get("trade_payload") or {})
        if not payload:
            return False
        live_governance = self._build_slot_governance_from_trade_store()
        if not self._can_open_trade_for_system(system_name, live_governance):
            self._append_skip(
                state,
                {
                    "timestamp": now.isoformat(),
                    "system": system_name,
                    "reason": "Talos blocked the autonomous entry because slot governance changed before the order was written.",
                },
            )
            return False
        trade_id = self.trade_store.create_trade(payload)
        trade = self.trade_store.get_trade(trade_id) or {"id": trade_id}
        trade_number = trade.get("trade_number") or trade_id
        state.setdefault("trade_metadata", {})[str(trade_id)] = {
            "slot_key": system_name.lower(),
            "candidate_score": float(candidate.get("candidate_score") or 0.0),
            "decision_note": str(candidate.get("decision_note") or ""),
            "selection_basis": str(candidate.get("selection_basis") or ""),
            "score_breakdown_display": str(candidate.get("score_breakdown_display") or ""),
            "score_breakdown": dict(candidate.get("score_breakdown") or {}),
            "pricing_basis_label": str(candidate.get("pricing_basis_label") or ""),
            "pricing_math_display": str(candidate.get("pricing_math_display") or ""),
            "entry_gate_label": str(candidate.get("entry_gate_label") or ""),
            "opened_at": now.isoformat(),
        }
        self._append_activity(
            state,
            {
                "timestamp": now.isoformat(),
                "category": "entry",
                "title": f"Talos opened {system_name}",
                "detail": f"Trade #{trade_number} was opened automatically. {candidate.get('decision_note') or ''}".strip(),
            },
        )
        return True

    def _build_candidates(self, account_balance: float, open_records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any] | None]:
        governance = self._build_slot_governance(open_records)
        return {
            "Apollo": self._build_apollo_candidate(account_balance) if self._should_evaluate_candidate("Apollo", governance) else None,
            "Kairos": self._build_kairos_candidate(account_balance) if self._should_evaluate_candidate("Kairos", governance) else None,
        }

    def _build_apollo_candidate(self, account_balance: float) -> Dict[str, Any] | None:
        service = ApolloService(
            market_data_service=self.market_data_service,
            options_chain_service=self.options_chain_service,
            config=replace(self.config, apollo_account_value=max(account_balance, 1.0)),
        )
        precheck = service.run_precheck()
        trade_candidates = precheck.get("trade_candidates") or {}
        candidates = [item for item in (trade_candidates.get("candidates") or []) if isinstance(item, dict) and item.get("available")]
        if not candidates:
            return None
        scored_candidates = [
            (
                item,
                self._score_candidate(
                    system_name="Apollo",
                    candidate_profile=item.get("mode_label") or item.get("pass_type_label") or "Standard",
                    em_multiple=self._coerce_float(item.get("actual_em_multiple") or item.get("em_multiple")),
                    distance_points=self._coerce_float(item.get("actual_distance_to_short") or item.get("distance_points")),
                    short_delta=self._coerce_float(item.get("short_delta")),
                    expected_move=self._coerce_float(item.get("expected_move_used") or item.get("expected_move")),
                    credit=self._coerce_float(item.get("credit") or item.get("premium_per_contract")),
                    spread_width=self._coerce_float(item.get("width")),
                    max_loss=self._coerce_float(item.get("max_theoretical_risk") or item.get("max_loss")),
                    total_premium=(
                        max(self._coerce_float(item.get("executable_credit")) or self._coerce_float(item.get("credit")) or 0.0, 0.0)
                        * float(item.get("adjusted_contract_size") or item.get("recommended_contract_size") or 1)
                        * 100.0
                    ),
                    raw_candidate_score=self._coerce_float(item.get("score")),
                    structure_context=(precheck.get("structure") or {}).get("final_grade") or (precheck.get("structure") or {}).get("grade"),
                    macro_context=(precheck.get("macro") or {}).get("grade"),
                ),
            )
            for item in candidates
        ]
        selected, score_summary = max(
            scored_candidates,
            key=lambda entry: (
                float(entry[1].get("candidate_score") or 0.0),
                float(entry[1].get("safety_component") or 0.0),
                float(entry[1].get("raw_score") or 0.0),
            ),
        )
        entry_pricing = self._resolve_apollo_entry_pricing(selected)
        contracts = int(selected.get("adjusted_contract_size") or selected.get("recommended_contract_size") or 1)
        actual_entry_credit = entry_pricing["actual_entry_credit"]
        premium_per_contract = round(actual_entry_credit * 100.0, 2)
        payload = {
            "trade_mode": "talos",
            "system_name": "Apollo",
            "journal_name": JOURNAL_NAME_DEFAULT,
            "system_version": self._version_number(),
            "candidate_profile": selected.get("mode_label") or selected.get("pass_type_label") or "Standard",
            "status": "open",
            "trade_date": current_timestamp().split("T", 1)[0],
            "entry_datetime": current_timestamp(),
            "expiration_date": self._format_market_day((precheck.get("market_calendar") or {}).get("next_market_day")),
            "underlying_symbol": "SPX",
            "spx_at_entry": (precheck.get("spx") or {}).get("value") or "",
            "vix_at_entry": (precheck.get("vix") or {}).get("value") or "",
            "structure_grade": ((precheck.get("structure") or {}).get("final_grade") or (precheck.get("structure") or {}).get("grade") or ""),
            "macro_grade": (precheck.get("macro") or {}).get("grade") or "",
            "expected_move": selected.get("expected_move") or "",
            "expected_move_used": selected.get("expected_move_used") or selected.get("expected_move") or "",
            "expected_move_source": selected.get("expected_move_source") or "same_day_atm_straddle",
            "option_type": "Put Credit Spread",
            "short_strike": selected.get("short_strike") or "",
            "long_strike": selected.get("long_strike") or "",
            "spread_width": selected.get("width") or "",
            "contracts": contracts,
            "candidate_credit_estimate": selected.get("credit") or "",
            "actual_entry_credit": actual_entry_credit,
            "net_credit_per_contract": actual_entry_credit,
            "distance_to_short": selected.get("distance_points") or "",
            "em_multiple_floor": selected.get("applied_em_multiple_floor") or selected.get("target_em_multiple") or "",
            "percent_floor": selected.get("percent_floor") or "",
            "boundary_rule_used": selected.get("boundary_rule_used") or "",
            "actual_distance_to_short": selected.get("actual_distance_to_short") or selected.get("distance_points") or "",
            "actual_em_multiple": selected.get("actual_em_multiple") or selected.get("em_multiple") or "",
            "fallback_used": "yes" if selected.get("fallback_used") else "no",
            "fallback_rule_name": selected.get("fallback_rule_name") or "",
            "short_delta": selected.get("short_delta") or "",
            "pass_type": selected.get("pass_type") or "",
            "premium_per_contract": premium_per_contract,
            "total_premium": round(premium_per_contract * contracts, 2),
            "max_theoretical_risk": selected.get("max_theoretical_risk") or selected.get("max_loss") or "",
            "risk_efficiency": selected.get("risk_efficiency") or "",
            "target_em": selected.get("target_em") or selected.get("target_em_multiple") or "",
            "notes_entry": self._build_decision_note(
                system_name="Apollo",
                descriptor=selected.get("mode_label") or selected.get("mode_descriptor") or "candidate",
                account_balance=account_balance,
                candidate_score=float(score_summary.get("candidate_score") or 0.0),
                details=(
                    f"Distance {selected.get('distance_points') or 'n/a'} pts; EM {selected.get('em_multiple') or 'n/a'}x. "
                    f"Entry fill {entry_pricing['pricing_math_display']}. "
                    f"{score_summary.get('score_breakdown_display') or ''}"
                ).strip(),
            ),
            "prefill_source": "talos-apollo",
            "automation_status": "autonomous-entry-and-management",
            "close_method": "",
            "close_reason": "",
            "notes_exit": "",
        }
        decision_note = str(payload.get("notes_entry") or "")
        return {
            "candidate_score": float(score_summary.get("candidate_score") or 0.0),
            "decision_note": decision_note,
            "selection_basis": str(score_summary.get("selection_basis") or ""),
            "score_breakdown": score_summary,
            "score_breakdown_display": str(score_summary.get("score_breakdown_display") or ""),
            "pricing_basis_label": entry_pricing["pricing_basis_label"],
            "pricing_math_display": entry_pricing["pricing_math_display"],
            "entry_gate_label": "Talos accepts Apollo when a qualified Apollo candidate clears governance and has the highest deterministic score.",
            "trade_payload": payload,
        }

    def _build_kairos_candidate(self, account_balance: float) -> Dict[str, Any] | None:
        service = KairosService(
            market_data_service=self.market_data_service,
            options_chain_service=self.options_chain_service,
            config=replace(self.config, apollo_account_value=max(account_balance, 1.0)),
            scenario_repository=self.scenario_repository,
            trade_store=self.trade_store,
        )
        if hasattr(service, "initialize_live_kairos_on_page_load"):
            dashboard = service.initialize_live_kairos_on_page_load()
        else:
            dashboard = service.get_dashboard_payload()
        best_trade = (dashboard or {}).get("best_trade_override") or {}
        candidate = best_trade.get("candidate") if isinstance(best_trade, dict) else None
        if best_trade.get("status") != "ready" or not isinstance(candidate, dict):
            return None
        mode_key = str(candidate.get("mode_key") or "").strip().lower()
        if mode_key not in {"prime", "window-open"}:
            return None
        score_summary = self._score_candidate(
            system_name="Kairos",
            candidate_profile="Prime",
            em_multiple=self._coerce_float(candidate.get("actual_em_multiple") or candidate.get("daily_move_multiple")),
            distance_points=self._coerce_float(candidate.get("actual_distance_to_short") or candidate.get("distance_points")),
            short_delta=self._coerce_float(candidate.get("estimated_short_delta")),
            expected_move=self._coerce_float(candidate.get("expected_move_used")),
            credit=self._coerce_float(candidate.get("credit_estimate_dollars")) / 100.0 if self._coerce_float(candidate.get("credit_estimate_dollars")) is not None else None,
            spread_width=self._coerce_float(candidate.get("spread_width")),
            max_loss=self._coerce_float(candidate.get("max_loss_dollars")),
            total_premium=(
                max(self._coerce_float(candidate.get("conservative_credit_dollars")) or self._coerce_float(candidate.get("credit_estimate_dollars")) or 0.0, 0.0)
                * float(candidate.get("recommended_contracts") or 1)
            ),
            raw_candidate_score=self._coerce_float(candidate.get("fit_score") or candidate.get("score")),
            structure_context=(dashboard or {}).get("current_structure_status") or (best_trade or {}).get("structure_status"),
            macro_context=(dashboard or {}).get("current_timing_status") or candidate.get("mode_descriptor"),
        )
        spx_snapshot = self.market_data_service.get_latest_snapshot("^GSPC", query_type="talos_kairos_spx") or {}
        vix_snapshot = self.market_data_service.get_latest_snapshot("^VIX", query_type="talos_kairos_vix") or {}
        entry_pricing = self._resolve_kairos_entry_pricing(candidate)
        credit_estimate_dollars = float(candidate.get("credit_estimate_dollars") or 0.0)
        session_profile = "Prime"
        actual_entry_credit = entry_pricing["actual_entry_credit"]
        premium_per_contract = round(actual_entry_credit * 100.0, 2)
        contracts = int(candidate.get("recommended_contracts") or 1)
        payload = {
            "trade_mode": "talos",
            "system_name": "Kairos",
            "journal_name": "Horme",
            "system_version": self._version_number(),
            "candidate_profile": session_profile,
            "status": "open",
            "trade_date": current_timestamp().split("T", 1)[0],
            "entry_datetime": current_timestamp(),
            "expiration_date": current_timestamp().split("T", 1)[0],
            "underlying_symbol": "SPX",
            "spx_at_entry": spx_snapshot.get("Latest Value") or "",
            "vix_at_entry": vix_snapshot.get("Latest Value") or "",
            "structure_grade": session_profile,
            "macro_grade": "Improving",
            "expected_move": candidate.get("expected_move_used") or "",
            "expected_move_used": candidate.get("expected_move_used") or "",
            "expected_move_source": candidate.get("expected_move_source") or "same_day_atm_straddle",
            "option_type": "Put Credit Spread",
            "short_strike": candidate.get("short_strike") or "",
            "long_strike": candidate.get("long_strike") or "",
            "spread_width": candidate.get("spread_width") or "",
            "contracts": contracts,
            "candidate_credit_estimate": round((credit_estimate_dollars / 100.0), 4) if credit_estimate_dollars else "",
            "actual_entry_credit": actual_entry_credit,
            "net_credit_per_contract": actual_entry_credit,
            "distance_to_short": candidate.get("distance_points") or "",
            "em_multiple_floor": candidate.get("em_multiple_floor") or "",
            "percent_floor": candidate.get("percent_floor") or "",
            "boundary_rule_used": candidate.get("boundary_rule_used") or "",
            "actual_distance_to_short": candidate.get("actual_distance_to_short") or candidate.get("distance_points") or "",
            "actual_em_multiple": candidate.get("actual_em_multiple") or candidate.get("daily_move_multiple") or "",
            "fallback_used": "yes" if candidate.get("fallback_used") else "no",
            "fallback_rule_name": candidate.get("fallback_rule_name") or "",
            "short_delta": candidate.get("estimated_short_delta") or "",
            "pass_type": candidate.get("mode_key") or "",
            "premium_per_contract": premium_per_contract,
            "total_premium": round(premium_per_contract * contracts, 2),
            "max_theoretical_risk": candidate.get("max_loss_dollars") or "",
            "risk_efficiency": "",
            "target_em": candidate.get("daily_move_multiple") or "",
            "notes_entry": self._build_decision_note(
                system_name="Kairos",
                descriptor=candidate.get("mode_descriptor") or candidate.get("mode_key") or "candidate",
                account_balance=account_balance,
                candidate_score=float(score_summary.get("candidate_score") or 0.0),
                details=(
                    f"Distance {candidate.get('distance_points') or 'n/a'} pts; EM {candidate.get('daily_move_multiple') or 'n/a'}x. "
                    f"Entry fill {entry_pricing['pricing_math_display']}. Talos only admits Kairos Prime / Window Open candidates. "
                    f"{score_summary.get('score_breakdown_display') or ''}"
                ).strip(),
            ),
            "prefill_source": "talos-kairos",
            "automation_status": "autonomous-entry-and-management",
            "close_method": "",
            "close_reason": "",
            "notes_exit": "",
        }
        decision_note = str(payload.get("notes_entry") or "")
        return {
            "candidate_score": float(score_summary.get("candidate_score") or 0.0),
            "decision_note": decision_note,
            "selection_basis": str(score_summary.get("selection_basis") or ""),
            "score_breakdown": score_summary,
            "score_breakdown_display": str(score_summary.get("score_breakdown_display") or ""),
            "pricing_basis_label": entry_pricing["pricing_basis_label"],
            "pricing_math_display": entry_pricing["pricing_math_display"],
            "entry_gate_label": "Talos accepts Kairos only when the best-trade override is Ready and the mode is Prime or Window Open.",
            "trade_payload": payload,
        }

    def _resolve_apollo_entry_pricing(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        executable_credit = self._coerce_float(candidate.get("executable_credit"))
        if executable_credit is None:
            executable_credit = max(self._coerce_float(candidate.get("credit")) or 0.0, 0.0)
        executable_credit = round(max(executable_credit, 0.0), 4)
        short_bid = self._coerce_float((candidate.get("short_put") or {}).get("bid"))
        long_ask = self._coerce_float((candidate.get("long_put") or {}).get("ask"))
        if short_bid is not None and long_ask is not None:
            pricing_math_display = f"short bid {short_bid:.2f} - long ask {long_ask:.2f} = {executable_credit:.2f}"
        else:
            pricing_math_display = f"executable credit {executable_credit:.2f}"
        return {
            "actual_entry_credit": executable_credit,
            "pricing_basis_label": "Conservative executable entry credit",
            "pricing_math_display": pricing_math_display,
        }

    def _resolve_kairos_entry_pricing(self, candidate: Dict[str, Any]) -> Dict[str, Any]:
        conservative_credit_dollars = self._coerce_float(candidate.get("conservative_credit_dollars"))
        if conservative_credit_dollars is None:
            conservative_credit_dollars = max(self._coerce_float(candidate.get("credit_estimate_dollars")) or 0.0, 0.0)
        conservative_credit_dollars = round(max(conservative_credit_dollars, 0.0), 2)
        short_bid = self._coerce_float(candidate.get("short_leg_bid"))
        long_ask = self._coerce_float(candidate.get("long_leg_ask"))
        actual_entry_credit = round(conservative_credit_dollars / 100.0, 4)
        if short_bid is not None and long_ask is not None:
            pricing_math_display = f"short bid {short_bid:.2f} - long ask {long_ask:.2f} = {actual_entry_credit:.2f}"
        else:
            pricing_math_display = f"conservative credit {actual_entry_credit:.2f}"
        return {
            "actual_entry_credit": actual_entry_credit,
            "pricing_basis_label": "Conservative executable entry credit",
            "pricing_math_display": pricing_math_display,
        }

    def _build_decision_note(
        self,
        *,
        system_name: str,
        descriptor: str,
        account_balance: float,
        candidate_score: float,
        details: str,
    ) -> str:
        return (
            f"Talos auto-entered this {system_name} setup from the {descriptor} slot using account balance {self._format_currency(account_balance)}. "
            f"Score {candidate_score:.1f}. {details} Autonomous management remains enabled."
        )

    def _compute_account_balance(self, open_records: List[Dict[str, Any]]) -> float:
        return self._compute_balance_components(open_records)["settled_balance"]

    def _compute_balance_components(self, open_records: List[Dict[str, Any]]) -> Dict[str, float]:
        realized_pnl = 0.0
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status not in {"open", "reduced"}:
                realized_pnl += float(trade.get("gross_pnl") or 0.0)
        unrealized_pnl = sum(float(item.get("current_pl") or 0.0) for item in open_records)
        starting_balance = float(self._load_state().get("starting_balance") or self.STARTING_BALANCE)
        settled_balance = starting_balance + realized_pnl
        current_account_value = settled_balance + unrealized_pnl
        return {
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "settled_balance": settled_balance,
            "equity_balance": settled_balance,
            "current_account_value": current_account_value,
        }

    def _filter_talos_records(self, management_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            item
            for item in (management_payload.get("records") or [])
            if str(item.get("trade_mode") or "").strip().lower() == "talos"
        ]

    def _sync_trade_metadata(self, state: Dict[str, Any], open_records: List[Dict[str, Any]]) -> bool:
        active_ids = {str(int(item.get("trade_id") or 0)) for item in open_records if int(item.get("trade_id") or 0) > 0}
        metadata = dict(state.get("trade_metadata") or {})
        changed = False
        for key in list(metadata.keys()):
            if key not in active_ids:
                metadata.pop(key, None)
                changed = True
        state["trade_metadata"] = metadata
        return changed

    def _default_state(self, *, starting_balance: float | None = None, last_archive_path: str = "") -> Dict[str, Any]:
        return {
            "paused": False,
            "starting_balance": float(starting_balance if starting_balance is not None else self.STARTING_BALANCE),
            "last_cycle_at": "",
            "last_cycle_reason": "",
            "last_cycle_status": "idle",
            "last_scan_at": "",
            "next_scan_at": "",
            "scan_engine_running": False,
            "total_scan_count": 0,
            "last_archive_path": last_archive_path,
            "activity_log": [],
            "skip_log": [],
            "trade_metadata": {},
        }

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return self._default_state()
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return self._default_state()
        if not isinstance(payload, dict):
            return self._default_state()
        state = self._default_state()
        state.update(payload)
        return state

    def _save_state(self, state: Dict[str, Any]) -> None:
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")

    def _append_activity(self, state: Dict[str, Any], item: Dict[str, Any]) -> None:
        activity_log = [entry for entry in (state.get("activity_log") or []) if isinstance(entry, dict)]
        activity_log.insert(0, item)
        state["activity_log"] = activity_log[: self.MAX_ACTIVITY_ITEMS]

    def _append_skip(self, state: Dict[str, Any], item: Dict[str, Any]) -> None:
        skip_log = [entry for entry in (state.get("skip_log") or []) if isinstance(entry, dict)]
        if skip_log:
            latest = skip_log[0]
            latest_reason = str(latest.get("reason") or "").strip()
            latest_system = str(latest.get("system") or "").strip()
            item_reason = str(item.get("reason") or "").strip()
            item_system = str(item.get("system") or "").strip()
            if latest_reason == item_reason and latest_system == item_system:
                latest["repeat_count"] = int(latest.get("repeat_count") or 1) + 1
                latest["latest_timestamp"] = item.get("timestamp") or latest.get("latest_timestamp") or latest.get("timestamp")
                latest.setdefault("first_timestamp", latest.get("timestamp") or item.get("timestamp") or "")
                latest["timestamp"] = item.get("timestamp") or latest.get("timestamp") or ""
                state["skip_log"] = skip_log[: self.MAX_SKIP_ITEMS]
                return
        item = dict(item)
        item.setdefault("repeat_count", 1)
        item.setdefault("first_timestamp", item.get("timestamp") or "")
        item.setdefault("latest_timestamp", item.get("timestamp") or "")
        skip_log.insert(0, item)
        state["skip_log"] = skip_log[: self.MAX_SKIP_ITEMS]

    def _is_market_window(self, now: datetime) -> bool:
        local_now = now.astimezone(self.display_timezone)
        if not self.market_calendar_service.is_tradable_market_day(local_now.date()):
            return False
        current_time = local_now.time()
        return self.MARKET_OPEN <= current_time <= self.MARKET_CLOSE

    def _resolve_cycle_mode(self, now: datetime) -> str:
        local_now = now.astimezone(self.display_timezone)
        if not self.market_calendar_service.is_tradable_market_day(local_now.date()):
            return "closed"
        current_time = local_now.time()
        final_scan_cutoff = self._final_post_close_time(local_now.date())
        if self.MARKET_OPEN <= current_time <= self.MARKET_CLOSE:
            return "market"
        if self.MARKET_CLOSE < current_time <= final_scan_cutoff:
            return "post-close-final"
        return "closed"

    def _resolve_next_background_run_at(self, now: datetime) -> datetime | None:
        local_now = now.astimezone(self.display_timezone)
        session_date = self.market_calendar_service.get_next_or_same_tradable_market_day(local_now.date())
        market_open_at = self._combine_market_time(session_date, self.MARKET_OPEN)
        market_close_at = self._combine_market_time(session_date, self.MARKET_CLOSE)
        final_scan_at = market_close_at + timedelta(minutes=self.POST_CLOSE_FINAL_SCAN_MINUTES)
        if session_date != local_now.date() or not self.market_calendar_service.is_tradable_market_day(local_now.date()):
            return market_open_at
        if local_now < market_open_at:
            return market_open_at
        if local_now < market_close_at:
            return local_now + timedelta(seconds=self.BACKGROUND_INTERVAL_SECONDS)
        if local_now < final_scan_at:
            return final_scan_at
        next_session = self.market_calendar_service.get_next_or_same_tradable_market_day(local_now.date() + timedelta(days=1))
        return self._combine_market_time(next_session, self.MARKET_OPEN)

    def _final_post_close_time(self, session_date: date) -> time:
        return (datetime.combine(session_date, self.MARKET_CLOSE, tzinfo=self.display_timezone) + timedelta(minutes=self.POST_CLOSE_FINAL_SCAN_MINUTES)).time()

    def _combine_market_time(self, session_date: date, session_time: time) -> datetime:
        return datetime.combine(session_date, session_time, tzinfo=self.display_timezone)

    def _build_slot_governance(self, open_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        system_counts = Counter(normalize_system_name(item.get("system_name")) for item in open_records)
        return {
            "total_open_count": len(open_records),
            "system_counts": dict(system_counts),
        }

    def _build_slot_governance_from_trade_store(self) -> Dict[str, Any]:
        live_trades = []
        for trade in self.trade_store.list_trades("talos"):
            status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
            if status in {"open", "reduced"}:
                live_trades.append(trade)
        return self._build_slot_governance(live_trades)

    def _should_evaluate_candidate(self, system_name: str, governance: Dict[str, Any]) -> bool:
        system_count = int((governance.get("system_counts") or {}).get(system_name, 0))
        if system_count > int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1)):
            return False
        if system_count > 0:
            return True
        return int(governance.get("total_open_count") or 0) < self.MAX_TOTAL_OPEN_TRADES

    def _can_open_trade_for_system(self, system_name: str, governance: Dict[str, Any]) -> bool:
        system_count = int((governance.get("system_counts") or {}).get(system_name, 0))
        total_open_count = int(governance.get("total_open_count") or 0)
        if total_open_count >= self.MAX_TOTAL_OPEN_TRADES:
            return False
        return system_count < int(self.MAX_OPEN_TRADES_PER_SYSTEM.get(system_name, 1))

    def _archive_runtime_snapshot(self, state: Dict[str, Any], talos_trades: List[Dict[str, Any]]) -> Path:
        self.archive_directory.mkdir(parents=True, exist_ok=True)
        stamp = self._now().strftime("%Y%m%d_%H%M%S")
        archive_path = self.archive_directory / f"talos_reset_{stamp}.json"
        archive_payload = {
            "archived_at": self._now().isoformat(),
            "state": state,
            "talos_trades": talos_trades,
        }
        archive_path.write_text(json.dumps(archive_payload, indent=2, sort_keys=True), encoding="utf-8")
        return archive_path

    def _score_candidate(
        self,
        *,
        system_name: str,
        candidate_profile: str,
        em_multiple: float | None,
        distance_points: float | None,
        short_delta: float | None,
        expected_move: float | None,
        credit: float | None,
        spread_width: float | None,
        max_loss: float | None,
        total_premium: float | None,
        raw_candidate_score: float | None,
        structure_context: Any,
        macro_context: Any,
    ) -> Dict[str, Any]:
        history = self._build_history_profile(system_name, candidate_profile)
        safety_component = self._score_safety_component(
            system_name=system_name,
            em_multiple=em_multiple,
            distance_points=distance_points,
            expected_move=expected_move,
            short_delta=short_delta,
        )
        credit_component = self._score_credit_efficiency_component(credit, spread_width)
        profit_component = self._score_profit_opportunity_component(total_premium)
        fit_component = self._fit_score(system_name=system_name, structure_context=structure_context, macro_context=macro_context) * 10.0
        history_component = history["history_component"]
        loss_penalty_component = history["loss_penalty_component"]
        candidate_score = max(
            0.0,
            min(
                100.0,
                round(
                    safety_component
                    + history_component
                    + credit_component
                    + profit_component
                    + fit_component
                    - loss_penalty_component,
                    2,
                ),
            ),
        )
        score_breakdown_display = (
            f"Score {candidate_score:.1f} = Safety {safety_component:.1f} + History {history_component:.1f} + "
            f"Credit {credit_component:.1f} + Profit {profit_component:.1f} + Fit {fit_component:.1f} - "
            f"Loss Penalty {loss_penalty_component:.1f}."
        )
        return {
            "candidate_score": candidate_score,
            "selection_basis": (
                f"Talos used deterministic weighted scoring for {system_name} {candidate_profile}: Safety(0-40) + History(0-25) + "
                f"CreditEfficiency(0-15) + ProfitOpportunity(0-10) + Fit(0-10) - LossPenalty(0-20)."
            ),
            "score_breakdown_display": score_breakdown_display,
            "history_scope": history["scope"],
            "history_sample_count": history["sample_count"],
            "safety_component": round(safety_component, 2),
            "history_component": round(history_component, 2),
            "credit_component": round(credit_component, 2),
            "profit_component": round(profit_component, 2),
            "fit_component": round(fit_component, 2),
            "loss_penalty_component": round(loss_penalty_component, 2),
            "win_rate_component": round(history["win_rate_component"], 2),
            "expectancy_component": round(history["expectancy_component"], 2),
            "sample_confidence_component": round(history["sample_confidence_component"], 2),
            "em_multiple_component": round(self._score_em_multiple_component(em_multiple), 2),
            "distance_component": round(self._score_distance_component(system_name, distance_points, expected_move, em_multiple), 2),
            "short_delta_component": round(self._score_short_delta_component(short_delta), 2),
            "raw_score": float(raw_candidate_score or 0.0),
        }

    def _build_history_profile(self, system_name: str, candidate_profile: str) -> Dict[str, Any]:
        scoped_records = self._filter_history_records(system_name=system_name, candidate_profile=candidate_profile)
        scope = "profile"
        if not scoped_records:
            scoped_records = self._filter_history_records(system_name=system_name, candidate_profile=None)
            scope = "system"
        if not scoped_records:
            return {
                "scope": "neutral",
                "sample_count": 0,
                "win_rate_component": 6.0,
                "expectancy_component": 4.0,
                "sample_confidence_component": 0.0,
                "history_component": 10.0,
                "average_loss_severity": 0.15,
                "black_swan_rate": 0.0,
                "loss_penalty_component": 3.0,
            }
        sample_count = len(scoped_records)
        winners = [item for item in scoped_records if float(item.get("gross_pnl") or 0.0) > 0.0]
        losers = [item for item in scoped_records if float(item.get("gross_pnl") or 0.0) < 0.0]
        win_rate = len(winners) / sample_count
        average_win = self._average(item.get("gross_pnl") for item in winners)
        average_loss = abs(self._average(item.get("gross_pnl") for item in losers))
        loss_events = losers
        loss_rate = (len(loss_events) / sample_count) if sample_count else 0.0
        expectancy = calculate_expectancy(
            win_rate=win_rate,
            average_win=average_win,
            loss_rate=loss_rate,
            average_loss=average_loss,
        )
        black_swan_rate = len([item for item in scoped_records if str(item.get("win_loss_result") or "").strip().lower() == "black swan"]) / sample_count
        if losers:
            average_loss_severity = sum(self._loss_severity(item) for item in losers) / len(losers)
        else:
            average_loss_severity = 0.0
        win_rate_component = 12.0 * max(0.0, min((win_rate - 0.35) / 0.45, 1.0))
        expectancy_component = 8.0 * max(0.0, min((expectancy + 25.0) / 150.0, 1.0))
        sample_confidence_component = self._sample_confidence_component(sample_count)
        history_component = win_rate_component + expectancy_component + sample_confidence_component
        loss_penalty_component = min(20.0, (12.0 * average_loss_severity) + (8.0 * black_swan_rate))
        return {
            "scope": scope,
            "sample_count": sample_count,
            "win_rate_component": win_rate_component,
            "expectancy_component": expectancy_component,
            "sample_confidence_component": sample_confidence_component,
            "history_component": history_component,
            "average_loss_severity": average_loss_severity,
            "black_swan_rate": black_swan_rate,
            "loss_penalty_component": loss_penalty_component,
        }

    def _score_safety_component(
        self,
        *,
        system_name: str,
        em_multiple: float | None,
        distance_points: float | None,
        expected_move: float | None,
        short_delta: float | None,
    ) -> float:
        return (
            self._score_em_multiple_component(em_multiple)
            + self._score_distance_component(system_name, distance_points, expected_move, em_multiple)
            + self._score_short_delta_component(short_delta)
        )

    @staticmethod
    def _score_em_multiple_component(em_multiple: float | None) -> float:
        value = float(em_multiple or 0.0)
        if value >= 2.0:
            return 18.0
        if value >= 1.8:
            return 16.0
        if value >= 1.6:
            return 13.0
        if value >= 1.4:
            return 9.0
        if value >= 1.2:
            return 5.0
        return 0.0

    def _score_distance_component(
        self,
        system_name: str,
        distance_points: float | None,
        expected_move: float | None,
        em_multiple: float | None,
    ) -> float:
        distance_value = max(float(distance_points or 0.0), 0.0)
        target_distance = self._resolve_target_distance(system_name, distance_value, expected_move, em_multiple)
        if target_distance <= 0:
            return 0.0
        return min(14.0, max(0.0, (distance_value / target_distance) * 14.0))

    def _resolve_target_distance(self, system_name: str, distance_points: float, expected_move: float | None, em_multiple: float | None) -> float:
        normalized_system = normalize_system_name(system_name)
        target_em_multiple = 1.8 if normalized_system == "Apollo" else 1.5
        expected_move_value = self._coerce_float(expected_move)
        em_multiple_value = self._coerce_float(em_multiple)
        if expected_move_value is None and em_multiple_value not in {None, 0.0} and distance_points > 0:
            expected_move_value = distance_points / float(em_multiple_value)
        if expected_move_value is None:
            return 55.0 if normalized_system == "Apollo" else 32.0
        return max(expected_move_value * target_em_multiple, 1.0)

    @staticmethod
    def _score_short_delta_component(short_delta: float | None) -> float:
        value = abs(float(short_delta or 0.0))
        if value <= 0.08:
            return 8.0
        if value <= 0.12:
            return 6.0
        if value <= 0.16:
            return 4.0
        if value <= 0.20:
            return 2.0
        return 0.0

    @staticmethod
    def _score_credit_efficiency_component(credit: float | None, spread_width: float | None) -> float:
        if credit in {None, 0.0} or spread_width in {None, 0.0}:
            return 0.0
        ratio = max(float(credit) / float(spread_width), 0.0)
        return min(15.0, (ratio / 0.20) * 15.0)

    @staticmethod
    def _score_profit_opportunity_component(total_premium: float | None) -> float:
        if total_premium in {None, 0.0}:
            return 0.0
        normalized = max(float(total_premium), 0.0)
        return min(10.0, ((normalized / 600.0) ** 0.5) * 10.0)

    @staticmethod
    def _sample_confidence_component(sample_count: int) -> float:
        if sample_count >= 20:
            return 5.0
        if sample_count >= 12:
            return 4.0
        if sample_count >= 8:
            return 3.0
        if sample_count >= 4:
            return 2.0
        if sample_count >= 2:
            return 1.0
        return 0.0

    def _filter_history_records(self, *, system_name: str, candidate_profile: str | None) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for trade_mode in ("real", "simulated"):
            for trade in self.trade_store.list_trades(trade_mode):
                status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
                if status in {"open", "reduced"}:
                    continue
                if normalize_system_name(trade.get("system_name")) != system_name:
                    continue
                if candidate_profile is not None:
                    profile_value = str(trade.get("candidate_profile") or trade.get("pass_type") or "").strip().lower()
                    if profile_value != str(candidate_profile).strip().lower():
                        continue
                records.append(trade)
        return records

    def _loss_severity(self, trade: Dict[str, Any]) -> float:
        gross_pnl = abs(float(trade.get("gross_pnl") or 0.0))
        max_loss = self._coerce_float(trade.get("max_theoretical_risk") or trade.get("max_loss") or trade.get("max_theoretical_loss"))
        if max_loss in {None, 0.0}:
            return self._clamp_ratio(gross_pnl, ceiling=1000.0)
        return min(1.0, gross_pnl / max_loss)

    def _fit_score(self, *, system_name: str, structure_context: Any, macro_context: Any) -> float:
        if system_name == "Kairos":
            structure_score = self._qualitative_score(structure_context, good_terms=("bullish", "confirm", "developing", "improving", "prime"), poor_terms=("failed", "weakening", "expired", "broken"), neutral=0.6)
            macro_score = self._qualitative_score(macro_context, good_terms=("prime", "window-open", "improving", "ready"), poor_terms=("late", "weak", "failed"), neutral=0.65)
            return min(1.0, (0.65 * structure_score) + (0.35 * macro_score))
        structure_score = self._qualitative_score(structure_context, good_terms=("good", "allowed", "healthy", "balanced"), poor_terms=("poor", "stand aside", "blocked", "risk-off"), neutral=0.65)
        macro_score = self._qualitative_score(macro_context, good_terms=("none", "minor", "neutral"), poor_terms=("major", "high", "blocked", "risk"), neutral=0.6)
        return min(1.0, (0.7 * structure_score) + (0.3 * macro_score))

    def _qualitative_score(self, value: Any, *, good_terms: tuple[str, ...], poor_terms: tuple[str, ...], neutral: float) -> float:
        text = str(value or "").strip().lower()
        if not text:
            return neutral
        if any(term in text for term in poor_terms):
            return 0.25
        if any(term in text for term in good_terms):
            return 0.9
        return neutral

    @staticmethod
    def _clamp_ratio(value: float | None, *, ceiling: float) -> float:
        if value is None or ceiling <= 0:
            return 0.0
        return max(0.0, min(float(value) / ceiling, 1.0))

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _format_market_day(self, value: Any) -> str:
        if value is None:
            return ""
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _average(values) -> float:
        items = [float(value) for value in values if value not in {None, ""}]
        if not items:
            return 0.0
        return sum(items) / len(items)

    def _version_number(self) -> str:
        return str(self.config.app_version_label or "Version 7.1").replace("Version", "").strip() or "7.1"

    def _format_currency(self, value: float) -> str:
        return f"${value:,.2f}"

    def _format_datetime(self, value: Any) -> str:
        if not value:
            return "Not yet evaluated"
        if isinstance(value, datetime):
            stamp = value
        else:
            stamp = datetime.fromisoformat(str(value))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=self.display_timezone)
        return stamp.astimezone(self.display_timezone).strftime("%Y-%m-%d %I:%M:%S %p %Z")

    def _now(self) -> datetime:
        return self.now_provider()
