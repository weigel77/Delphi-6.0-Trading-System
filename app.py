"""Flask entry point for the local SPX and VIX market lookup tool."""

from __future__ import annotations

import atexit
import logging
import json
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime
from logging.handlers import RotatingFileHandler
from threading import Timer
from typing import Any, Dict, Optional
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo
import webbrowser

import pandas as pd
from flask import Flask, abort, current_app, jsonify, redirect, render_template, request, send_file, session, url_for

from config import get_app_config
from services import (
    ApolloService,
    ExportService,
    KairosService,
    MarketDataAuthenticationError,
    MarketDataError,
    MarketDataReauthenticationRequired,
    MarketDataService,
    OpenTradeManager,
    PerformanceDashboardService,
    PerformanceEngine,
    PushoverService,
)
from services.performance_dashboard_service import PERFORMANCE_FILTER_GROUPS
from services.kairos_scenario_repository import KairosScenarioRepository, get_persistent_data_dir
from services.trade_importer import parse_trade_import
from services.trade_store import (
    DEFAULT_CANDIDATE_PROFILE,
    JOURNAL_NAME_DEFAULT,
    TradeStore,
    blank_trade_form,
    classify_closed_trade_outcome,
    build_trade_duplicate_signature,
    current_timestamp,
    form_trade_record,
    normalize_candidate_profile,
    normalize_expected_move_source,
    resolve_trade_credit_model,
    resolve_trade_distance,
    resolve_trade_expected_move,
    summarize_trade_close_events,
    normalize_trade_mode,
    normalize_system_name,
)

APP_CONFIG = get_app_config()
CHICAGO_TZ = ZoneInfo(APP_CONFIG.app_timezone)
APP_HOST = APP_CONFIG.app_host
APP_PORT = APP_CONFIG.app_port
LOCAL_DEV_HOSTS = {"127.0.0.1", "localhost"}


@dataclass(frozen=True)
class QueryDefinition:
    """Metadata describing a supported query."""

    key: str
    label: str
    action: str
    ticker: str
    symbol: str
    date_mode: str


@dataclass(frozen=True)
class QueryRequest:
    """Normalized query input from the form."""

    definition: QueryDefinition
    single_date: Optional[date] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ValidationError(Exception):
    """Raised when the incoming form data is invalid."""


QUERY_DEFINITIONS = [
    QueryDefinition("latest_spx", "Latest SPX data", "latest", "^GSPC", "SPX", "none"),
    QueryDefinition("spx_close_range", "SPX closing values for a date range", "range", "^GSPC", "SPX", "range"),
    QueryDefinition("spx_change_date", "SPX daily change for a single date", "single_change", "^GSPC", "SPX", "single"),
    QueryDefinition("spx_change_range", "SPX daily change for a date range", "range", "^GSPC", "SPX", "range"),
    QueryDefinition("latest_vix", "Latest VIX data", "latest", "^VIX", "VIX", "none"),
    QueryDefinition("vix_close_date", "VIX closing value for a single date", "single_close", "^VIX", "VIX", "single"),
    QueryDefinition("vix_close_range", "VIX closing values for a date range", "range", "^VIX", "VIX", "range"),
]
QUERY_LOOKUP = {definition.key: definition for definition in QUERY_DEFINITIONS}
TRADE_MODE_LABELS = {"real": "Real Trades", "simulated": "Simulated Trades"}
TRADE_MODE_DESCRIPTIONS = {
    "real": "Persistent live-trade log for real-world positions.",
    "simulated": "Persistent paper-trade log for simulated execution review.",
}
TRADE_STATUS_OPTIONS = ["open", "closed", "expired", "cancelled"]
TRADE_PROFILE_OPTIONS = ["Legacy", "Aggressive", "Fortress", "Standard"]
TRADE_OPTION_TYPE_OPTIONS = ["Put Credit Spread", "Call Credit Spread"]
TRADE_SYSTEM_OPTIONS = ["Apollo", "Kairos", "Aegis"]
TRADE_JOURNAL_OPTIONS = [JOURNAL_NAME_DEFAULT]
APOLLO_PREFILL_SESSION_KEY = "apollo_trade_prefill"
MANAGEMENT_CLOSE_PREFILL_SESSION_KEY = "management_close_prefill"
TRADE_IMPORT_PREVIEW_DIRNAME = "trade_import_previews"
TRADE_FORM_FIELDS = [
    "trade_number",
    "trade_mode",
    "system_name",
    "journal_name",
    "system_version",
    "candidate_profile",
    "status",
    "trade_date",
    "entry_datetime",
    "expiration_date",
    "underlying_symbol",
    "spx_at_entry",
    "vix_at_entry",
    "structure_grade",
    "macro_grade",
    "expected_move",
    "expected_move_used",
    "expected_move_source",
    "option_type",
    "short_strike",
    "long_strike",
    "spread_width",
    "contracts",
    "candidate_credit_estimate",
    "actual_entry_credit",
    "distance_to_short",
    "em_multiple_floor",
    "percent_floor",
    "boundary_rule_used",
    "actual_distance_to_short",
    "actual_em_multiple",
    "pass_type",
    "premium_per_contract",
    "total_premium",
    "max_theoretical_risk",
    "risk_efficiency",
    "target_em",
    "fallback_used",
    "fallback_rule_name",
    "short_delta",
    "notes_entry",
    "prefill_source",
    "exit_datetime",
    "spx_at_exit",
    "actual_exit_value",
    "close_method",
    "close_reason",
    "notes_exit",
]
TRADE_FILTER_GROUPS = {
    "system": ["Apollo", "Kairos", "Aegis"],
    "profile": ["Legacy", "Aggressive", "Fortress", "Standard"],
    "result": ["Win", "Loss", "Black Swan"],
}


def build_oauth_session_keys(namespace: str) -> Dict[str, str]:
    """Return the environment-specific session keys used by the Schwab OAuth flow."""
    prefix = str(namespace or "delphi").strip().lower() or "delphi"
    return {
        "oauth_state": f"{prefix}_oauth_state",
        "pkce_verifier": f"{prefix}_pkce_verifier",
        "login_in_progress": f"{prefix}_login_in_progress",
        "connected": f"{prefix}_connected",
        "authorized": f"{prefix}_authorized",
        "callback_pending": f"{prefix}_callback_pending",
    }


def mask_oauth_state(value: Any) -> str:
    """Return a short non-secret representation of the OAuth state token."""
    text = str(value or "").strip()
    if not text:
        return "missing"
    if len(text) <= 10:
        return text
    return f"{text[:6]}...{text[-4:]}"


def _build_kairos_replay_legacy_dirs(app: Flask) -> list[Path]:
    storage_dir = Path(app.config["KAIROS_REPLAY_STORAGE_DIR"]).resolve()
    legacy_candidates = [Path(app.instance_path) / "kairos_replays"]
    if not app.config.get("TESTING"):
        workspace_root = Path(__file__).resolve().parents[2]
        legacy_candidates.extend(
            [
                workspace_root / "instance" / "kairos_replays",
                get_persistent_data_dir() / "kairos_replays",
            ]
        )
    legacy_dirs: list[Path] = []
    seen_dirs: set[Path] = set()
    for candidate in legacy_candidates:
        resolved_candidate = candidate.resolve()
        if resolved_candidate == storage_dir or resolved_candidate in seen_dirs:
            continue
        seen_dirs.add(resolved_candidate)
        legacy_dirs.append(candidate)
    return legacy_dirs


def create_app(test_config: Optional[Dict[str, Any]] = None) -> Flask:
    """Application factory."""
    app = Flask(__name__, instance_relative_config=True)
    app.secret_key = APP_CONFIG.flask_secret_key
    app.config["SESSION_COOKIE_NAME"] = APP_CONFIG.session_cookie_name
    app.config["OAUTH_SESSION_NAMESPACE"] = APP_CONFIG.oauth_session_namespace
    app.config["APP_DISPLAY_NAME"] = APP_CONFIG.app_display_name
    app.config["APP_PAGE_KICKER"] = APP_CONFIG.app_page_kicker
    app.config["APP_VERSION_LABEL"] = APP_CONFIG.app_version_label
    if test_config:
        app.config.update(test_config)
    app.config.setdefault("TRADE_DATABASE", str(Path(app.instance_path) / "horme_trades.db"))
    app.config.setdefault(
        "KAIROS_REPLAY_STORAGE_DIR",
        APP_CONFIG.kairos_replay_storage_dir or str(get_persistent_data_dir() / "kairos_replays"),
    )
    configure_logging(app)

    market_data_service = MarketDataService(config=APP_CONFIG)
    export_service = ExportService()
    apollo_service = ApolloService(market_data_service=market_data_service, config=APP_CONFIG)
    kairos_scenario_repository = KairosScenarioRepository(
        app.config["KAIROS_REPLAY_STORAGE_DIR"],
        legacy_storage_dirs=_build_kairos_replay_legacy_dirs(app),
    )
    trade_store = TradeStore(app.config["TRADE_DATABASE"], market_data_service=market_data_service)
    kairos_live_service = KairosService(
        market_data_service=market_data_service,
        config=APP_CONFIG,
        scenario_repository=kairos_scenario_repository,
        pushover_service=PushoverService(config=APP_CONFIG),
        trade_store=trade_store,
    )
    kairos_sim_service = KairosService(
        market_data_service=market_data_service,
        config=APP_CONFIG,
        replay_storage_dir=app.config["KAIROS_REPLAY_STORAGE_DIR"],
        scenario_repository=kairos_scenario_repository,
        trade_store=trade_store,
    )
    pushover_service = kairos_live_service.pushover_service or PushoverService(config=APP_CONFIG)
    kairos_sim_service.configure_runtime({"mode": "Simulation"})
    performance_service = PerformanceDashboardService(trade_store)
    performance_engine = PerformanceEngine(trade_store)
    trade_store.initialize()
    open_trade_manager = OpenTradeManager(
        trade_store=trade_store,
        market_data_service=market_data_service,
        apollo_service=apollo_service,
        options_chain_service=apollo_service.options_chain_service,
        pushover_service=pushover_service,
        kairos_service=kairos_live_service,
        config=APP_CONFIG,
    )
    open_trade_manager.initialize()
    app.extensions["trade_store"] = trade_store
    app.extensions["market_data_service"] = market_data_service
    app.extensions["apollo_service"] = apollo_service
    app.extensions["kairos_service"] = kairos_live_service
    app.extensions["kairos_live_service"] = kairos_live_service
    app.extensions["kairos_sim_service"] = kairos_sim_service
    app.extensions["kairos_scenario_repository"] = kairos_scenario_repository
    app.extensions["performance_service"] = performance_service
    app.extensions["performance_engine"] = performance_engine
    app.extensions["open_trade_manager"] = open_trade_manager
    app.extensions["pushover_service"] = pushover_service
    app.extensions["oauth_session_keys"] = build_oauth_session_keys(APP_CONFIG.oauth_session_namespace)

    if not app.config.get("TESTING"):
        open_trade_manager.start_background_monitoring()
        atexit.register(open_trade_manager.shutdown)
        atexit.register(kairos_live_service.shutdown)
        atexit.register(kairos_sim_service.shutdown)

    @app.context_processor
    def inject_universal_header_status() -> Dict[str, Any]:
        return {
            "menu_status": build_startup_menu_payload(market_data_service),
            "app_identity": {
                "display_name": app.config["APP_DISPLAY_NAME"],
                "page_kicker": app.config["APP_PAGE_KICKER"],
                "version_label": app.config["APP_VERSION_LABEL"],
                "session_cookie_name": app.config["SESSION_COOKIE_NAME"],
            },
        }

    @app.route("/", methods=["GET", "POST"])
    def index() -> str:
        if request.method == "POST":
            return app.view_functions["research"]()

        return render_template(
            "home.html",
            info_message=pop_status_message(),
        )

    def render_research_page(
        *,
        form_data: Dict[str, str],
        result: Optional[Dict[str, Any]],
        apollo_result: Optional[Dict[str, Any]],
        error_message: Optional[str],
        info_message: Optional[Dict[str, str]],
        diagnostic_message: Optional[str],
        active_page: str,
        page_browser_title: str,
        page_heading: str,
        page_copy: str,
    ) -> str:
        return render_template(
            "index.html",
            query_options=QUERY_DEFINITIONS,
            form_data=form_data,
            result=result,
            apollo_result=apollo_result,
            error_message=error_message,
            info_message=info_message,
            diagnostic_message=diagnostic_message,
            provider_meta=market_data_service.get_provider_metadata(),
            active_page=active_page,
            page_browser_title=page_browser_title,
            page_kicker="Delphi 2.0",
            page_heading=page_heading,
            page_copy=page_copy,
        )

    @app.route("/research", methods=["GET", "POST"])
    def research() -> str:
        form_data = get_form_data(request.form if request.method == "POST" else None)
        result = None
        error_message = None
        info_message = pop_status_message()
        diagnostic_message = None
        apollo_result = None
        query = None

        if request.method == "POST":
            try:
                query = parse_query_request(request.form)
                result = execute_query(query=query, service=market_data_service)
                form_data = get_form_data(request.form)
                app.logger.info("Completed query %s for %s", query.definition.key, query.definition.ticker)
            except MarketDataReauthenticationRequired as exc:
                set_status_message(str(exc), level="warning")
                app.logger.warning("Provider session expired: %s", exc)
                return redirect(url_for("login"))
            except MarketDataAuthenticationError as exc:
                error_message = str(exc)
                app.logger.warning("Provider login required: %s", exc)
            except ValidationError as exc:
                error_message = str(exc)
                app.logger.warning("Validation error: %s", exc)
            except MarketDataError as exc:
                error_message = str(exc)
                diagnostic_message = build_diagnostic_message(query, str(exc), market_data_service)
                app.logger.warning("Market data error: %s", exc)
            except Exception as exc:  # pragma: no cover - defensive logging
                error_message = "An unexpected error occurred while processing the request. Check the log for details."
                app.logger.exception("Unexpected query error: %s", exc)

        return render_research_page(
            form_data=form_data,
            result=result,
            apollo_result=apollo_result,
            error_message=error_message,
            info_message=info_message,
            diagnostic_message=diagnostic_message,
            active_page="research",
            page_browser_title="Research | Delphi",
            page_heading="Research",
            page_copy="Live and historical SPX / VIX research workspace with provider-aware routing and export-ready lookup tools.",
        )

    def get_request_payload() -> Dict[str, Any]:
        payload = request.get_json(silent=True)
        if payload is None:
            payload = request.form.to_dict(flat=True)
        return payload

    def normalize_kairos_workspace_payload(workspace: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        normalized_payload = dict(payload or {})
        if workspace == "live":
            normalized_payload["mode"] = "Live"
        elif workspace == "sim":
            normalized_payload["mode"] = "Simulation"
        return normalized_payload

    def render_kairos_workspace(*, workspace: str, service: KairosService) -> str:
        kairos_payload = service.initialize_live_kairos_on_page_load() if workspace == "live" else service.get_dashboard_payload()
        return render_template(
            "kairos.html",
            kairos_payload=kairos_payload,
            workspace=workspace,
            info_message=pop_status_message(),
        )

    @app.get("/kairos")
    def kairos_placeholder() -> Any:
        return redirect(url_for("kairos_live_page"))

    @app.get("/kairos/live")
    def kairos_live_page() -> str:
        return render_kairos_workspace(workspace="live", service=kairos_live_service)

    @app.get("/kairos/sim")
    def kairos_sim_page() -> str:
        return render_kairos_workspace(workspace="sim", service=kairos_sim_service)

    @app.get("/kairos/status")
    def kairos_status() -> Any:
        return jsonify(kairos_live_service.get_dashboard_payload())

    @app.get("/kairos/live/status")
    def kairos_live_status() -> Any:
        return jsonify(kairos_live_service.get_dashboard_payload())

    @app.get("/kairos/sim/status")
    def kairos_sim_status() -> Any:
        return jsonify(kairos_sim_service.get_dashboard_payload())

    @app.post("/api/text-status")
    def text_status_api() -> Any:
        if not open_trade_manager.notifications_enabled():
            return jsonify({"ok": False, "error": "Notifications are currently OFF.", "title": "Delphi 4.3 Test Alert"}), 409
        status_timestamp = datetime.now(CHICAGO_TZ)
        spx_snapshot = get_status_snapshot(market_data_service, "^GSPC", query_type="pushover_status_spx")
        vix_snapshot = get_status_snapshot(market_data_service, "^VIX", query_type="pushover_status_vix")
        title = "Delphi 4.3 Test Alert"
        message = build_pushover_test_message(
            spx_snapshot=spx_snapshot,
            vix_snapshot=vix_snapshot,
            generated_at=status_timestamp,
        )
        result = pushover_service.send_notification(title=title, message=message, priority=0)
        status_code = int(result.get("status_code") or 200)
        if result.get("ok"):
            app.logger.info("Manual Delphi 4.3 Dev Pushover test notification sent.")
        else:
            app.logger.warning(
                "Manual Delphi 4.3 Dev Pushover test notification failed: %s",
                result.get("error") or "Unknown error",
            )
        response_payload = {
            key: value for key, value in result.items() if key not in {"status_code"}
        }
        response_payload["title"] = title
        response_payload["message_body"] = message
        return jsonify(response_payload), status_code

    @app.post("/kairos/activate")
    def kairos_activate() -> Any:
        return jsonify(kairos_live_service.activate_for_today())

    @app.post("/kairos/live/activate")
    def kairos_live_activate() -> Any:
        return jsonify(kairos_live_service.activate_for_today())

    @app.post("/kairos/sim/activate")
    def kairos_sim_activate() -> Any:
        return jsonify(kairos_sim_service.activate_for_today())

    @app.post("/kairos/configure")
    def kairos_configure() -> Any:
        return jsonify(kairos_live_service.configure_runtime(get_request_payload()))

    @app.post("/kairos/live/configure")
    def kairos_live_configure() -> Any:
        payload = normalize_kairos_workspace_payload("live", get_request_payload())
        return jsonify(kairos_live_service.configure_runtime(payload))

    @app.post("/kairos/sim/configure")
    def kairos_sim_configure() -> Any:
        payload = normalize_kairos_workspace_payload("sim", get_request_payload())
        return jsonify(kairos_sim_service.configure_runtime(payload))

    @app.post("/kairos/runner/start")
    def kairos_runner_start() -> Any:
        return jsonify(kairos_live_service.start_simulation_runner(get_request_payload()))

    @app.post("/kairos/live/runner/start")
    def kairos_live_runner_start() -> Any:
        payload = normalize_kairos_workspace_payload("live", get_request_payload())
        return jsonify(kairos_live_service.start_simulation_runner(payload))

    @app.post("/kairos/sim/runner/start")
    def kairos_sim_runner_start() -> Any:
        payload = normalize_kairos_workspace_payload("sim", get_request_payload())
        return jsonify(kairos_sim_service.start_simulation_runner(payload))

    @app.post("/kairos/runner/pause")
    def kairos_runner_pause() -> Any:
        return jsonify(kairos_live_service.pause_simulation_runner())

    @app.post("/kairos/live/runner/pause")
    def kairos_live_runner_pause() -> Any:
        return jsonify(kairos_live_service.pause_simulation_runner())

    @app.post("/kairos/sim/runner/pause")
    def kairos_sim_runner_pause() -> Any:
        return jsonify(kairos_sim_service.pause_simulation_runner())

    @app.post("/kairos/runner/resume")
    def kairos_runner_resume() -> Any:
        return jsonify(kairos_live_service.resume_simulation_runner(get_request_payload()))

    @app.post("/kairos/live/runner/resume")
    def kairos_live_runner_resume() -> Any:
        payload = normalize_kairos_workspace_payload("live", get_request_payload())
        return jsonify(kairos_live_service.resume_simulation_runner(payload))

    @app.post("/kairos/sim/runner/resume")
    def kairos_sim_runner_resume() -> Any:
        payload = normalize_kairos_workspace_payload("sim", get_request_payload())
        return jsonify(kairos_sim_service.resume_simulation_runner(payload))

    @app.post("/kairos/runner/restart")
    def kairos_runner_restart() -> Any:
        return jsonify(kairos_live_service.restart_simulation_runner(get_request_payload()))

    @app.post("/kairos/live/runner/restart")
    def kairos_live_runner_restart() -> Any:
        payload = normalize_kairos_workspace_payload("live", get_request_payload())
        return jsonify(kairos_live_service.restart_simulation_runner(payload))

    @app.post("/kairos/sim/runner/restart")
    def kairos_sim_runner_restart() -> Any:
        payload = normalize_kairos_workspace_payload("sim", get_request_payload())
        return jsonify(kairos_sim_service.restart_simulation_runner(payload))

    @app.post("/kairos/runner/candidate/select")
    def kairos_runner_candidate_select() -> Any:
        return jsonify(kairos_live_service.select_simulation_trade_candidate(get_request_payload()))

    @app.post("/kairos/live/runner/candidate/select")
    def kairos_live_runner_candidate_select() -> Any:
        payload = normalize_kairos_workspace_payload("live", get_request_payload())
        return jsonify(kairos_live_service.select_simulation_trade_candidate(payload))

    @app.post("/kairos/sim/runner/candidate/select")
    def kairos_sim_runner_candidate_select() -> Any:
        payload = normalize_kairos_workspace_payload("sim", get_request_payload())
        return jsonify(kairos_sim_service.select_simulation_trade_candidate(payload))

    @app.post("/kairos/runner/candidate/take")
    def kairos_runner_candidate_take() -> Any:
        payload = get_request_payload()
        payload["pause_on_exit_gates"] = True
        return jsonify(kairos_live_service.take_simulation_trade_candidate(payload))

    @app.post("/kairos/live/runner/candidate/take")
    def kairos_live_runner_candidate_take() -> Any:
        payload = normalize_kairos_workspace_payload("live", get_request_payload())
        payload["pause_on_exit_gates"] = True
        return jsonify(kairos_live_service.take_simulation_trade_candidate(payload))

    @app.post("/kairos/sim/runner/candidate/take")
    def kairos_sim_runner_candidate_take() -> Any:
        payload = normalize_kairos_workspace_payload("sim", get_request_payload())
        payload["pause_on_exit_gates"] = True
        return jsonify(kairos_sim_service.take_simulation_trade_candidate(payload))

    @app.post("/kairos/runner/candidate/ignore")
    def kairos_runner_candidate_ignore() -> Any:
        return jsonify(kairos_live_service.ignore_simulation_trade_candidate())

    @app.post("/kairos/live/runner/candidate/ignore")
    def kairos_live_runner_candidate_ignore() -> Any:
        return jsonify(kairos_live_service.ignore_simulation_trade_candidate())

    @app.post("/kairos/sim/runner/candidate/ignore")
    def kairos_sim_runner_candidate_ignore() -> Any:
        return jsonify(kairos_sim_service.ignore_simulation_trade_candidate())

    @app.post("/kairos/sim/runner/exit/accept")
    def kairos_sim_runner_exit_accept() -> Any:
        return jsonify(kairos_sim_service.accept_simulation_exit_gate())

    @app.post("/kairos/sim/runner/exit/skip")
    def kairos_sim_runner_exit_skip() -> Any:
        return jsonify(kairos_sim_service.skip_simulation_exit_gate())

    @app.post("/kairos/runner/end")
    def kairos_runner_end() -> Any:
        return jsonify(kairos_live_service.end_simulation_runner())

    @app.post("/kairos/live/runner/end")
    def kairos_live_runner_end() -> Any:
        return jsonify(kairos_live_service.end_simulation_runner())

    @app.post("/kairos/sim/runner/end")
    def kairos_sim_runner_end() -> Any:
        return jsonify(kairos_sim_service.end_simulation_runner())

    @app.post("/kairos/sim/replay/import")
    def kairos_sim_replay_import() -> Any:
        payload = get_request_payload()
        return jsonify(kairos_sim_service.import_historical_replay_template(payload))

    @app.post("/kairos/best-trade")
    def kairos_best_trade() -> Any:
        return jsonify(kairos_live_service.request_best_trade_override())

    @app.post("/kairos/live/best-trade")
    def kairos_live_best_trade() -> Any:
        return jsonify(kairos_live_service.request_best_trade_override())

    @app.post("/kairos/live/trade/open")
    def kairos_live_trade_open() -> Any:
        payload = kairos_live_service.open_live_trade(get_request_payload())
        prefill = payload.pop("journal_prefill", None)
        if prefill:
            store_trade_prefill("real", prefill)
            set_status_message("Kairos live trade data is loaded into a journal draft. Review it before saving.", level="info")
            payload["redirect_url"] = url_for("trade_dashboard", trade_mode="real", prefill=1, _anchor="trade-entry-form")
        return jsonify(payload)

    @app.post("/kairos/live/trade/exit/accept")
    def kairos_live_trade_exit_accept() -> Any:
        return jsonify(kairos_live_service.accept_live_exit_recommendation())

    @app.post("/kairos/live/trade/exit/skip")
    def kairos_live_trade_exit_skip() -> Any:
        return jsonify(kairos_live_service.skip_live_exit_recommendation())

    @app.post("/kairos/sim/runner/best-trade")
    def kairos_sim_runner_best_trade() -> Any:
        return jsonify(kairos_sim_service.request_best_trade_override())

    @app.post("/kairos/stop")
    def kairos_stop() -> Any:
        return jsonify(kairos_live_service.stop_for_today())

    @app.post("/kairos/live/stop")
    def kairos_live_stop() -> Any:
        return jsonify(kairos_live_service.stop_for_today())

    @app.post("/kairos/sim/stop")
    def kairos_sim_stop() -> Any:
        return jsonify(kairos_sim_service.stop_for_today())

    @app.route("/apollo", methods=["GET", "POST"])
    def run_apollo():
        form_data = get_form_data(request.form if request.method == "POST" else None)
        error_message = None
        apollo_result = None
        diagnostic_message = None
        info_message = pop_status_message()
        trigger_source = get_apollo_trigger_source()

        if trigger_source:
            try:
                apollo_result = execute_apollo_precheck(apollo_service, trigger_source=trigger_source)
                app.logger.info("Completed Apollo pre-check via %s", trigger_source)
            except MarketDataReauthenticationRequired as exc:
                set_status_message(str(exc), level="warning")
                app.logger.warning("Provider session expired during Apollo run: %s", exc)
                return redirect(url_for("login"))
            except MarketDataAuthenticationError as exc:
                error_message = str(exc)
                app.logger.warning("Provider login required during Apollo run: %s", exc)
            except MarketDataError as exc:
                error_message = str(exc)
                app.logger.warning("Apollo market data error: %s", exc)
            except Exception as exc:  # pragma: no cover - defensive logging
                error_message = "An unexpected error occurred while running Apollo. Check the log for details."
                app.logger.exception("Unexpected Apollo error: %s", exc)

        return render_research_page(
            form_data=form_data,
            result=None,
            apollo_result=apollo_result,
            error_message=error_message,
            info_message=info_message,
            diagnostic_message=diagnostic_message,
            active_page="apollo",
            page_browser_title="Apollo | Delphi",
            page_heading="Apollo",
            page_copy="Delphi Apollo workflow for live structure, macro review, and next-market-day candidate selection.",
        )

    @app.route("/debug/run-apollo", methods=["GET", "POST"])
    def debug_run_apollo():
        if not is_local_dev_request():
            return ("Not Found", 404)

        try:
            apollo_result = execute_apollo_precheck(apollo_service, trigger_source="autorun URL")
            app.logger.info("Completed Apollo debug pre-check")
        except MarketDataReauthenticationRequired as exc:
            app.logger.warning("Provider session expired during Apollo debug run: %s", exc)
            return jsonify({"ok": False, "error": str(exc), "requires_login": True}), 401
        except MarketDataAuthenticationError as exc:
            app.logger.warning("Provider login required during Apollo debug run: %s", exc)
            return jsonify({"ok": False, "error": str(exc), "requires_login": True}), 401
        except MarketDataError as exc:
            app.logger.warning("Apollo market data error during debug run: %s", exc)
            return jsonify({"ok": False, "error": str(exc)}), 400
        except Exception as exc:  # pragma: no cover - defensive logging
            app.logger.exception("Unexpected Apollo debug error: %s", exc)
            return jsonify({"ok": False, "error": "Unexpected Apollo debug error."}), 500

        if request.args.get("format", "json").strip().lower() == "html":
            return render_research_page(
                form_data=get_form_data(None),
                result=None,
                apollo_result=apollo_result,
                error_message=None,
                info_message=None,
                diagnostic_message=None,
                active_page="apollo",
                page_browser_title="Apollo | Delphi",
                page_heading="Apollo",
                page_copy="Delphi Apollo workflow for live structure, macro review, and next-market-day candidate selection.",
            )

        return jsonify(
            {
                "ok": True,
                "trigger_source": apollo_result.get("apollo_trigger_source", "autorun URL"),
                "title": apollo_result.get("title"),
                "status": apollo_result.get("status"),
                "structure_grade": apollo_result.get("structure_grade"),
                "macro_grade": apollo_result.get("macro_grade"),
                "trade_candidates_count": apollo_result.get("trade_candidates_count"),
                "trigger_note": apollo_result.get("apollo_trigger_note"),
            }
        )

    @app.get("/debug/option-chain")
    def debug_option_chain():
        provider = market_data_service.live_provider
        if not hasattr(provider, "debug_option_chain_request"):
            return "Option-chain debug is unavailable for the active provider.", 400

        try:
            payload = provider.debug_option_chain_request("^GSPC", target_date=date(2026, 4, 6), minimal_only=True)
            app.logger.info("Schwab option-chain debug payload: %s", json.dumps(payload, default=str))
            return f"<pre>{json.dumps(payload, default=str, indent=2)}</pre>"
        except MarketDataReauthenticationRequired as exc:
            app.logger.warning("Provider session expired during option-chain debug: %s", exc)
            return f"<pre>{exc}</pre>", 401
        except MarketDataAuthenticationError as exc:
            app.logger.warning("Provider login required during option-chain debug: %s", exc)
            return f"<pre>{exc}</pre>", 401
        except Exception as exc:  # pragma: no cover - defensive logging
            app.logger.exception("Unexpected option-chain debug error: %s", exc)
            return f"<pre>{exc}</pre>", 500

    @app.get("/login")
    def login():
        provider = market_data_service.provider
        if not hasattr(provider, "auth_service"):
            set_status_message("The active market data provider does not use OAuth login.", level="info")
            return redirect(url_for("index"))

        auth_service = provider.auth_service
        oauth_session_keys = app.extensions["oauth_session_keys"]
        state = auth_service.build_state_token()
        session.pop("schwab_oauth_state", None)
        session[oauth_session_keys["oauth_state"]] = state
        session[oauth_session_keys["pkce_verifier"]] = ""
        session[oauth_session_keys["login_in_progress"]] = True
        session[oauth_session_keys["callback_pending"]] = True
        session[oauth_session_keys["connected"]] = False
        session[oauth_session_keys["authorized"]] = False
        app.logger.info(
            "OAuth state created | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | session_cookie=%s | oauth_namespace=%s | oauth_state=%s",
            APP_CONFIG.app_display_name,
            APP_CONFIG.app_port,
            APP_CONFIG.schwab_redirect_uri,
            APP_CONFIG.schwab_token_path,
            APP_CONFIG.session_cookie_name,
            APP_CONFIG.oauth_session_namespace,
            mask_oauth_state(state),
        )

        try:
            return redirect(auth_service.build_authorization_url(state=state))
        except Exception as exc:
            session[oauth_session_keys["login_in_progress"]] = False
            session[oauth_session_keys["callback_pending"]] = False
            set_status_message(str(exc), level="error")
            app.logger.warning("Unable to build Schwab authorization URL: %s", exc)
            return redirect(url_for("index"))

    @app.get("/callback")
    def callback():
        provider = market_data_service.provider
        if not hasattr(provider, "auth_service"):
            set_status_message("The active market data provider does not support OAuth callbacks.", level="error")
            return redirect(url_for("index"))

        oauth_session_keys = app.extensions["oauth_session_keys"]
        received_state = request.args.get("state")
        authorization_code = request.args.get("code")
        app.logger.info(
            "OAuth callback received | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | session_cookie=%s | oauth_namespace=%s | has_code=%s | oauth_state=%s",
            APP_CONFIG.app_display_name,
            APP_CONFIG.app_port,
            APP_CONFIG.schwab_redirect_uri,
            APP_CONFIG.schwab_token_path,
            APP_CONFIG.session_cookie_name,
            APP_CONFIG.oauth_session_namespace,
            bool(authorization_code),
            mask_oauth_state(received_state),
        )

        error = request.args.get("error")
        if error:
            session[oauth_session_keys["login_in_progress"]] = False
            session[oauth_session_keys["callback_pending"]] = False
            set_status_message(f"Schwab authorization failed: {error}", level="error")
            return redirect(url_for("index"))

        expected_state = session.pop(oauth_session_keys["oauth_state"], None)
        session.pop("schwab_oauth_state", None)
        if not expected_state or received_state != expected_state:
            session[oauth_session_keys["login_in_progress"]] = False
            session[oauth_session_keys["callback_pending"]] = False
            session[oauth_session_keys["connected"]] = False
            session[oauth_session_keys["authorized"]] = False
            session.pop(oauth_session_keys["pkce_verifier"], None)
            app.logger.warning(
                "OAuth state validation failed | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | oauth_namespace=%s | expected_state=%s | received_state=%s",
                APP_CONFIG.app_display_name,
                APP_CONFIG.app_port,
                APP_CONFIG.schwab_redirect_uri,
                APP_CONFIG.schwab_token_path,
                APP_CONFIG.oauth_session_namespace,
                mask_oauth_state(expected_state),
                mask_oauth_state(received_state),
            )
            set_status_message("Schwab authorization state did not match. Please try again.", level="error")
            return redirect(url_for("index"))
        app.logger.info(
            "OAuth state validation passed | env=%s | port=%s | redirect_uri=%s | token_target_path=%s | oauth_namespace=%s | oauth_state=%s",
            APP_CONFIG.app_display_name,
            APP_CONFIG.app_port,
            APP_CONFIG.schwab_redirect_uri,
            APP_CONFIG.schwab_token_path,
            APP_CONFIG.oauth_session_namespace,
            mask_oauth_state(received_state),
        )

        if not authorization_code:
            session[oauth_session_keys["login_in_progress"]] = False
            session[oauth_session_keys["callback_pending"]] = False
            set_status_message("Schwab did not return an authorization code.", level="error")
            return redirect(url_for("index"))

        try:
            provider.auth_service.exchange_code_for_tokens(authorization_code)
            session[oauth_session_keys["login_in_progress"]] = False
            session[oauth_session_keys["callback_pending"]] = False
            session[oauth_session_keys["connected"]] = True
            session[oauth_session_keys["authorized"]] = True
            session.pop(oauth_session_keys["pkce_verifier"], None)
            set_status_message("Connected to Schwab successfully.", level="info")
        except Exception as exc:
            session[oauth_session_keys["login_in_progress"]] = False
            session[oauth_session_keys["callback_pending"]] = False
            session[oauth_session_keys["connected"]] = False
            session[oauth_session_keys["authorized"]] = False
            set_status_message(str(exc), level="error")
            app.logger.warning("Schwab token exchange failed: %s", exc)

        return redirect(url_for("index"))

    @app.get("/trades/<trade_mode>")
    def trade_dashboard(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        prefill_requested = str(request.args.get("prefill", "")).strip().lower() in {"1", "true", "yes", "on"}
        form_values = blank_trade_form(normalized_mode)
        form_values["trade_number"] = str(trade_store.next_trade_number())
        form_title = "Add Manual Trade"
        prefill_active = False
        prefill_notice = ""

        if prefill_requested:
            draft_values = get_trade_prefill(normalized_mode)
            if draft_values:
                form_values = merge_trade_form_values(form_values, draft_values)
                form_values["trade_number"] = form_values.get("trade_number") or str(trade_store.next_trade_number())
                prefill_meta = get_trade_prefill_metadata(form_values)
                form_title = prefill_meta["title"]
                prefill_notice = prefill_meta["notice"]
                prefill_active = True

        return render_template(
            "trades.html",
            **build_trade_page_context(
                store=trade_store,
                trade_mode=normalized_mode,
                form_values=form_values,
                form_action=url_for("trade_create", trade_mode=normalized_mode),
                form_title=form_title,
                editing_trade_id=None,
                error_message=None,
                info_message=pop_status_message(),
                prefill_active=prefill_active,
                prefill_notice=prefill_notice,
            ),
        )

    @app.route("/trades/<trade_mode>/new", methods=["GET", "POST"])
    def trade_create(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        if request.method == "GET":
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode, _anchor="trade-entry-form"))

        if request.method == "POST":
            submitted_values = coerce_trade_form_input(request.form)
            try:
                if is_apollo_prefill_submission(submitted_values):
                    duplicate = trade_store.find_recent_duplicate(submitted_values, window_seconds=15)
                    if duplicate:
                        clear_trade_prefill(normalized_mode)
                        set_status_message("Apollo draft already saved recently. Review the existing trade instead of submitting twice.", level="warning")
                        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

                trade_id = trade_store.create_trade(submitted_values)
                created_trade = trade_store.get_trade(trade_id) or {"trade_mode": normalized_mode}
                redirect_mode = str(created_trade.get("trade_mode", normalized_mode))
                clear_trade_prefill(redirect_mode)
                save_message, save_level = build_trade_save_status(created_trade, action="saved")
                set_status_message(save_message, level=save_level)
                return redirect(url_for("trade_dashboard", trade_mode=redirect_mode))
            except ValueError as exc:
                return render_template(
                    "trades.html",
                    **build_trade_page_context(
                        store=trade_store,
                        trade_mode=normalized_mode,
                        form_values=submitted_values,
                        form_action=url_for("trade_create", trade_mode=normalized_mode),
                        form_title=(get_trade_prefill_metadata(submitted_values)["title"] if str(submitted_values.get("prefill_source") or "").strip() else "Add Manual Trade"),
                        editing_trade_id=None,
                        error_message=str(exc),
                        info_message=pop_status_message(),
                        prefill_active=bool(str(submitted_values.get("prefill_source") or "").strip()),
                        prefill_notice=get_trade_prefill_metadata(submitted_values)["notice"] if str(submitted_values.get("prefill_source") or "").strip() else "",
                    ),
                )

    @app.route("/trades/<trade_mode>/<int:trade_id>/edit", methods=["GET", "POST"])
    def trade_edit(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

        if request.method == "POST":
            submitted_values = coerce_trade_form_input(request.form)
            submitted_close_events = coerce_trade_close_event_input(request.form)
            if submitted_close_events is not None:
                submitted_values["close_events"] = submitted_close_events
            try:
                trade_store.update_trade(trade_id, submitted_values)
                updated_trade = trade_store.get_trade(trade_id) or trade
                redirect_mode = str(updated_trade.get("trade_mode", normalized_mode))
                clear_trade_close_prefill(trade_id)
                save_message, save_level = build_trade_save_status(updated_trade, action="updated")
                set_status_message(save_message, level=save_level)
                return redirect(url_for("trade_dashboard", trade_mode=redirect_mode))
            except ValueError as exc:
                return render_template(
                    "trades.html",
                    **build_trade_page_context(
                        store=trade_store,
                        trade_mode=normalized_mode,
                        form_values=submitted_values,
                        form_action=url_for("trade_edit", trade_mode=normalized_mode, trade_id=trade_id),
                        form_title=f"Edit Trade #{trade.get('trade_number') or trade_id}",
                        editing_trade_id=trade_id,
                        editing_trade=build_edit_trade_preview(trade, submitted_values, submitted_close_events),
                        error_message=str(exc),
                        info_message=pop_status_message(),
                        prefill_active=False,
                    ),
                )

        edit_mode = str(trade.get("trade_mode") or normalized_mode)
        close_prefill = get_trade_close_prefill(trade_id)
        editing_trade = apply_trade_close_prefill(trade, close_prefill)
        return render_template(
            "trades.html",
            **build_trade_page_context(
                store=trade_store,
                trade_mode=edit_mode,
                form_values=form_trade_record(trade),
                form_action=url_for("trade_edit", trade_mode=edit_mode, trade_id=trade_id),
                form_title=f"Edit Trade #{trade.get('trade_number') or trade_id}",
                editing_trade_id=trade_id,
                editing_trade=editing_trade,
                error_message=None,
                info_message=pop_status_message(),
                prefill_active=False,
            ),
        )

    @app.post("/trades/<trade_mode>/<int:trade_id>/delete")
    def trade_delete(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if trade:
            trade_store.delete_trade(trade_id)
            set_status_message(f"Deleted trade #{trade.get('trade_number') or trade_id}.", level="info")
            return redirect(url_for("trade_dashboard", trade_mode=str(trade.get("trade_mode") or normalized_mode)))

        set_status_message("Trade not found.", level="error")
        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

    @app.post("/trades/<trade_mode>/<int:trade_id>/reduce")
    def trade_reduce(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode, _anchor="trade-log-table"))

        try:
            trade_store.reduce_trade(
                trade_id,
                {
                    "contracts_closed": request.form.get("contracts_closed"),
                    "actual_exit_value": request.form.get("actual_exit_value"),
                    "event_datetime": request.form.get("event_datetime"),
                    "spx_at_exit": request.form.get("spx_at_exit"),
                    "close_method": request.form.get("close_method") or "Reduce",
                    "close_reason": request.form.get("close_reason") or "Partial reduction",
                    "notes_exit": request.form.get("notes_exit") or "",
                },
            )
            updated_trade = trade_store.get_trade(trade_id) or trade
            set_status_message(
                f"Reduced trade #{updated_trade.get('trade_number') or trade_id}.",
                level="info",
            )
            return redirect(url_for("trade_dashboard", trade_mode=str(updated_trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))
        except ValueError as exc:
            set_status_message(str(exc), level="error")
            return redirect(url_for("trade_dashboard", trade_mode=str(trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))

    @app.post("/trades/<trade_mode>/<int:trade_id>/expire")
    def trade_expire(trade_mode: str, trade_id: int):
        normalized_mode = resolve_trade_mode(trade_mode)
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode, _anchor="trade-log-table"))

        try:
            trade_store.expire_trade(
                trade_id,
                {
                    "event_datetime": request.form.get("event_datetime"),
                    "actual_exit_value": request.form.get("actual_exit_value") or 0,
                    "spx_at_exit": request.form.get("spx_at_exit"),
                    "close_method": request.form.get("close_method") or "Expire",
                    "close_reason": request.form.get("close_reason") or "Expired Worthless",
                    "notes_exit": request.form.get("notes_exit") or "",
                },
            )
            updated_trade = trade_store.get_trade(trade_id) or trade
            set_status_message(
                f"Expired trade #{updated_trade.get('trade_number') or trade_id}.",
                level="info",
            )
            return redirect(url_for("trade_dashboard", trade_mode=str(updated_trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))
        except ValueError as exc:
            set_status_message(str(exc), level="error")
            return redirect(url_for("trade_dashboard", trade_mode=str(trade.get("trade_mode") or normalized_mode), _anchor="trade-log-table"))

    @app.post("/trades/<trade_mode>/import/preview")
    def trade_import_preview(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        form_values = blank_trade_form(normalized_mode)
        upload = request.files.get("import_file")
        import_journal_name = str(request.form.get("import_journal_name") or JOURNAL_NAME_DEFAULT).strip() or JOURNAL_NAME_DEFAULT

        try:
            preview_source = parse_trade_import(upload, trade_mode=normalized_mode, journal_name=import_journal_name)
            import_preview = build_trade_import_preview(store=trade_store, preview_source=preview_source)
            import_token = store_trade_import_preview(
                app,
                trade_mode=normalized_mode,
                preview_payload={
                    "importable_rows": import_preview["importable_rows"],
                    "file_name": import_preview["file_name"],
                    "journal_name": import_preview["journal_name"],
                    "duplicate_count": import_preview["duplicate_count"],
                },
            )
            import_preview["token"] = import_token
            return render_template(
                "trades.html",
                **build_trade_page_context(
                    store=trade_store,
                    trade_mode=normalized_mode,
                    form_values=form_values,
                    form_action=url_for("trade_create", trade_mode=normalized_mode),
                    form_title="Add Manual Trade",
                    editing_trade_id=None,
                    error_message=None,
                    info_message=pop_status_message(),
                    prefill_active=False,
                    import_preview=import_preview,
                    import_journal_name=import_journal_name,
                ),
            )
        except ValueError as exc:
            return render_template(
                "trades.html",
                **build_trade_page_context(
                    store=trade_store,
                    trade_mode=normalized_mode,
                    form_values=form_values,
                    form_action=url_for("trade_create", trade_mode=normalized_mode),
                    form_title="Add Manual Trade",
                    editing_trade_id=None,
                    error_message=str(exc),
                    info_message=pop_status_message(),
                    prefill_active=False,
                    import_preview=None,
                    import_journal_name=import_journal_name,
                ),
            )

    @app.post("/trades/<trade_mode>/import/confirm")
    def trade_import_confirm(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        import_token = str(request.form.get("import_token") or "").strip()
        stored_preview = load_trade_import_preview(app, import_token)
        if not stored_preview or stored_preview.get("trade_mode") != normalized_mode:
            set_status_message("Trade import preview expired. Upload the file again before importing.", level="warning")
            return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

        imported_count = 0
        skipped_duplicates = int(stored_preview.get("duplicate_count") or 0)
        seen_signatures: set[tuple[Any, ...]] = set()
        for payload in stored_preview.get("importable_rows", []):
            signature = build_trade_duplicate_signature(payload, already_normalized=True)
            if signature in seen_signatures or trade_store.find_duplicate_trade(payload):
                skipped_duplicates += 1
                continue
            trade_store.create_trade(payload)
            seen_signatures.add(signature)
            imported_count += 1

        delete_trade_import_preview(app, import_token)
        if imported_count:
            summary = f"Imported {imported_count} trade{'s' if imported_count != 1 else ''}."
            if skipped_duplicates:
                summary += f" Skipped {skipped_duplicates} duplicate{'s' if skipped_duplicates != 1 else ''}."
            set_status_message(summary, level="info")
        else:
            set_status_message("No new trades were imported. All preview rows were duplicates or unavailable.", level="warning")
        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

    @app.post("/trades/<trade_mode>/import/cancel")
    def trade_import_cancel(trade_mode: str):
        normalized_mode = resolve_trade_mode(trade_mode)
        delete_trade_import_preview(app, str(request.form.get("import_token") or "").strip())
        set_status_message("Trade import preview cleared.", level="info")
        return redirect(url_for("trade_dashboard", trade_mode=normalized_mode))

    @app.post("/apollo/prefill-candidate")
    def apollo_prefill_candidate():
        target_mode = resolve_trade_mode(request.form.get("target_mode") or request.form.get("trade_mode") or "simulated")
        draft_values = coerce_apollo_trade_input(request.form, trade_mode=target_mode)
        store_trade_prefill(target_mode, draft_values)
        set_status_message(
            f"Apollo {draft_values.get('candidate_profile') or 'Candidate'} sent to {TRADE_MODE_LABELS[target_mode]}. Review the draft, then click Save Trade to commit it.",
            level="info",
        )
        return redirect(url_for("trade_dashboard", trade_mode=target_mode, prefill=1, _anchor="trade-entry-form"))

    @app.post("/kairos/prefill-candidate")
    def kairos_prefill_candidate():
        target_mode = resolve_trade_mode(request.form.get("target_mode") or request.form.get("trade_mode") or "simulated")
        draft_values = coerce_kairos_trade_input(request.form, trade_mode=target_mode)
        store_trade_prefill(target_mode, draft_values)
        set_status_message(
            f"Kairos {draft_values.get('candidate_profile') or 'Candidate'} sent to {TRADE_MODE_LABELS[target_mode]}. Review the draft, then click Save Trade to commit it.",
            level="info",
        )
        return redirect(url_for("trade_dashboard", trade_mode=target_mode, prefill=1, _anchor="trade-entry-form"))

    @app.get("/performance")
    def performance_dashboard() -> str:
        dashboard_payload = performance_service.build_dashboard()
        return render_template(
            "performance.html",
            dashboard_payload=dashboard_payload,
            filter_groups=PERFORMANCE_FILTER_GROUPS,
            info_message=pop_status_message(),
        )

    @app.get("/management/open-trades")
    def open_trade_management_page() -> str:
        management_payload = open_trade_manager.evaluate_open_trades(send_alerts=False)
        return render_template(
            "open_trade_management.html",
            management_payload=management_payload,
            info_message=pop_status_message(),
        )

    @app.post("/management/open-trades/notifications-toggle")
    def open_trade_management_toggle_notifications() -> Any:
        enabled = str(request.form.get("notifications_enabled") or "").strip().lower() == "true"
        open_trade_manager.set_notifications_enabled(enabled)
        set_status_message(
            f"Notifications turned {'ON' if enabled else 'OFF'} for real-time trade alerts.",
            level="info",
        )
        return redirect(url_for("open_trade_management_page"))

    @app.post("/management/open-trades/status-update/<trade_mode>")
    def open_trade_management_status_update(trade_mode: str) -> Any:
        normalized_trade_mode = str(trade_mode or "").strip().lower()
        if normalized_trade_mode not in {"real", "simulated"}:
            abort(404)
        result = open_trade_manager.send_manual_status_update(trade_mode=normalized_trade_mode)
        trade_mode_label = "real" if normalized_trade_mode == "real" else "simulated"
        if result["sent"]:
            suffix = " Automatic notifications remain OFF." if not open_trade_manager.notifications_enabled() else ""
            set_status_message(
                f"Sent Pushover status update for {result['record_count']} {trade_mode_label} open trade(s).{suffix}",
                level="info",
            )
        elif result["record_count"] == 0:
            set_status_message(
                f"No open {trade_mode_label} trades are available for a status update.",
                level="warning",
            )
        else:
            set_status_message(
                f"Pushover {trade_mode_label} status update failed: {result['error'] or 'Unable to send notification.'}",
                level="warning",
            )
        return redirect(url_for("open_trade_management_page"))

    @app.post("/management/open-trades/<int:trade_id>/prefill-close")
    def open_trade_management_prefill_close(trade_id: int) -> Any:
        trade = trade_store.get_trade(trade_id)
        if not trade:
            set_status_message("Trade not found.", level="error")
            return redirect(url_for("open_trade_management_page"))

        management_payload = open_trade_manager.evaluate_open_trades(send_alerts=False)
        record = next((item for item in management_payload["records"] if int(item.get("trade_id") or 0) == trade_id), None)
        if record is None:
            set_status_message("Open trade record was not available for management prefill.", level="warning")
            return redirect(url_for("open_trade_management_page"))
        if record.get("current_spread_mark") in {None, ""}:
            set_status_message("Current close cost could not be derived from the live option chain, so no journal prefill was created.", level="warning")
            return redirect(url_for("open_trade_management_page"))
        if int(record.get("contracts") or 0) <= 0:
            set_status_message("No remaining contracts were available to prefill.", level="warning")
            return redirect(url_for("open_trade_management_page"))

        store_trade_close_prefill(trade_id, build_manage_trade_close_prefill(record))
        set_status_message(
            f"Prefilled a close event for trade #{trade.get('trade_number') or trade_id}. Review the journal entry and click Save Trade to commit it.",
            level="info",
        )
        return redirect(url_for("trade_edit", trade_mode=str(trade.get("trade_mode") or "real"), trade_id=trade_id, _anchor="position-management"))

    @app.get("/performance/data")
    def performance_dashboard_data():
        filters = parse_performance_request_filters(request.args)
        return jsonify(performance_service.build_dashboard(filters=filters))

    @app.get("/performance-summary")
    def performance_summary() -> str:
        enriched_trades = performance_engine.load_trades()
        return render_template(
            "performance_summary.html",
            enriched_trades=enriched_trades,
            overall_summary=performance_engine.get_overall_performance(),
            summary_by_system=performance_engine.get_performance_by_system(),
            summary_by_structure=performance_engine.get_performance_by_structure(),
            summary_by_vix_bucket=performance_engine.get_performance_by_vix_bucket(),
            info_message=pop_status_message(),
        )

    @app.post("/export/<file_format>")
    def export(file_format: str):
        form_data = get_form_data(request.form)
        result = None
        error_message = None
        diagnostic_message = None
        apollo_result = None
        query = None

        try:
            query = parse_query_request(request.form)
            result = execute_query(query=query, service=market_data_service)
            export_frame = result["export_frame"]
            payload = export_service.export_dataframe(
                dataframe=export_frame,
                ticker=query.definition.ticker,
                query_type=query.definition.key,
                file_format=file_format,
            )
            app.logger.info("Exported %s for %s as %s", query.definition.key, query.definition.ticker, file_format)
            return send_file(payload.content, as_attachment=True, download_name=payload.filename, mimetype=payload.mimetype)
        except MarketDataReauthenticationRequired as exc:
            set_status_message(str(exc), level="warning")
            app.logger.warning("Provider session expired during export: %s", exc)
            return redirect(url_for("login"))
        except MarketDataAuthenticationError as exc:
            error_message = str(exc)
            app.logger.warning("Provider login required during export: %s", exc)
        except ValidationError as exc:
            error_message = str(exc)
            app.logger.warning("Validation error during export: %s", exc)
        except MarketDataError as exc:
            error_message = str(exc)
            diagnostic_message = build_diagnostic_message(query, str(exc), market_data_service)
            app.logger.warning("Market data error during export: %s", exc)
        except Exception as exc:  # pragma: no cover - defensive logging
            error_message = "An unexpected error occurred while exporting the data. Check the log for details."
            app.logger.exception("Unexpected export error: %s", exc)

        return render_research_page(
            form_data=form_data,
            result=result,
            apollo_result=apollo_result,
            error_message=error_message,
            info_message=pop_status_message(),
            diagnostic_message=diagnostic_message,
            active_page="research",
            page_browser_title="Research | Delphi",
            page_heading="Research",
            page_copy="Live and historical SPX / VIX research workspace with provider-aware routing and export-ready lookup tools.",
        )

    return app


def configure_logging(app: Flask) -> None:
    """Configure console and rotating-file logging."""
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    log_path = Path(APP_CONFIG.app_log_path).expanduser() if APP_CONFIG.app_log_path else Path(app.root_path) / "market_lookup.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    for handler in app.logger.handlers:
        handler.close()
    app.logger.handlers.clear()
    app.logger.addHandler(file_handler)
    app.logger.addHandler(stream_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.propagate = False


def get_form_data(source: Optional[Any]) -> Dict[str, str]:
    """Return template-safe form state."""
    source = source or {}
    return {
        "query_type": source.get("query_type", "latest_spx"),
        "single_date": source.get("single_date", ""),
        "start_date": source.get("start_date", ""),
        "end_date": source.get("end_date", ""),
    }


def build_startup_menu_payload(market_data_service: MarketDataService) -> Dict[str, Any]:
    """Return fresh startup-menu status cards for Delphi's command hub."""
    provider_meta = market_data_service.get_provider_metadata()
    requires_auth = bool(provider_meta.get("requires_auth"))
    authenticated = bool(provider_meta.get("authenticated", True))
    notes: list[str] = []

    def _snapshot_card(label: str, ticker: str, key: str) -> Dict[str, str]:
        try:
            snapshot = market_data_service.get_latest_snapshot(ticker, query_type=f"startup_{key}")
            change_summary = build_market_change_summary(snapshot)
            return {
                "label": label,
                "value": format_value(snapshot.get("Latest Value")),
                "value_trend": change_summary["trend"],
                "change": change_summary["display"],
                "meta": format_value(snapshot.get("As Of")),
            }
        except MarketDataReauthenticationRequired:
            notes.append(f"{label} unavailable until the Schwab session is refreshed.")
            return {
                "label": label,
                "value": "—",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": "Session refresh required",
            }
        except MarketDataAuthenticationError:
            notes.append(f"{label} unavailable until Schwab login is completed.")
            return {
                "label": label,
                "value": "—",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": "Login required",
            }
        except MarketDataError as exc:
            notes.append(f"{label} unavailable: {exc}")
            return {
                "label": label,
                "value": "—",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": str(exc),
            }
        except Exception as exc:  # pragma: no cover - defensive rendering fallback
            notes.append(f"{label} unavailable: {exc}")
            return {
                "label": label,
                "value": "—",
                "value_trend": "neutral",
                "change": "Change unavailable",
                "meta": "Unavailable",
            }

    connection_label = "Connected"
    connection_meta = provider_meta.get("live_provider_name", "Unknown Provider")
    if requires_auth and not authenticated:
        connection_label = "Login required"
    elif not requires_auth:
        connection_label = "Ready"

    return {
        "brand_name": "Delphi",
        "connection_label": connection_label,
        "connection_meta": connection_meta,
        "connection_requires_login": requires_auth and not authenticated,
        "cards": [
            _snapshot_card("SPX", "^GSPC", "spx"),
            _snapshot_card("VIX", "^VIX", "vix"),
            {
                "label": "Schwab connection",
                "value": connection_label,
                "value_trend": "connection",
                "change": "",
                "meta": connection_meta,
            },
        ],
        "notes": notes,
    }


def build_market_change_summary(snapshot: Dict[str, Any]) -> Dict[str, str]:
    point_change = _coerce_float(snapshot.get("Daily Point Change"))
    percent_change = _coerce_float(snapshot.get("Daily Percent Change"))

    if point_change is None and percent_change is None:
        return {"trend": "neutral", "display": "Change unavailable"}

    direction_source = point_change if point_change not in (None, 0) else percent_change
    if direction_source is None or direction_source == 0:
        trend = "neutral"
    else:
        trend = "positive" if direction_source > 0 else "negative"

    segments: list[str] = []
    if point_change is not None:
        segments.append(f"{point_change:+,.2f} pts")
    if percent_change is not None:
        segments.append(f"{percent_change:+,.2f}%")

    if not segments:
        segments.append("Change unavailable")

    return {"trend": trend, "display": " · ".join(segments)}


def get_status_snapshot(market_data_service: MarketDataService, ticker: str, *, query_type: str) -> Dict[str, Any]:
    """Return a snapshot payload suitable for notification formatting without raising provider errors."""
    try:
        return market_data_service.get_latest_snapshot(ticker, query_type=query_type)
    except MarketDataReauthenticationRequired:
        return {"status_unavailable": True, "status_note": "Session refresh required"}
    except MarketDataAuthenticationError:
        return {"status_unavailable": True, "status_note": "Login required"}
    except MarketDataError as exc:
        return {"status_unavailable": True, "status_note": str(exc)}
    except Exception:
        return {"status_unavailable": True, "status_note": "Unavailable"}


def build_pushover_test_message(
    *,
    spx_snapshot: Optional[Dict[str, Any]],
    vix_snapshot: Optional[Dict[str, Any]],
    generated_at: datetime,
) -> str:
    """Build the Delphi 4.3 Dev header-button Pushover test message."""
    return "\n".join(
        [
            "SPX Update",
            f"SPX: {format_notification_snapshot(spx_snapshot)}",
            f"VIX: {format_notification_snapshot(vix_snapshot)}",
            f"Time: {generated_at.strftime('%Y-%m-%d %I:%M %p %Z').lstrip('0')}",
            "Source: Delphi 4.3 Dev Pushover test",
        ]
    )


def format_notification_snapshot(snapshot: Optional[Dict[str, Any]]) -> str:
    if not snapshot or snapshot.get("status_unavailable"):
        return "Unavailable"
    latest_value = snapshot.get("Latest Value")
    as_of = str(snapshot.get("As Of") or "").strip()
    if latest_value in {None, ""}:
        return "Unavailable"
    formatted_value = format_value(latest_value)
    return f"{formatted_value} ({as_of})" if as_of else formatted_value


def parse_performance_request_filters(source: Any) -> Dict[str, list[str]]:
    filters: Dict[str, list[str]] = {}
    for key in PERFORMANCE_FILTER_GROUPS:
        values = source.getlist(key) if hasattr(source, "getlist") else source.get(key, [])
        if isinstance(values, str):
            values = [item for item in values.split(",") if item]
        filters[key] = list(values or [])
    return filters


def set_status_message(message: str, level: str = "info") -> None:
    """Store a one-time UI message in the session."""
    session["status_message"] = {"text": message, "level": level}


def pop_status_message() -> Optional[Dict[str, str]]:
    """Retrieve and clear a one-time UI message from the session."""
    return session.pop("status_message", None)


def store_trade_prefill(trade_mode: str, values: Dict[str, Any]) -> None:
    drafts = dict(session.get(APOLLO_PREFILL_SESSION_KEY, {}))
    drafts[resolve_trade_mode(trade_mode)] = {key: values.get(key, "") for key in TRADE_FORM_FIELDS}
    session[APOLLO_PREFILL_SESSION_KEY] = drafts


def get_trade_prefill(trade_mode: str) -> Optional[Dict[str, Any]]:
    drafts = session.get(APOLLO_PREFILL_SESSION_KEY, {}) or {}
    draft = drafts.get(resolve_trade_mode(trade_mode))
    return dict(draft) if isinstance(draft, dict) else None


def clear_trade_prefill(trade_mode: Optional[str] = None) -> None:
    if trade_mode is None:
        session.pop(APOLLO_PREFILL_SESSION_KEY, None)
        return

    drafts = dict(session.get(APOLLO_PREFILL_SESSION_KEY, {}))
    drafts.pop(resolve_trade_mode(trade_mode), None)
    if drafts:
        session[APOLLO_PREFILL_SESSION_KEY] = drafts
    else:
        session.pop(APOLLO_PREFILL_SESSION_KEY, None)


def store_trade_close_prefill(trade_id: int, values: Dict[str, Any]) -> None:
    drafts = dict(session.get(MANAGEMENT_CLOSE_PREFILL_SESSION_KEY, {}))
    drafts[str(int(trade_id))] = dict(values)
    session[MANAGEMENT_CLOSE_PREFILL_SESSION_KEY] = drafts


def get_trade_close_prefill(trade_id: int) -> Optional[Dict[str, Any]]:
    drafts = session.get(MANAGEMENT_CLOSE_PREFILL_SESSION_KEY, {}) or {}
    draft = drafts.get(str(int(trade_id)))
    return dict(draft) if isinstance(draft, dict) else None


def clear_trade_close_prefill(trade_id: Optional[int] = None) -> None:
    if trade_id is None:
        session.pop(MANAGEMENT_CLOSE_PREFILL_SESSION_KEY, None)
        return
    drafts = dict(session.get(MANAGEMENT_CLOSE_PREFILL_SESSION_KEY, {}))
    drafts.pop(str(int(trade_id)), None)
    if drafts:
        session[MANAGEMENT_CLOSE_PREFILL_SESSION_KEY] = drafts
    else:
        session.pop(MANAGEMENT_CLOSE_PREFILL_SESSION_KEY, None)


def get_trade_import_preview_directory(app: Flask) -> Path:
    directory = Path(app.instance_path) / TRADE_IMPORT_PREVIEW_DIRNAME
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def store_trade_import_preview(app: Flask, trade_mode: str, preview_payload: Dict[str, Any]) -> str:
    token = uuid4().hex
    payload = {"trade_mode": resolve_trade_mode(trade_mode), **preview_payload}
    preview_path = get_trade_import_preview_directory(app) / f"{token}.json"
    preview_path.write_text(json.dumps(payload), encoding="utf-8")
    return token


def load_trade_import_preview(app: Flask, token: str) -> Optional[Dict[str, Any]]:
    normalized_token = "".join(character for character in str(token or "") if character.isalnum())
    if not normalized_token:
        return None
    preview_path = get_trade_import_preview_directory(app) / f"{normalized_token}.json"
    if not preview_path.exists():
        return None
    try:
        return json.loads(preview_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def delete_trade_import_preview(app: Flask, token: str) -> None:
    normalized_token = "".join(character for character in str(token or "") if character.isalnum())
    if not normalized_token:
        return
    preview_path = get_trade_import_preview_directory(app) / f"{normalized_token}.json"
    if preview_path.exists():
        preview_path.unlink()


def coerce_trade_form_input(source: Any) -> Dict[str, Any]:
    values = {key: source.get(key, "") for key in TRADE_FORM_FIELDS}
    values["trade_mode"] = source.get("trade_mode") or source.get("trade_mode_filter") or "real"
    values["journal_name"] = source.get("journal_name") or JOURNAL_NAME_DEFAULT
    return values


def coerce_trade_close_event_input(source: Any) -> Optional[list[Dict[str, Any]]]:
    if not hasattr(source, "getlist"):
        return None
    if not source.get("close_events_present"):
        return None
    row_ids = source.getlist("close_event_id")
    contracts_closed = source.getlist("close_event_contracts_closed")
    actual_exit_values = source.getlist("close_event_actual_exit_value")
    close_methods = source.getlist("close_event_method")
    event_datetimes = source.getlist("close_event_event_datetime")
    notes_exit = source.getlist("close_event_notes_exit")
    row_count = max(len(row_ids), len(contracts_closed), len(actual_exit_values), len(close_methods), len(event_datetimes), len(notes_exit), 0)
    rows: list[Dict[str, Any]] = []
    for index in range(row_count):
        row = {
            "id": row_ids[index] if index < len(row_ids) else "",
            "contracts_closed": contracts_closed[index] if index < len(contracts_closed) else "",
            "actual_exit_value": actual_exit_values[index] if index < len(actual_exit_values) else "",
            "close_method": close_methods[index] if index < len(close_methods) else "",
            "event_datetime": event_datetimes[index] if index < len(event_datetimes) else "",
            "notes_exit": notes_exit[index] if index < len(notes_exit) else "",
        }
        if row["id"] in {None, ""} and all(row[key] in {None, ""} for key in ("contracts_closed", "actual_exit_value", "close_method")):
            continue
        rows.append(row)
    return rows


def merge_trade_form_values(base_values: Dict[str, Any], override_values: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base_values)
    for key in TRADE_FORM_FIELDS:
        if key in override_values and override_values.get(key) not in {None, ""}:
            merged[key] = override_values.get(key)
    merged["trade_mode"] = override_values.get("trade_mode") or merged.get("trade_mode")
    merged["journal_name"] = override_values.get("journal_name") or merged.get("journal_name") or JOURNAL_NAME_DEFAULT
    return merged


def is_apollo_prefill_submission(values: Dict[str, Any]) -> bool:
    return str(values.get("prefill_source") or "").strip().lower() == "apollo"


def get_trade_prefill_metadata(values: Dict[str, Any]) -> Dict[str, str]:
    prefill_source = str(values.get("prefill_source") or "").strip().lower()
    if prefill_source == "kairos-live":
        return {
            "title": "Review Kairos Live Draft",
            "notice": "Kairos live trade data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
        }
    if prefill_source == "kairos-candidate":
        return {
            "title": "Review Kairos Candidate Draft",
            "notice": "Kairos candidate data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
        }
    if prefill_source == "apollo":
        return {
            "title": "Review Apollo Draft",
            "notice": "Apollo candidate data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
        }
    return {
        "title": "Review Prefilled Draft",
        "notice": "Prefilled trade data is loaded into this draft. Review any field, then click Save Trade to write it to the journal.",
    }


def coerce_apollo_trade_input(source: Any, trade_mode: Optional[str] = None) -> Dict[str, Any]:
    timestamp = current_timestamp()
    resolved_trade_mode = normalize_trade_mode(trade_mode or source.get("trade_mode") or "simulated")
    candidate_profile = str(source.get("candidate_profile") or "Apollo").strip()
    structure_grade = str(source.get("structure_grade") or "").strip()
    macro_grade = str(source.get("macro_grade") or "").strip()

    return {
        "trade_number": source.get("trade_number") or "",
        "trade_mode": resolved_trade_mode,
        "system_name": "Apollo",
        "journal_name": JOURNAL_NAME_DEFAULT,
        "system_version": "4.3",
        "candidate_profile": candidate_profile,
        "status": "open",
        "trade_date": timestamp.split("T", 1)[0],
        "entry_datetime": timestamp,
        "expiration_date": source.get("expiration_date") or "",
        "underlying_symbol": source.get("underlying_symbol") or "SPX",
        "spx_at_entry": source.get("spx_at_entry") or "",
        "vix_at_entry": source.get("vix_at_entry") or "",
        "structure_grade": structure_grade,
        "macro_grade": macro_grade,
        "expected_move": source.get("expected_move") or "",
        "expected_move_used": source.get("expected_move_used") or source.get("expected_move") or "",
        "expected_move_source": source.get("expected_move_source") or "",
        "option_type": source.get("option_type") or "Put Credit Spread",
        "short_strike": source.get("short_strike") or "",
        "long_strike": source.get("long_strike") or "",
        "spread_width": source.get("spread_width") or "",
        "contracts": source.get("contracts") or "",
        "candidate_credit_estimate": source.get("candidate_credit_estimate") or "",
        "actual_entry_credit": source.get("actual_entry_credit") or source.get("candidate_credit_estimate") or "",
        "distance_to_short": source.get("distance_to_short") or "",
        "em_multiple_floor": source.get("em_multiple_floor") or "",
        "percent_floor": source.get("percent_floor") or "",
        "boundary_rule_used": source.get("boundary_rule_used") or "",
        "actual_distance_to_short": source.get("actual_distance_to_short") or source.get("distance_to_short") or "",
        "actual_em_multiple": source.get("actual_em_multiple") or "",
        "fallback_used": source.get("fallback_used") or "no",
        "fallback_rule_name": source.get("fallback_rule_name") or "",
        "short_delta": source.get("short_delta") or "",
        "notes_entry": "Prefilled from Apollo candidate card.",
        "prefill_source": "apollo",
        "exit_datetime": "",
        "spx_at_exit": "",
        "actual_exit_value": "",
        "close_method": "",
        "close_reason": "",
        "notes_exit": "",
    }


def coerce_kairos_trade_input(source: Any, trade_mode: Optional[str] = None) -> Dict[str, Any]:
    timestamp = current_timestamp()
    resolved_trade_mode = normalize_trade_mode(trade_mode or source.get("trade_mode") or "simulated")
    candidate_profile = str(source.get("candidate_profile") or "Kairos").strip()
    structure_grade = str(source.get("structure_grade") or "").strip()
    macro_grade = str(source.get("macro_grade") or "").strip()

    return {
        "trade_number": source.get("trade_number") or "",
        "trade_mode": resolved_trade_mode,
        "system_name": "Kairos",
        "journal_name": source.get("journal_name") or JOURNAL_NAME_DEFAULT,
        "system_version": source.get("system_version") or "4.0",
        "candidate_profile": candidate_profile,
        "status": "open",
        "trade_date": timestamp.split("T", 1)[0],
        "entry_datetime": timestamp,
        "expiration_date": source.get("expiration_date") or timestamp.split("T", 1)[0],
        "underlying_symbol": source.get("underlying_symbol") or "SPX",
        "spx_at_entry": source.get("spx_at_entry") or "",
        "vix_at_entry": source.get("vix_at_entry") or "",
        "structure_grade": structure_grade,
        "macro_grade": macro_grade,
        "expected_move": source.get("expected_move") or "",
        "expected_move_used": source.get("expected_move_used") or source.get("expected_move") or "",
        "expected_move_source": source.get("expected_move_source") or "same_day_atm_straddle",
        "option_type": source.get("option_type") or "Put Credit Spread",
        "short_strike": source.get("short_strike") or "",
        "long_strike": source.get("long_strike") or "",
        "spread_width": source.get("spread_width") or "",
        "contracts": source.get("contracts") or "",
        "candidate_credit_estimate": source.get("candidate_credit_estimate") or "",
        "actual_entry_credit": source.get("actual_entry_credit") or source.get("candidate_credit_estimate") or "",
        "distance_to_short": source.get("distance_to_short") or "",
        "em_multiple_floor": source.get("em_multiple_floor") or "",
        "percent_floor": source.get("percent_floor") or "",
        "boundary_rule_used": source.get("boundary_rule_used") or "",
        "actual_distance_to_short": source.get("actual_distance_to_short") or source.get("distance_to_short") or "",
        "actual_em_multiple": source.get("actual_em_multiple") or "",
        "fallback_used": source.get("fallback_used") or "no",
        "fallback_rule_name": source.get("fallback_rule_name") or "",
        "short_delta": source.get("short_delta") or "",
        "notes_entry": source.get("notes_entry") or "Prefilled from Kairos candidate card.",
        "prefill_source": "kairos-candidate",
        "exit_datetime": "",
        "spx_at_exit": "",
        "actual_exit_value": "",
        "close_method": "",
        "close_reason": "",
        "notes_exit": "",
    }


def build_trade_page_context(
    store: TradeStore,
    trade_mode: str,
    form_values: Dict[str, Any],
    form_action: str,
    form_title: str,
    editing_trade_id: Optional[int],
    error_message: Optional[str],
    info_message: Optional[Dict[str, str]],
    prefill_active: bool = False,
    prefill_notice: str = "",
    import_preview: Optional[Dict[str, Any]] = None,
    import_journal_name: str = JOURNAL_NAME_DEFAULT,
    editing_trade: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_mode = resolve_trade_mode(trade_mode)
    trades = [build_trade_row_payload(item) for item in store.list_trades(normalized_mode)]
    summary = store.summarize(normalized_mode)
    return {
        "trade_mode": normalized_mode,
        "trade_mode_label": TRADE_MODE_LABELS[normalized_mode],
        "trade_mode_description": TRADE_MODE_DESCRIPTIONS[normalized_mode],
        "trade_modes": [{"key": key, "label": label} for key, label in TRADE_MODE_LABELS.items()],
        "trade_statuses": TRADE_STATUS_OPTIONS,
        "candidate_profiles": TRADE_PROFILE_OPTIONS,
        "option_type_options": TRADE_OPTION_TYPE_OPTIONS,
        "system_name_options": TRADE_SYSTEM_OPTIONS,
        "filter_groups": TRADE_FILTER_GROUPS,
        "journal_name_options": TRADE_JOURNAL_OPTIONS,
        "summary_metrics": [
            {"label": "Total trades", "value": format_value(summary["total_trades"])},
            {"label": "Open trades", "value": format_value(summary["open_trades"])},
            {"label": "Closed trades", "value": format_value(summary["closed_trades"])},
            {"label": "Total P/L", "value": format_currency(summary["total_pnl"])},
            {"label": "Average P/L", "value": format_currency(summary["average_pnl"])},
            {"label": "Wins", "value": format_value(summary["win_count"])},
            {"label": "Losses", "value": format_value(summary["loss_count"])},
        ],
        "trades": trades,
        "form_values": prepare_trade_form_values(form_values),
        "distance_field_meta": build_distance_field_context(form_values),
        "expected_move_field_meta": build_expected_move_field_context(form_values),
        "total_max_loss_field": build_total_max_loss_field_context(form_values),
        "form_action": form_action,
        "form_title": form_title,
        "editing_trade_id": editing_trade_id,
        "editing_trade": build_trade_detail_payload(editing_trade) if editing_trade else None,
        "error_message": error_message,
        "info_message": info_message,
        "prefill_active": prefill_active,
        "prefill_notice": prefill_notice,
        "import_preview": import_preview,
        "import_journal_name": import_journal_name,
    }


def prepare_trade_form_values(values: Dict[str, Any]) -> Dict[str, str]:
    prepared = dict(values)
    for field in ("entry_datetime", "exit_datetime"):
        prepared[field] = format_datetime_local_input(prepared.get(field))
    for field in TRADE_FORM_FIELDS:
        prepared.setdefault(field, "")
    prepared["system_name"] = normalize_system_name(prepared.get("system_name"))
    prepared["candidate_profile"] = normalize_candidate_profile(prepared.get("candidate_profile"))
    expected_move_metadata = resolve_trade_expected_move(prepared)
    prepared["expected_move"] = (
        format_value(expected_move_metadata.get("value")) if expected_move_metadata.get("value") is not None else ""
    )
    prepared["expected_move_source"] = expected_move_metadata.get("source") or ""
    distance_metadata = resolve_trade_distance(prepared)
    prepared["distance_to_short"] = (
        format_value(distance_metadata.get("value")) if distance_metadata.get("value") is not None else ""
    )
    return prepared


def build_expected_move_field_context(values: Dict[str, Any]) -> Dict[str, Any]:
    expected_move_metadata = resolve_trade_expected_move(values)
    source = normalize_expected_move_source(expected_move_metadata.get("source"))
    if source == "recovered_candidate":
        note = "Recovered from Kairos entry inputs using the stored daily move anchor formula."
    elif source == "recovered_snapshot":
        note = "Recovered from a matching stored import snapshot."
    elif source == "estimated_calibrated":
        note = "Estimated from SPX and VIX entry values, then calibration-adjusted before learning and safety metrics use it."
    elif source == "original":
        note = "Stored journal expected move retained as the authoritative value."
    else:
        note = "Expected move unresolved. Add the original value or preserve SPX and VIX entry data for recovery/backfill."
    return {
        "value": expected_move_metadata.get("value"),
        "source": source,
        "estimated": bool(expected_move_metadata.get("estimated")),
        "note": note,
    }


def build_trade_save_status(trade: Dict[str, Any], *, action: str) -> tuple[str, str]:
    expected_move_metadata = resolve_trade_expected_move(trade)
    if expected_move_metadata.get("value") is None:
        return (
            f"Trade {action}, but expected move is still unresolved. Safety-ratio analytics will exclude it until the field is recovered or entered.",
            "warning",
        )
    return (f"Trade {action} successfully.", "info")


def build_total_max_loss_field_context(values: Dict[str, Any]) -> Dict[str, Any]:
    credit_model = resolve_trade_credit_model(values)
    total_max_loss = credit_model.get("max_theoretical_risk")
    return {
        "value": total_max_loss,
        "value_display": format_currency(total_max_loss) if total_max_loss is not None else "—",
        "note": "System-calculated from spread width minus net credit, multiplied by contracts and 100. This field is saved automatically and cannot be edited.",
    }


def build_distance_field_context(values: Dict[str, Any]) -> Dict[str, Any]:
    distance_metadata = resolve_trade_distance(values)
    source = distance_metadata.get("source")
    if source == "derived":
        note = "System-derived from SPX at entry and short strike."
    elif source == "estimated_fallback":
        note = "Apollo fallback estimate from the SPX close on the trading day before expiration."
    elif source == "original":
        note = "Stored journal distance retained as the authoritative value."
        if distance_metadata.get("discrepancy_is_material"):
            note = "Stored journal distance retained. Current entry inputs differ materially from that journal value."
    else:
        note = "Distance unresolved. Add SPX at entry and short strike, or run Apollo history backfill for older trades."
    return {
        "value": distance_metadata.get("value"),
        "source": source,
        "estimated": bool(distance_metadata.get("estimated")),
        "has_material_discrepancy": bool(distance_metadata.get("discrepancy_is_material")),
        "note": note,
    }


def build_trade_row_payload(trade: Dict[str, Any]) -> Dict[str, Any]:
    result_label = derive_trade_result(trade)
    result_filter = normalize_result_filter_value(result_label)
    derived_status_raw = str(trade.get("derived_status_raw") or trade.get("status") or "open").strip().lower()
    original_contracts = trade.get("original_contracts") if trade.get("original_contracts") is not None else trade.get("contracts")
    closed_contracts = trade.get("contracts_closed") if trade.get("contracts_closed") is not None else trade.get("closed_contracts")
    remaining_contracts = trade.get("contracts_remaining") if trade.get("contracts_remaining") is not None else trade.get("remaining_contracts")
    realized_pnl = trade.get("realized_pnl") if trade.get("realized_pnl") is not None else trade.get("gross_pnl")
    distance_metadata = resolve_trade_distance(trade)
    total_max_loss = trade.get("total_max_loss")
    if total_max_loss is None:
        total_max_loss = trade.get("max_theoretical_risk")
    if total_max_loss is None:
        total_max_loss = trade.get("max_loss") if trade.get("max_loss") is not None else trade.get("max_risk")
    return {
        "id": trade.get("id"),
        "trade_number": format_value(trade.get("trade_number")),
        "trade_number_raw": trade.get("trade_number") or 0,
        "status": str(trade.get("derived_status_label") or trade.get("status") or "—").title(),
        "status_raw": derived_status_raw,
        "candidate_profile": normalize_candidate_profile(trade.get("candidate_profile")),
        "system_name": normalize_system_name(trade.get("system_name")),
        "system_version": trade.get("system_version") or "—",
        "trade_date": trade.get("trade_date") or "—",
        "trade_date_raw": trade.get("trade_date") or "",
        "expiration_date": trade.get("expiration_date") or "—",
        "expiration_date_raw": trade.get("expiration_date") or "",
        "underlying_symbol": trade.get("underlying_symbol") or "—",
        "underlying_symbol_raw": str(trade.get("underlying_symbol") or "").upper(),
        "strike_pair": build_strike_pair_label(trade),
        "strike_pair_raw": build_strike_pair_sort_value(trade),
        "contracts": format_value(original_contracts),
        "contracts_raw": original_contracts if original_contracts is not None else "",
        "closed_contracts": format_value(closed_contracts),
        "closed_contracts_raw": closed_contracts if closed_contracts is not None else 0,
        "remaining_contracts": remaining_contracts if remaining_contracts is not None else trade.get("contracts"),
        "remaining_contracts_raw": remaining_contracts if remaining_contracts is not None else trade.get("contracts") or 0,
        "actual_entry_credit": format_value(trade.get("actual_entry_credit")),
        "actual_entry_credit_raw": trade.get("actual_entry_credit") if trade.get("actual_entry_credit") is not None else "",
        "actual_exit_value": build_trade_exit_display(trade),
        "actual_exit_value_raw": trade.get("weighted_exit_value") if trade.get("weighted_exit_value") is not None else (trade.get("actual_exit_value") if trade.get("actual_exit_value") is not None else ""),
        "total_max_loss": format_currency(total_max_loss),
        "total_max_loss_raw": total_max_loss if total_max_loss is not None else "",
        "distance_to_short": format_value(distance_metadata.get("value")),
        "distance_to_short_raw": distance_metadata.get("value") if distance_metadata.get("value") is not None else "",
        "distance_source": distance_metadata.get("source") or trade.get("distance_source"),
        "gross_pnl": format_currency(realized_pnl),
        "gross_pnl_raw": realized_pnl if realized_pnl is not None else "",
        "max_risk": format_currency(trade.get("max_risk")),
        "roi_on_risk": format_ratio_percent(trade.get("roi_on_risk")),
        "roi_on_risk_raw": trade.get("roi_on_risk") if trade.get("roi_on_risk") is not None else "",
        "win_loss_result": result_label,
        "result_filter": result_filter,
        "result_sort": result_filter or str(result_label or "").lower(),
        "result_class": build_result_cell_class(result_filter),
        "notes_entry": trade.get("notes_entry") or "",
        "notes_exit": trade.get("notes_exit") or "",
        "trade_mode": trade.get("trade_mode") or "real",
        "journal_name": trade.get("journal_name") or JOURNAL_NAME_DEFAULT,
        "journal_name_raw": str(trade.get("journal_name") or JOURNAL_NAME_DEFAULT).lower(),
        "close_reason": trade.get("close_reason") or "",
    }


def build_trade_import_preview(store: TradeStore, preview_source: Dict[str, Any]) -> Dict[str, Any]:
    preview_rows = []
    importable_rows = []
    seen_signatures: set[tuple[Any, ...]] = set()

    for row in preview_source.get("rows", []):
        status = row.get("status") or "invalid"
        messages = list(row.get("messages") or [])
        payload = row.get("payload")

        if status == "ready" and isinstance(payload, dict):
            signature = build_trade_duplicate_signature(payload, already_normalized=True)
            if signature in seen_signatures:
                status = "duplicate"
                messages.append("Duplicate row inside this import file.")
            elif store.find_duplicate_trade(payload):
                status = "duplicate"
                messages.append("Matches an existing trade already saved in this journal.")
            else:
                seen_signatures.add(signature)
                importable_rows.append(payload)

        preview_rows.append(
            {
                "row_number": row.get("row_number"),
                "status": status,
                "status_label": {
                    "ready": "Ready",
                    "duplicate": "Duplicate",
                    "invalid": "Needs Review",
                }.get(status, "Needs Review"),
                "status_class": {
                    "ready": "good",
                    "duplicate": "warning",
                    "invalid": "error",
                }.get(status, "error"),
                "candidate_profile": format_value((payload or {}).get("candidate_profile")),
                "trade_date": format_value((payload or {}).get("trade_date")),
                "expiration_date": format_value((payload or {}).get("expiration_date")),
                "symbol": format_value((payload or {}).get("underlying_symbol")),
                "strike_pair": build_strike_pair_label(payload or {}),
                "contracts": format_value((payload or {}).get("contracts")),
                "actual_entry_credit": format_value((payload or {}).get("actual_entry_credit")),
                "actual_exit_value": format_value((payload or {}).get("actual_exit_value")),
                "journal_name": format_value((payload or {}).get("journal_name")),
                "messages": messages,
                "mapped_fields": row.get("mapped_fields") or [],
            }
        )

    return {
        "file_name": preview_source.get("file_name") or "Import file",
        "journal_name": (importable_rows[0].get("journal_name") if importable_rows else None) or JOURNAL_NAME_DEFAULT,
        "recognized_columns": preview_source.get("recognized_columns") or [],
        "rows": preview_rows,
        "row_count": len(preview_rows),
        "ready_count": sum(1 for row in preview_rows if row["status"] == "ready"),
        "duplicate_count": sum(1 for row in preview_rows if row["status"] == "duplicate"),
        "invalid_count": sum(1 for row in preview_rows if row["status"] == "invalid"),
        "importable_rows": importable_rows,
    }


def build_strike_pair_label(trade: Dict[str, Any]) -> str:
    short_strike = format_value(trade.get("short_strike"))
    long_strike = format_value(trade.get("long_strike"))
    if short_strike == "—" and long_strike == "—":
        return "—"
    return f"{short_strike} / {long_strike}"


def build_strike_pair_sort_value(trade: Dict[str, Any]) -> str:
    short_strike = _coerce_float(trade.get("short_strike"))
    long_strike = _coerce_float(trade.get("long_strike"))
    if short_strike is None and long_strike is None:
        return ""
    short_label = f"{short_strike:012.2f}" if short_strike is not None else "000000000.00"
    long_label = f"{long_strike:012.2f}" if long_strike is not None else "000000000.00"
    return f"{short_label}:{long_label}"


def derive_trade_result(trade: Dict[str, Any]) -> str:
    current_status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
    if current_status == "reduced":
        return "Reduced"
    if current_status == "open":
        return "Open"
    classified_result = classify_closed_trade_outcome(
        gross_pnl=trade.get("gross_pnl") if trade.get("gross_pnl") is not None else trade.get("pnl"),
        max_theoretical_risk=trade.get("total_max_loss") if trade.get("total_max_loss") is not None else trade.get("max_theoretical_risk"),
        explicit_result=trade.get("win_loss_result") or trade.get("result"),
        close_reason=trade.get("close_reason"),
    )
    if classified_result == "Flat":
        return "Scratched"
    if classified_result:
        return classified_result
    return "—"


def build_trade_exit_display(trade: Dict[str, Any]) -> str:
    status = str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower()
    weighted_exit = trade.get("weighted_exit_value") if trade.get("weighted_exit_value") is not None else trade.get("actual_exit_value")
    if status == "reduced":
        if weighted_exit is None:
            return "Partial"
        return f"Partial @ {format_value(weighted_exit)}"
    if status == "open":
        return "—"
    return format_value(weighted_exit)


def build_trade_detail_payload(trade: Dict[str, Any]) -> Dict[str, Any]:
    if not trade:
        return {}
    close_events = trade.get("close_events") or []
    if trade.get("original_contracts") is None or trade.get("contracts_closed") is None or trade.get("contracts_remaining") is None:
        summary = summarize_trade_close_events(trade, close_events)
    else:
        summary = trade
    distance_metadata = resolve_trade_distance(summary)
    total_max_loss = trade.get("total_max_loss")
    if total_max_loss is None:
        total_max_loss = trade.get("max_theoretical_risk")
    if total_max_loss is None:
        total_max_loss = trade.get("max_loss") if trade.get("max_loss") is not None else trade.get("max_risk")
    return {
        "trade_number": trade.get("trade_number"),
        "status": str(summary.get("derived_status_label") or summary.get("status") or "—").title(),
        "result": derive_trade_result(summary),
        "original_contracts": format_value(summary.get("original_contracts") if summary.get("original_contracts") is not None else summary.get("contracts")),
        "original_contracts_raw": summary.get("original_contracts") if summary.get("original_contracts") is not None else summary.get("contracts") or 0,
        "closed_contracts": format_value(summary.get("contracts_closed") if summary.get("contracts_closed") is not None else summary.get("closed_contracts")),
        "closed_contracts_raw": summary.get("contracts_closed") if summary.get("contracts_closed") is not None else summary.get("closed_contracts") or 0,
        "remaining_contracts": format_value(summary.get("contracts_remaining") if summary.get("contracts_remaining") is not None else summary.get("remaining_contracts")),
        "remaining_contracts_raw": summary.get("contracts_remaining") if summary.get("contracts_remaining") is not None else summary.get("remaining_contracts") or 0,
        "realized_pnl": format_currency(summary.get("realized_pnl") if summary.get("realized_pnl") is not None else summary.get("gross_pnl")),
        "realized_pnl_raw": summary.get("realized_pnl") if summary.get("realized_pnl") is not None else summary.get("gross_pnl") or 0,
        "total_max_loss": format_currency(total_max_loss),
        "total_max_loss_raw": total_max_loss if total_max_loss is not None else 0,
        "entry_credit_raw": trade.get("actual_entry_credit") if trade.get("actual_entry_credit") is not None else 0,
        "exit_display": build_trade_exit_display(summary),
        "distance_to_short": format_value(distance_metadata.get("value")),
        "distance_source": distance_metadata.get("source"),
        "distance_is_estimated": bool(distance_metadata.get("estimated")),
        "close_events": [
            {
                "id": format_value(event.get("id")),
                "contracts_closed": format_value(event.get("contracts_closed")),
                "contracts_closed_value": format_value(event.get("contracts_closed")),
                "actual_exit_value": format_value(event.get("actual_exit_value")),
                "actual_exit_value_value": format_value(event.get("actual_exit_value")),
                "event_datetime": format_datetime_local_input(event.get("event_datetime")) or format_value(event.get("event_datetime")),
                "event_datetime_value": format_datetime_local_input(event.get("event_datetime")) or "",
                "close_method": format_value(event.get("close_method") or event.get("event_type") or "Reduce"),
                "close_method_value": format_value(event.get("close_method") or event.get("event_type") or "Reduce"),
                "notes_exit": format_value(event.get("notes_exit")),
                "notes_exit_value": format_value(event.get("notes_exit")) if event.get("notes_exit") not in {None, "—"} else "",
            }
            for event in close_events
        ],
    }


def build_edit_trade_preview(existing_trade: Dict[str, Any], form_values: Dict[str, Any], close_events: list[Dict[str, Any]]) -> Dict[str, Any]:
    preview = dict(existing_trade or {})
    for key in TRADE_FORM_FIELDS:
        if key in form_values:
            preview[key] = form_values.get(key)
    preview["total_max_loss"] = resolve_trade_credit_model(preview).get("max_theoretical_risk")
    preview["close_events"] = close_events
    preview.update(summarize_trade_close_events(preview, close_events))
    return preview


def build_manage_trade_close_prefill(record: Dict[str, Any]) -> Dict[str, Any]:
    close_timestamp = current_timestamp()
    return {
        "id": "",
        "contracts_closed": str(int(record.get("contracts") or 0)),
        "actual_exit_value": f"{float(record.get('current_spread_mark') or 0.0):.2f}",
        "close_method": "Manage Trade Prefill",
        "event_datetime": close_timestamp,
        "notes_exit": (
            f"Prefilled from Manage Trades at {record.get('evaluated_at_display') or close_timestamp} "
            f"using current total close cost {record.get('current_total_close_cost_display') or '—'}."
        ),
    }


def apply_trade_close_prefill(trade: Dict[str, Any], close_prefill: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not close_prefill:
        return trade
    preview = dict(trade or {})
    existing_events = [dict(item) for item in (preview.get("close_events") or [])]
    prefill_timestamp = str(close_prefill.get("event_datetime") or "").strip()
    prefill_method = str(close_prefill.get("close_method") or "").strip().lower()
    if any(
        str(item.get("id") or "").strip() == ""
        and str(item.get("event_datetime") or "").strip() == prefill_timestamp
        and str(item.get("close_method") or "").strip().lower() == prefill_method
        for item in existing_events
    ):
        return preview
    existing_events.append(dict(close_prefill))
    preview["close_events"] = existing_events
    preview.update(summarize_trade_close_events(preview, existing_events))
    return preview


def normalize_result_filter_value(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == "black swan":
        return "black-swan"
    if text == "win":
        return "win"
    if text == "loss":
        return "loss"
    return ""


def build_result_cell_class(result_filter: str) -> str:
    return {
        "win": "trade-result-win",
        "loss": "trade-result-loss",
        "black-swan": "trade-result-black-swan",
    }.get(result_filter, "")


def resolve_trade_mode(trade_mode: str) -> str:
    try:
        return normalize_trade_mode(trade_mode)
    except ValueError as exc:
        raise abort(404) from exc


def format_datetime_local_input(value: Any) -> str:
    if value in {None, ""}:
        return ""
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError:
        return text
    if timestamp.tzinfo is not None:
        timestamp = timestamp.astimezone(CHICAGO_TZ)
    return timestamp.strftime("%Y-%m-%dT%H:%M")


def build_diagnostic_message(
    query: Optional[QueryRequest],
    error_message: str,
    service: MarketDataService,
) -> Optional[str]:
    """Build a short on-screen diagnostic note for historical Schwab failures."""
    if query is None:
        return None
    if query.definition.action == "latest":
        return None

    provider_metadata = service.get_provider_metadata()
    provider_name = provider_metadata.get("provider_name", "")
    if query.definition.ticker == "^GSPC":
        provider_name = provider_metadata.get("spx_historical_provider_name", provider_name)
    elif query.definition.ticker == "^VIX":
        provider_name = provider_metadata.get("vix_historical_provider_name", provider_name)

    if provider_name != "Schwab":
        return None

    if "historical" in error_message.lower() or "schwab rejected the historical request" in error_message.lower():
        return f"Historical lookup diagnostic: {error_message}"

    return None


def execute_apollo_precheck(apollo_service: ApolloService, trigger_source: str) -> Dict[str, Any]:
    """Run Apollo once and convert the result into a template-ready payload."""
    return build_apollo_result_payload(
        apollo_service.run_precheck(),
        trigger_source=trigger_source,
    )


def get_apollo_trigger_source() -> Optional[str]:
    """Return the Apollo trigger source for the current request."""
    if request.method == "POST":
        return "button"
    if is_local_dev_request() and str(request.args.get("autorun", "")).strip().lower() in {"1", "true", "yes", "on"}:
        return "autorun URL"
    return None


def is_local_dev_request() -> bool:
    """Return whether the current request is coming from a local development host."""
    host = (request.host.split(":", 1)[0] if request.host else "").lower()
    return bool(app.testing or host in LOCAL_DEV_HOSTS)


def build_apollo_candidate_prefill_fields(
    item: Dict[str, Any],
    *,
    spx: Dict[str, Any],
    vix: Dict[str, Any],
    macro: Dict[str, Any],
    structure: Dict[str, Any],
    option_chain: Dict[str, Any],
    trade_candidates: Dict[str, Any],
) -> Dict[str, Any]:
    candidate_profile = {
        "fortress": "Fortress",
        "standard": "Standard",
        "aggressive": "Aggressive",
    }.get(str(item.get("mode_key") or "").strip().lower(), str(item.get("mode_label") or item.get("mode_descriptor") or "Apollo").split("(", 1)[0].strip() or "Apollo")

    contracts = item.get("adjusted_contract_size")
    if contracts in (None, ""):
        contracts = item.get("recommended_contract_size")
    if contracts in (None, ""):
        contracts = item.get("original_contract_size")

    return {
        "system_name": "Apollo",
        "journal_name": JOURNAL_NAME_DEFAULT,
        "system_version": "4.3",
        "candidate_profile": candidate_profile,
        "expiration_date": option_chain.get("expiration_date") or trade_candidates.get("expiration_date") or "",
        "underlying_symbol": option_chain.get("symbol_requested") or "SPX",
        "spx_at_entry": spx.get("value") or trade_candidates.get("underlying_price") or "",
        "vix_at_entry": vix.get("value") or "",
        "structure_grade": structure.get("grade") or "",
        "macro_grade": macro.get("grade") or "",
        "expected_move": item.get("expected_move") or trade_candidates.get("expected_move") or "",
        "expected_move_used": item.get("expected_move_used") or item.get("expected_move") or trade_candidates.get("expected_move") or "",
        "expected_move_source": item.get("expected_move_source") or "same_day_atm_straddle",
        "option_type": "Put Credit Spread",
        "short_strike": item.get("short_strike") or "",
        "long_strike": item.get("long_strike") or "",
        "spread_width": item.get("width") or "",
        "contracts": contracts or "",
        "candidate_credit_estimate": item.get("credit") or "",
        "actual_entry_credit": item.get("credit") or "",
        "distance_to_short": item.get("distance_points") or "",
        "em_multiple_floor": item.get("applied_em_multiple_floor") or item.get("target_em_multiple") or "",
        "percent_floor": item.get("percent_floor") or "",
        "boundary_rule_used": item.get("boundary_rule_used") or "",
        "actual_distance_to_short": item.get("actual_distance_to_short") or item.get("distance_points") or "",
        "actual_em_multiple": item.get("actual_em_multiple") or item.get("em_multiple") or "",
        "pass_type": item.get("pass_type") or "",
        "premium_per_contract": item.get("premium_per_contract") or "",
        "total_premium": item.get("total_premium") or item.get("premium_received_dollars") or "",
        "max_theoretical_risk": item.get("max_theoretical_risk") or item.get("max_loss") or "",
        "risk_efficiency": item.get("risk_efficiency") or "",
        "credit_efficiency_pct": item.get("credit_efficiency_pct") or "",
        "target_em": item.get("target_em") or item.get("target_em_multiple") or "",
        "fallback_used": "yes" if item.get("fallback_used") else "no",
        "fallback_rule_name": item.get("fallback_rule_name") or "",
        "short_delta": item.get("short_delta") or "",
        "notes_entry": "Prefilled from Apollo candidate card.",
        "prefill_source": "apollo",
    }


def build_apollo_result_payload(apollo_data: Dict[str, Any], trigger_source: Optional[str] = None) -> Dict[str, Any]:
    """Convert Apollo service output into a template-ready payload."""
    spx = apollo_data.get("spx") or {}
    vix = apollo_data.get("vix") or {}
    macro = apollo_data.get("macro") or {}
    structure = apollo_data.get("structure") or {}
    market_calendar = apollo_data.get("market_calendar") or {}
    option_chain = apollo_data.get("option_chain") or {}
    trade_candidates = apollo_data.get("trade_candidates") or {}
    option_chain_diagnostics = option_chain.get("request_diagnostics") or {}
    metrics = structure.get("metrics") or {}
    option_chain_failure_category = str(
        option_chain.get("failure_category") or option_chain_diagnostics.get("failure_category") or ""
    ).strip().lower()
    option_chain_failure_label = str(
        option_chain.get("failure_label") or option_chain_diagnostics.get("failure_label") or ""
    ).strip()
    option_chain_status = "Ready" if option_chain.get("success", False) else (option_chain_failure_label or "Unavailable")
    option_chain_status_class = (
        "good"
        if option_chain.get("success", False)
        else (option_chain.get("failure_status_class") or ("poor" if option_chain_failure_category == "malformed-request" else "not-available"))
    )
    outcome_profile = get_real_trade_outcome_profile()
    routine_loss_percentage = float(outcome_profile.get("routine_loss_percentage") or 0.0)
    black_swan_loss_percentage = float(outcome_profile.get("black_swan_loss_percentage") or 0.0)
    routine_loss_count = int(outcome_profile.get("loss_count") or 0)
    black_swan_loss_count = int(outcome_profile.get("black_swan_count") or 0)

    trade_candidate_items = []
    for index, item in enumerate(trade_candidates.get("candidates", [])):
        if not isinstance(item, dict):
            continue
        probability_labels = build_candidate_probability_labels(item.get("short_delta"), item.get("long_delta"))
        prefill_fields = build_apollo_candidate_prefill_fields(
            item,
            spx=spx,
            vix=vix,
            macro=macro,
            structure=structure,
            option_chain=option_chain,
            trade_candidates=trade_candidates,
        )
        trade_candidate_items.append(
            {
                "rank": item.get("rank", index + 1),
                "mode_key": str(item.get("mode_key", "mode")),
                "mode_label": str(item.get("mode_label", f"Mode {index + 1}")),
                "mode_descriptor": str(item.get("mode_descriptor", "")),
                "available": bool(item.get("available", True)),
                "no_trade_message": str(item.get("no_trade_message", "")),
                "strategy_label": str(item.get("strategy_label", "SPX put credit spread")),
                "position_label": "Put Spread",
                "score": int(item.get("score", 0) or 0),
                "short_strike": format_value(item.get("short_strike")),
                "long_strike": format_value(item.get("long_strike")),
                "width": format_value(item.get("width")),
                "credit": format_value(item.get("credit")),
                "net_credit": format_currency(item.get("credit"), decimals=2),
                "premium_per_contract": format_currency(item.get("premium_per_contract")),
                "premium_received_dollars": format_currency(item.get("premium_received_dollars")),
                "total_premium": format_currency(item.get("total_premium") if item.get("total_premium") is not None else item.get("premium_received_dollars")),
                "max_theoretical_risk": format_currency(item.get("max_theoretical_risk") if item.get("max_theoretical_risk") is not None else item.get("max_loss")),
                "risk_efficiency": format_ratio(item.get("risk_efficiency"), decimals=4),
                "credit_efficiency": append_percent(item.get("credit_efficiency_pct")),
                "routine_loss": format_currency(project_historical_loss(item.get("max_loss"), routine_loss_percentage)),
                "black_swan_loss": format_currency(project_historical_loss(item.get("max_loss"), black_swan_loss_percentage)),
                "routine_loss_percentage": format_ratio_percent(routine_loss_percentage),
                "black_swan_loss_percentage": format_ratio_percent(black_swan_loss_percentage),
                "routine_loss_count": routine_loss_count,
                "black_swan_loss_count": black_swan_loss_count,
                "loss_model_source": "Historical real-trade ROI averages only (ordinary losses exclude Black Swans)",
                "max_loss": format_currency(item.get("max_loss")),
                "max_loss_per_contract": format_value(item.get("max_loss_per_contract")),
                "risk_cap_dollars": format_currency(item.get("risk_cap_dollars")),
                "risk_cap_status": str(item.get("risk_cap_status", "—")),
                "risk_cap_adjusted": "Yes" if item.get("risk_cap_adjusted") else "No",
                "original_contract_size": format_value(item.get("original_contract_size")),
                "adjusted_contract_size": format_value(item.get("adjusted_contract_size")),
                "account_risk_percent": append_percent(item.get("account_risk_percent")),
                "exit_plan_applied": "Yes" if item.get("exit_plan_applied") else "No",
                "break_even": format_value(item.get("break_even")),
                "short_delta": format_value(item.get("short_delta")),
                "long_delta": format_value(item.get("long_delta")),
                "distance_points": format_value(item.get("distance_points")),
                "distance_to_short": format_value(item.get("distance_points")),
                "distance_percent": append_percent(item.get("distance_percent")),
                "expected_move": format_value(item.get("expected_move")),
                "expected_move_used": format_value(item.get("expected_move_used") or item.get("expected_move")),
                "expected_move_source": str(item.get("expected_move_source") or "same_day_atm_straddle"),
                "expected_move_1_5x_threshold": format_value(item.get("expected_move_1_5x_threshold")),
                "expected_move_2x_threshold": format_value(item.get("expected_move_2x_threshold")),
                "em_multiple": format_em_multiple(item.get("em_multiple")),
                "target_em": format_em_multiple(item.get("target_em") if item.get("target_em") is not None else item.get("target_em_multiple")),
                "applied_em_floor": format_em_multiple(item.get("applied_em_multiple_floor") or item.get("target_em_multiple")),
                "percent_floor": append_percent(item.get("percent_floor")),
                "percent_floor_points": format_value(item.get("percent_floor_points")),
                "em_floor_points": format_value(item.get("em_floor_points")),
                "hybrid_threshold": format_value(item.get("hybrid_distance_threshold")),
                "boundary_binding_source": str(item.get("boundary_binding_source") or "—"),
                "pass_type": str(item.get("pass_type") or ""),
                "pass_type_label": str(item.get("pass_type_label") or item.get("pass_type") or "Strict Pass"),
                "fallback_used": "Yes" if item.get("fallback_used") else "No",
                "fallback_rule_name": str(item.get("fallback_rule_name") or "—"),
                "economic_filter_status": (
                    "Passed" if item.get("available") else str(item.get("no_trade_message") or "—")
                ),
                "boundary_rule_used": str(item.get("boundary_rule_used") or "—"),
                "actual_distance_to_short": format_value(item.get("actual_distance_to_short") or item.get("distance_points")),
                "actual_em_multiple": format_em_multiple(item.get("actual_em_multiple") or item.get("em_multiple")),
                "expected_move_comparison": str(item.get("expected_move_comparison", "—")),
                "required_distance_rule_used": str(item.get("required_distance_rule_used", "—")),
                "active_em_rule": str(item.get("active_em_rule", "—")),
                "active_rule_set": str(item.get("active_rule_set", "—")),
                "recommended_contract_size": format_value(item.get("recommended_contract_size")),
                "recommended_contract_size_reason": str(item.get("recommended_contract_size_reason", "—")),
                "pricing_basis": str(item.get("pricing_basis") or "—"),
                "selection_variant": str(item.get("selection_variant") or "—"),
                "short_open_interest": format_value(item.get("short_open_interest")),
                "short_volume": format_value(item.get("short_volume")),
                "premium_probability": probability_labels["premium"],
                "routine_probability": probability_labels["routine"],
                "tail_probability": probability_labels["tail"],
                "max_probability": probability_labels["max"],
                "prefill_fields": prefill_fields,
                "rationale": item.get("rationale", []),
                "exit_plan": item.get("exit_plan", []),
                "diagnostics": item.get("diagnostics", []),
            }
        )
    trade_candidates_credit_map = build_trade_candidates_credit_map(option_chain=option_chain, trade_candidates=trade_candidates)

    raw_macro_events = macro.get("macro_events") or []
    macro_events = [
        {
            "title": str(item.get("title", "—")),
            "time": str(item.get("time", "Time unavailable")),
            "impact": "Major" if str(item.get("impact", "")).title() == "Major" else "Minor",
            "reason": str(item.get("reason", "")),
        }
        for item in raw_macro_events
        if isinstance(item, dict)
    ]
    macro_major_events = [item for item in macro_events if item["impact"] == "Major"]
    macro_minor_events = [item for item in macro_events if item["impact"] != "Major"]
    macro_grade = str(macro.get("grade", "None") or "None").title()
    macro_status_class = {
        "None": "good",
        "Minor": "neutral",
        "Major": "poor",
    }.get(macro_grade, "not-available")
    valid_trade_candidate_count = int(
        trade_candidates.get("valid_mode_count")
        or sum(1 for item in trade_candidate_items if item.get("available"))
    )
    trade_candidates_outcome_category = "ready" if valid_trade_candidate_count else ""
    trade_candidates_outcome_label = "Ready" if valid_trade_candidate_count else ""
    if option_chain.get("success", False) and not valid_trade_candidate_count:
        trade_candidates_outcome_category = "no-candidates"
        trade_candidates_outcome_label = "No candidates"

    return {
        "title": apollo_data.get("title", "Apollo Gate 1 -- SPX Structure"),
        "provider_name": apollo_data.get("provider_name", "Unknown Provider"),
        "status": str(apollo_data.get("apollo_status", "blocked")).title(),
        "status_class": str(apollo_data.get("apollo_status", "blocked")).lower(),
        "local_datetime": format_apollo_datetime(apollo_data.get("local_datetime")),
        "run_timestamp": format_apollo_datetime(apollo_data.get("local_datetime")),
        "spx_value": format_value(spx.get("value")) if spx else "—",
        "spx_as_of": spx.get("as_of", "—") if spx else "—",
        "vix_value": format_value(vix.get("value")) if vix else "—",
        "vix_as_of": vix.get("as_of", "—") if vix else "—",
        "macro_title": "Apollo Gate 2 -- Macro Event",
        "macro_source": macro.get("source_name", "MarketWatch (unavailable)"),
        "macro_grade": macro_grade,
        "macro_grade_class": macro_status_class,
        "macro_target_day": format_long_date(macro.get("target_date")),
        "macro_checked_at": format_apollo_datetime(macro.get("checked_at")),
        "macro_checked_dates": ", ".join(str(item) for item in (macro.get("checked_dates") or [])) or "—",
        "macro_available": "Yes" if macro.get("available", True) else "No",
        "macro_event_count": len(macro_events),
        "macro_major_detected": "Yes" if macro.get("has_major_macro") else "No",
        "macro_explanation": macro.get("explanation", "—"),
        "macro_diagnostic": macro.get("diagnostic") or {},
        "macro_source_attempts": [
            {
                "source": str(item.get("source", "Source")),
                "status": str(item.get("status", "unknown")),
                "detail": str(item.get("detail", "—")),
                "response_status": str(item.get("response_status", "—")),
                "final_url": str(item.get("final_url", "—")),
                "parser_strategy": str(item.get("parser_strategy", "—")),
                "event_count": item.get("event_count", 0),
                "failure_reason": str(item.get("failure_reason", "")),
                "body_snippet": str(item.get("body_snippet", "")),
            }
            for item in (macro.get("source_attempts") or [])
            if isinstance(item, dict)
        ],
        "macro_events": macro_events,
        "macro_major_events": macro_major_events,
        "macro_minor_events": macro_minor_events,
        "next_market_day": format_value(market_calendar.get("next_market_day")),
        "next_market_day_note": market_calendar.get("note", "—"),
        "holiday_filter_applied": market_calendar.get("holiday_filter_applied_label", "No"),
        "skipped_holiday_name": market_calendar.get("skipped_holiday_name") or "—",
        "candidate_date_considered": format_value(market_calendar.get("candidate_date_considered")),
        "structure_available": structure.get("available", False),
        "structure_preferred_source": structure.get("preferred_source", "Not available"),
        "structure_attempted_sources": structure.get("attempted_sources", []),
        "structure_fallback_reason": structure.get("fallback_reason", ""),
        "structure_source_used": structure.get("source_used", "Not available"),
        "structure_grade": structure.get("final_grade", structure.get("grade", "Not available")),
        "structure_grade_class": str(structure.get("final_grade", structure.get("grade", "not-available"))).lower().replace(" ", "-"),
        "structure_base_grade": structure.get("base_grade", structure.get("grade", "Not available")),
        "structure_final_grade": structure.get("final_grade", structure.get("grade", "Not available")),
        "structure_rsi_modifier": structure.get("rsi_modifier_label", "None"),
        "structure_rsi_value": format_value(structure.get("rsi_value")),
        "structure_rsi_note": structure.get("rsi_note") or "Daily RSI unavailable; base structure kept.",
        "structure_trend_classification": structure.get("trend_classification", "Not available"),
        "structure_damage_classification": structure.get("damage_classification", "Not available"),
        "structure_summary": structure.get("summary") or "—",
        "structure_message": structure.get("message") or "",
        "structure_session_note": structure.get("session_note") or "",
        "structure_rules": structure.get("rules") or [],
        "structure_chart": structure.get("chart") or {"available": False, "points": []},
        "structure_session_high": format_value(metrics.get("session_high")),
        "structure_session_low": format_value(metrics.get("session_low")),
        "structure_current_price": format_value(metrics.get("current_price")),
        "structure_range_position": append_percent((metrics.get("range_position") or 0) * 100) if metrics.get("range_position") is not None else "—",
        "structure_ema8": format_value(metrics.get("ema8")),
        "structure_ema21": format_value(metrics.get("ema21")),
        "structure_recent_price_action": metrics.get("recent_price_action", "—"),
        "structure_session_window": (
            f"{metrics.get('session_start', '—')} to {metrics.get('session_end', '—')}"
            if metrics.get("session_start") or metrics.get("session_end")
            else "—"
        ),
        "option_chain_success": option_chain.get("success", False),
        "option_chain_status": option_chain_status,
        "option_chain_status_class": option_chain_status_class,
        "option_chain_source": option_chain.get("source_name", "Schwab"),
        "option_chain_failure_category": option_chain_failure_category,
        "option_chain_failure_label": option_chain_failure_label or "—",
        "option_chain_heading_date": format_long_date(option_chain.get("expiration_date")),
        "option_chain_symbol_requested": option_chain.get("symbol_requested", "—"),
        "option_chain_expiration_target": format_value(option_chain.get("expiration_target")),
        "option_chain_expiration": format_value(option_chain.get("expiration_date")),
        "option_chain_expiration_count": option_chain.get("expiration_count", 0),
        "option_chain_puts_count": option_chain.get("puts_count", 0),
        "option_chain_calls_count": option_chain.get("calls_count", 0),
        "option_chain_rows_displayed": option_chain.get("rows_displayed", 0),
        "option_chain_display_puts_count": option_chain.get("display_puts_count", 0),
        "option_chain_display_calls_count": option_chain.get("display_calls_count", 0),
        "option_chain_min_premium_target": format_value(option_chain.get("min_premium_target")),
        "option_chain_rows_setting": option_chain.get("rows_setting", "Adaptive"),
        "option_chain_grouping": option_chain.get("grouping", "Puts ascending → Calls ascending"),
        "option_chain_strike_range": option_chain.get("strike_range", "—"),
        "option_chain_message": option_chain.get("message", "—"),
        "option_chain_preview_rows": option_chain.get("preview_rows", []),
        "option_chain_final_symbol": option_chain_diagnostics.get("final_symbol", option_chain.get("symbol_requested", "—")),
        "option_chain_final_expiration_sent": option_chain_diagnostics.get("final_expiration", format_value(option_chain.get("expiration_target"))),
        "option_chain_request_attempt_used": option_chain_diagnostics.get("attempt_used", "—"),
        "option_chain_raw_params_sent": option_chain_diagnostics.get("raw_params_sent", {}),
        "option_chain_error_detail": option_chain_diagnostics.get("error_detail") or option_chain.get("message", "—"),
        "option_chain_attempt_results": [
            {
                "label": item.get("label", "Attempt"),
                "status": item.get("status", "unknown"),
                "status_code": item.get("status_code", "—"),
                "params": item.get("params", {}),
                "error_detail": item.get("error_detail") or "—",
                "failure_category": item.get("failure_category") or "",
                "failure_label": item.get("failure_label") or "",
            }
            for item in option_chain_diagnostics.get("attempts", [])
            if isinstance(item, dict)
        ],
        "trade_candidates_title": "Apollo Gate 3 -- Engine",
        "trade_candidates_status": trade_candidates.get("status", "Stand Aside"),
        "trade_candidates_status_class": trade_candidates.get("status_class", "not-available"),
        "trade_candidates_message": trade_candidates.get("message", "No trade candidates were produced."),
        "trade_candidates_count": trade_candidates.get("candidate_count", 0),
        "trade_candidates_valid_count": valid_trade_candidate_count,
        "trade_candidates_outcome_category": trade_candidates_outcome_category,
        "trade_candidates_outcome_label": trade_candidates_outcome_label,
        "trade_candidates_count_label": trade_candidates.get("count_label") or format_candidate_count_label(trade_candidates.get("candidate_count", 0)),
        "trade_candidates_underlying_price": format_value(trade_candidates.get("underlying_price")),
        "trade_candidates_expected_move": format_value(trade_candidates.get("expected_move")),
        "trade_candidates_expected_move_range": trade_candidates.get("expected_move_range", "—"),
        "trade_candidates_diagnostics": {
            **(trade_candidates.get("diagnostics", {}) or {}),
            "baseline_distance_points_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("baseline_distance_points")),
            "baseline_max_short_strike_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("baseline_max_short_strike")),
            "expected_move_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("expected_move")),
            "expected_move_1_5x_threshold_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("expected_move_1_5x_threshold")),
            "expected_move_2x_threshold_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("expected_move_2x_threshold")),
            "active_barrier_points_display": format_value((trade_candidates.get("diagnostics", {}) or {}).get("active_barrier_points")),
            "account_value_display": format_currency((trade_candidates.get("diagnostics", {}) or {}).get("account_value")),
            "base_structure_grade": (trade_candidates.get("diagnostics", {}) or {}).get("base_structure_grade", "Not available"),
            "rsi_modifier_applied": (trade_candidates.get("diagnostics", {}) or {}).get("rsi_modifier_applied", "No"),
            "rsi_modifier_label": (trade_candidates.get("diagnostics", {}) or {}).get("rsi_modifier_label", "None"),
            "evaluated_spread_details": [
                {
                    "short_strike": format_value(item.get("short_strike")),
                    "long_strike": format_value(item.get("long_strike")),
                    "distance_points": format_value(item.get("distance_points")),
                    "distance_percent": append_percent(item.get("distance_percent")),
                    "baseline_em_pass": "Pass" if item.get("baseline_em_pass") else "Fail",
                    "baseline_max_short_strike": format_value(item.get("baseline_max_short_strike")),
                    "expected_move": format_value(item.get("expected_move")),
                    "expected_move_1_5x_threshold": format_value(item.get("expected_move_1_5x_threshold")),
                    "two_x_expected_move_threshold": format_value(item.get("two_x_expected_move_threshold")),
                    "required_distance_mode": item.get("required_distance_mode", "—"),
                    "active_rule_set": item.get("active_rule_set", "—"),
                    "macro_modifier_status": item.get("macro_modifier_status", "No"),
                    "structure_modifier_status": item.get("structure_modifier_status", "No"),
                    "net_credit": format_currency(item.get("net_credit"), decimals=2),
                    "premium_received_dollars": format_currency(item.get("premium_received_dollars")),
                    "max_loss_dollars": format_currency(item.get("max_loss_dollars")),
                    "risk_cap_dollars": format_currency(item.get("risk_cap_dollars")),
                    "risk_cap_status": item.get("risk_cap_status", "—"),
                    "original_contract_size": format_value(item.get("original_contract_size")),
                    "account_risk_percent": append_percent(item.get("account_risk_percent")),
                    "short_delta": format_value(item.get("short_delta")),
                    "contract_size_chosen": format_value(item.get("contract_size_chosen")),
                    "qualifies_for_full_size": "Yes" if item.get("qualifies_for_full_size") else "No",
                    "reject_reason": item.get("reject_reason", "—"),
                }
                for item in (trade_candidates.get("diagnostics", {}) or {}).get("evaluated_spread_details", [])
                if isinstance(item, dict)
            ],
        },
        "trade_candidates_short_barrier_put": format_short_barrier(
            option_chain=option_chain,
            trade_candidates=trade_candidates,
            side="put",
        ),
        "trade_candidates_short_barrier_call": format_short_barrier(
            option_chain=option_chain,
            trade_candidates=trade_candidates,
            side="call",
        ),
        "trade_candidates_credit_map": trade_candidates_credit_map,
        "trade_candidates_items": trade_candidate_items,
        "apollo_trigger_source": trigger_source or "",
        "apollo_trigger_note": (f"Apollo was triggered by {trigger_source}." if trigger_source else ""),
        "reasons": apollo_data.get("reasons", []),
    }


def format_apollo_datetime(value: Any) -> str:
    """Format Apollo's local datetime field for display."""
    if isinstance(value, datetime):
        localized = value.astimezone(CHICAGO_TZ) if value.tzinfo else value.replace(tzinfo=CHICAGO_TZ)
        return localized.strftime("%a %Y-%m-%d %I:%M %p %Z").replace(" 0", " ")
    return str(value) if value is not None else "—"


def format_candidate_count_label(value: Any) -> str:
    count = 0
    try:
        count = int(value or 0)
    except (TypeError, ValueError):
        count = 0
    return f"{count} Candidate" if count == 1 else f"{count} Candidates"


def format_currency(value: Any, decimals: int = 0) -> str:
    amount = _coerce_float(value)
    if amount is None:
        return "—"
    sign = "-" if amount < 0 else ""
    return f"{sign}${abs(amount):,.{decimals}f}"


def format_em_multiple(value: Any) -> str:
    multiple = _coerce_float(value)
    if multiple is None:
        return "—"
    return f"{multiple:,.2f}x EM"


def format_ratio(value: Any, decimals: int = 4) -> str:
    ratio = _coerce_float(value)
    if ratio is None:
        return "—"
    return f"{ratio:,.{decimals}f}"


def format_ratio_percent(value: Any) -> str:
    ratio = _coerce_float(value)
    if ratio is None:
        return "—"
    return f"{ratio * 100:,.1f}%"


def format_loss_range(low: Any, high: Any) -> str:
    low_value = _coerce_float(low)
    high_value = _coerce_float(high)
    if low_value is None or high_value is None:
        return "—"
    return f"${low_value:,.0f}–${high_value:,.0f}"


def get_real_trade_outcome_profile() -> Dict[str, Any]:
    trade_store = current_app.extensions.get("trade_store") if current_app else None
    if trade_store is None:
        return {
            "routine_loss_percentage": 0.0,
            "black_swan_loss_percentage": 0.0,
        }
    return trade_store.build_real_trade_outcome_profile()


def project_historical_loss(max_loss: Any, percentage: Any) -> float | None:
    max_loss_value = _coerce_float(max_loss)
    percentage_value = _coerce_float(percentage)
    if max_loss_value is None or percentage_value is None:
        return None
    return max_loss_value * abs(percentage_value)


def build_candidate_probability_labels(short_delta: Any, long_delta: Any) -> Dict[str, str]:
    short_delta_value = clamp_probability_fraction(short_delta)
    long_delta_value = clamp_probability_fraction(long_delta)

    premium_probability = None if short_delta_value is None else (1.0 - short_delta_value)
    routine_probability = None if short_delta_value is None or long_delta_value is None else (short_delta_value - long_delta_value)
    tail_probability = long_delta_value

    return {
        "premium": format_probability(premium_probability),
        "routine": format_probability(routine_probability),
        "tail": format_probability(tail_probability),
        "max": "<1%",
    }


def clamp_probability_fraction(value: Any) -> float | None:
    amount = _coerce_float(value)
    if amount is None:
        return None
    return max(0.0, min(1.0, amount))


def format_probability(value: Any) -> str:
    probability = clamp_probability_fraction(value)
    if probability is None:
        return "—"
    return f"{probability * 100:.0f}%"


def build_trade_candidates_credit_map(option_chain: Dict[str, Any], trade_candidates: Dict[str, Any]) -> Dict[str, Any]:
    candidates = [item for item in (trade_candidates.get("candidates") or []) if isinstance(item, dict) and item.get("available")]
    spot = _coerce_float(trade_candidates.get("underlying_price") or option_chain.get("underlying_price"))
    expected_move = _coerce_float(trade_candidates.get("expected_move"))
    put_barrier = compute_short_barrier_value(option_chain=option_chain, trade_candidates=trade_candidates, side="put")
    call_barrier = compute_short_barrier_value(option_chain=option_chain, trade_candidates=trade_candidates, side="call")
    lower_em = (spot - expected_move) if spot is not None and expected_move is not None else None
    upper_em = (spot + expected_move) if spot is not None and expected_move is not None else None

    if spot is None:
        return {"available": False, "guides": [], "markers": [], "regions": []}

    plot_left_x = 130.0
    plot_right_x = 920.0
    baseline_y = 164.0
    floor_y = 146.0
    peak_y = 46.0
    spot_line_top_y = 28.0

    candidate_ranges = []
    has_put_side_positions = False
    has_call_side_positions = False
    relevant_values = [value for value in [spot, put_barrier, lower_em, upper_em, call_barrier] if value is not None]

    for item in candidates:
        short_strike = _coerce_float(item.get("short_strike"))
        long_strike = _coerce_float(item.get("long_strike"))
        if short_strike is None or long_strike is None:
            continue
        low_strike = min(short_strike, long_strike)
        high_strike = max(short_strike, long_strike)
        candidate_ranges.append(
            {
                "mode_key": str(item.get("mode_key", "mode")),
                "mode_label": str(item.get("mode_label", "Mode")),
                "short_strike": short_strike,
                "long_strike": long_strike,
                "low_strike": low_strike,
                "high_strike": high_strike,
                "premium": _coerce_float(item.get("premium_received_dollars")) or 0.0,
            }
        )
        relevant_values.extend([short_strike, long_strike])
        if high_strike <= spot:
            has_put_side_positions = True
        if low_strike >= spot:
            has_call_side_positions = True

    if not relevant_values:
        return {"available": False, "guides": [], "markers": [], "regions": []}

    left_reach = max((spot - value) for value in relevant_values if value <= spot) if any(value <= spot for value in relevant_values) else 0.0
    right_reach = max((value - spot) for value in relevant_values if value >= spot) if any(value >= spot for value in relevant_values) else 0.0

    data_span = max(max(relevant_values) - min(relevant_values), 1.0)
    base_padding = max(data_span * 0.08, 8.0)
    left_span = max(left_reach + base_padding, 1.0)
    right_span = max(right_reach + base_padding, 1.0)
    if has_put_side_positions and not has_call_side_positions:
        right_span = max(right_span, left_span * 0.58)
    elif has_call_side_positions and not has_put_side_positions:
        left_span = max(left_span, right_span * 0.58)
    else:
        shared_span = max(left_span, right_span)
        left_span = shared_span
        right_span = shared_span

    left_bound = spot - left_span
    right_bound = spot + right_span
    if right_bound <= left_bound:
        left_bound = spot - 1.0
        right_bound = spot + 1.0

    def to_x(value: float | None) -> float:
        if value is None:
            return plot_left_x + ((plot_right_x - plot_left_x) / 2)
        proportion = (value - left_bound) / (right_bound - left_bound)
        proportion = max(0.0, min(1.0, proportion))
        return plot_left_x + ((plot_right_x - plot_left_x) * proportion)

    def build_profile_paths(spot_x: float) -> tuple[str, str]:
        left_span_px = max(spot_x - plot_left_x, 1.0)
        right_span_px = max(plot_right_x - spot_x, 1.0)
        line_path = (
            f"M{plot_left_x:.1f} {floor_y:.1f} "
            f"C{(plot_left_x + left_span_px * 0.42):.1f} {floor_y:.1f}, {(plot_left_x + left_span_px * 0.78):.1f} {(peak_y + 58.0):.1f}, {spot_x:.1f} {peak_y:.1f} "
            f"C{(spot_x + right_span_px * 0.22):.1f} {(peak_y + 58.0):.1f}, {(spot_x + right_span_px * 0.58):.1f} {floor_y:.1f}, {plot_right_x:.1f} {floor_y:.1f}"
        )
        area_path = f"{line_path} L{plot_right_x:.1f} {baseline_y:.1f} L{plot_left_x:.1f} {baseline_y:.1f} Z"
        return line_path, area_path

    max_premium = max((_coerce_float(item.get("premium_received_dollars")) or 0.0) for item in candidates) if candidates else 0.0
    max_premium = max(max_premium, 1.0)

    guides = []
    for ratio in (1.0, 0.5, 0.2):
        y = 154.0 - (ratio * 92.0)
        guides.append({"y": round(y, 1), "label": format_currency(max_premium * ratio)})
    guides.append({"y": 164.0, "label": "$0"})

    markers = []
    short_labels = {"standard": "S", "aggressive": "A", "fortress": "F"}
    spread_regions = []
    spot_x = round(to_x(spot), 1)
    profile_line_path, profile_area_path = build_profile_paths(spot_x)

    for item in candidate_ranges:
        mode_key = item["mode_key"]
        premium = item["premium"]
        short_strike = item["short_strike"]
        long_strike = item["long_strike"]
        low_x = round(to_x(item["low_strike"]), 1)
        high_x = round(to_x(item["high_strike"]), 1)
        y = 154.0 - ((premium / max_premium) * 92.0)
        markers.append(
            {
                "mode_key": mode_key,
                "mode_label": item["mode_label"],
                "marker_label": short_labels.get(mode_key, mode_key[:1].upper()),
                "short_strike_label": format_value(short_strike),
                "long_strike_label": format_value(long_strike),
                "premium_label": format_currency(premium),
                "x": round(to_x(short_strike), 1),
                "y": round(max(56.0, min(154.0, y)), 1),
            }
        )
        spread_regions.append(
            {
                "mode_key": mode_key,
                "mode_label": item["mode_label"],
                "short_strike_label": format_value(short_strike),
                "long_strike_label": format_value(long_strike),
                "x": low_x,
                "width": round(max(high_x - low_x, 3.0), 1),
                "short_x": round(to_x(short_strike), 1),
                "long_x": round(to_x(long_strike), 1),
            }
        )

    show_upside_reference = not (has_put_side_positions and not has_call_side_positions)
    barrier_markers = [
        {
            "key": "put-barrier",
            "label": "Put Barrier",
            "value_label": format_value(put_barrier),
            "x": round(to_x(put_barrier), 1),
            "line_top_y": 132.0,
            "line_bottom_y": 176.0,
            "show": put_barrier is not None,
            "css_class": "apollo-risk-reference-danger",
        },
        {
            "key": "em-lower",
            "label": "EM Lower",
            "value_label": format_value(lower_em),
            "x": round(to_x(lower_em), 1),
            "line_top_y": 124.0,
            "line_bottom_y": 176.0,
            "show": lower_em is not None,
            "css_class": "apollo-risk-reference-em",
        },
        {
            "key": "em-upper",
            "label": "EM Upper",
            "value_label": format_value(upper_em),
            "x": round(to_x(upper_em), 1),
            "line_top_y": 124.0,
            "line_bottom_y": 176.0,
            "show": show_upside_reference and upper_em is not None,
            "css_class": "apollo-risk-reference-em",
        },
        {
            "key": "call-barrier",
            "label": "Call Barrier",
            "value_label": format_value(call_barrier),
            "x": round(to_x(call_barrier), 1),
            "line_top_y": 132.0,
            "line_bottom_y": 176.0,
            "show": show_upside_reference and call_barrier is not None,
            "css_class": "apollo-risk-reference-safe",
        },
    ]

    return {
        "available": True,
        "guides": guides,
        "baseline_y": baseline_y,
        "plot_left_x": plot_left_x,
        "plot_right_x": plot_right_x,
        "peak_y": peak_y,
        "spot_line_top_y": spot_line_top_y,
        "profile_line_path": profile_line_path,
        "profile_area_path": profile_area_path,
        "spot_x": spot_x,
        "put_barrier_x": round(to_x(put_barrier), 1),
        "em_lower_x": round(to_x(lower_em), 1),
        "em_upper_x": round(to_x(upper_em), 1),
        "call_barrier_x": round(to_x(call_barrier), 1),
        "spot_label": format_value(spot),
        "put_barrier_label": format_value(put_barrier),
        "em_lower_label": format_value(lower_em),
        "em_upper_label": format_value(upper_em),
        "call_barrier_label": format_value(call_barrier),
        "positioning_note": (
            "Put-side only positioning shifts SPX right to reveal more downside spread spacing."
            if has_put_side_positions and not has_call_side_positions
            else "Balanced positioning keeps SPX centered when both put and call sides matter."
        ),
        "spot_shifted_right": has_put_side_positions and not has_call_side_positions,
        "show_upside_reference": show_upside_reference,
        "barrier_markers": [item for item in barrier_markers if item["show"]],
        "markers": markers,
        "regions": spread_regions,
    }


def compute_short_barrier_value(option_chain: Dict[str, Any], trade_candidates: Dict[str, Any], side: str) -> float | None:
    diagnostics = trade_candidates.get("diagnostics") or {}
    spot = _coerce_float(trade_candidates.get("underlying_price") or option_chain.get("underlying_price"))
    if spot is None:
        return None

    active_distance_points = _coerce_float(diagnostics.get("active_barrier_points"))
    if active_distance_points is None:
        return None

    contracts = option_chain.get("puts" if side == "put" else "calls") or []
    strikes = sorted({_coerce_float(item.get("strike")) for item in contracts if _coerce_float(item.get("strike")) is not None})
    if side == "put":
        target = spot - active_distance_points
        return max((strike for strike in strikes if strike <= target), default=target)

    target = spot + active_distance_points
    return min((strike for strike in strikes if strike >= target), default=target)


def format_short_barrier(option_chain: Dict[str, Any], trade_candidates: Dict[str, Any], side: str) -> str:
    barrier = compute_short_barrier_value(option_chain=option_chain, trade_candidates=trade_candidates, side=side)
    if barrier is None:
        return "—"
    if side == "put":
        formatted = format_value(barrier)
        return f"<= {formatted}"
    formatted = format_value(barrier)
    return f">= {formatted}"


def _coerce_float(value: Any) -> float | None:
    if value in (None, "", "—"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_long_date(value: Any) -> str:
    """Format a date-like value as a long weekday/month/day/year label."""
    if isinstance(value, datetime):
        return value.strftime("%A, %B %d, %Y")
    if isinstance(value, date):
        return value.strftime("%A, %B %d, %Y")
    return "—"


def parse_query_request(form_data: Any) -> QueryRequest:
    """Parse and validate a form submission."""
    query_type = (form_data.get("query_type") or "").strip()
    definition = QUERY_LOOKUP.get(query_type)
    if definition is None:
        raise ValidationError("Please select a valid query option.")

    single_date = parse_date_input(form_data.get("single_date"), field_label="single date")
    start_date = parse_date_input(form_data.get("start_date"), field_label="start date")
    end_date = parse_date_input(form_data.get("end_date"), field_label="end date")

    if definition.date_mode == "single" and single_date is None:
        raise ValidationError("Please provide a valid single date in YYYY-MM-DD format.")

    if definition.date_mode == "range":
        if start_date is None or end_date is None:
            raise ValidationError("Please provide both a valid start date and end date in YYYY-MM-DD format.")
        if end_date < start_date:
            raise ValidationError("End date cannot be earlier than start date.")

    return QueryRequest(
        definition=definition,
        single_date=single_date,
        start_date=start_date,
        end_date=end_date,
    )


def parse_date_input(value: Optional[str], field_label: str) -> Optional[date]:
    """Parse a date field from the HTML form."""
    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValidationError(f"Invalid {field_label}. Use YYYY-MM-DD.") from exc


def execute_query(query: QueryRequest, service: MarketDataService) -> Dict[str, Any]:
    """Dispatch a validated query to the service layer and format the result for the UI."""
    definition = query.definition

    if definition.action == "latest":
        snapshot = service.get_latest_snapshot(definition.ticker, query_type=definition.key)
        result_meta = service.get_result_metadata(snapshot)
        export_frame = pd.DataFrame(
            [
                {
                    "Symbol": definition.symbol,
                    "Ticker": definition.ticker,
                    "Market Date": snapshot["Market Date"].isoformat(),
                    "Latest Value": snapshot["Latest Value"],
                    "Open": snapshot["Open"],
                    "High": snapshot["High"],
                    "Low": snapshot["Low"],
                    "Close": snapshot["Close"],
                    "Prior Close": snapshot["Prior Close"],
                    "Daily Point Change": snapshot["Daily Point Change"],
                    "Daily Percent Change": snapshot["Daily Percent Change"],
                    "As Of (America/Chicago)": snapshot["As Of"],
                }
            ]
        )
        summary_items = [
            ("Symbol", definition.symbol),
            ("Ticker", definition.ticker),
            ("Market Date", snapshot["Market Date"]),
            ("Latest Value", snapshot["Latest Value"]),
            ("Prior Close", snapshot["Prior Close"]),
            ("Daily Point Change", snapshot["Daily Point Change"]),
            ("Daily Percent Change", append_percent(snapshot["Daily Percent Change"])),
            ("As Of", snapshot["As Of"]),
        ]
        table_frame = export_frame.copy()
        return build_result_payload(
            title=definition.label,
            summary_items=summary_items,
            table_frame=table_frame,
            export_frame=export_frame,
            result_meta=result_meta,
            result_kind="latest",
        )

    if definition.action == "range":
        history_frame = service.get_history_with_changes(
            definition.ticker,
            query.start_date,
            query.end_date,
            query_type=definition.key,
        )
        result_meta = service.get_result_metadata(history_frame)
        export_frame = history_frame.copy()
        export_frame["Date"] = export_frame["Date"].apply(lambda value: value.isoformat())
        summary_items = [
            ("Symbol", definition.symbol),
            ("Ticker", definition.ticker),
            ("Start Date", query.start_date),
            ("End Date", query.end_date),
            ("Rows Returned", len(export_frame)),
        ]
        return build_result_payload(
            title=definition.label,
            summary_items=summary_items,
            table_frame=export_frame,
            export_frame=export_frame,
            result_meta=result_meta,
            result_kind="historical",
        )

    if definition.action == "single_change":
        detail = service.get_single_day_change(definition.ticker, query.single_date, query_type=definition.key)
        result_meta = service.get_result_metadata(detail)
        current_row = detail["current"]
        prior_row = detail["prior"]
        export_frame = pd.DataFrame(
            [
                {
                    "Date": current_row["Date"].isoformat(),
                    "Open": current_row["Open"],
                    "High": current_row["High"],
                    "Low": current_row["Low"],
                    "Close": current_row["Close"],
                    "Prior Trading Date": prior_row["Date"].isoformat(),
                    "Prior Close": prior_row["Close"],
                    "Daily Point Change": current_row["Daily Point Change"],
                    "Daily Percent Change": current_row["Daily Percent Change"],
                }
            ]
        )
        comparison_frame = pd.DataFrame(
            [
                {
                    "Row": "Prior Trading Day",
                    "Date": prior_row["Date"].isoformat(),
                    "Open": prior_row["Open"],
                    "High": prior_row["High"],
                    "Low": prior_row["Low"],
                    "Close": prior_row["Close"],
                },
                {
                    "Row": "Requested Date",
                    "Date": current_row["Date"].isoformat(),
                    "Open": current_row["Open"],
                    "High": current_row["High"],
                    "Low": current_row["Low"],
                    "Close": current_row["Close"],
                },
            ]
        )
        summary_items = [
            ("Symbol", definition.symbol),
            ("Ticker", definition.ticker),
            ("Requested Date", current_row["Date"]),
            ("Prior Trading Date", prior_row["Date"]),
            ("Close", current_row["Close"]),
            ("Prior Close", prior_row["Close"]),
            ("Daily Point Change", current_row["Daily Point Change"]),
            ("Daily Percent Change", append_percent(current_row["Daily Percent Change"])),
        ]
        return build_result_payload(
            title=definition.label,
            summary_items=summary_items,
            table_frame=comparison_frame,
            export_frame=export_frame,
            result_meta=result_meta,
            result_kind="historical",
        )

    if definition.action == "single_close":
        row = service.get_single_day_bar(definition.ticker, query.single_date, query_type=definition.key)
        result_meta = service.get_result_metadata(row)
        export_frame = pd.DataFrame(
            [
                {
                    "Date": row["Date"].isoformat(),
                    "Open": row["Open"],
                    "High": row["High"],
                    "Low": row["Low"],
                    "Close": row["Close"],
                }
            ]
        )
        summary_items = [
            ("Symbol", definition.symbol),
            ("Ticker", definition.ticker),
            ("Requested Date", row["Date"]),
            ("Close", row["Close"]),
        ]
        return build_result_payload(
            title=definition.label,
            summary_items=summary_items,
            table_frame=export_frame,
            export_frame=export_frame,
            result_meta=result_meta,
            result_kind="historical",
        )

    raise ValidationError("Unsupported query action.")


def build_result_payload(
    title: str,
    summary_items: list[tuple[str, Any]],
    table_frame: pd.DataFrame,
    export_frame: pd.DataFrame,
    result_meta: Optional[Dict[str, Any]] = None,
    result_kind: str = "latest",
) -> Dict[str, Any]:
    """Convert DataFrames and summary fields into template-ready structures."""
    table_rows = [
        {column: format_value(row[column]) for column in table_frame.columns}
        for _, row in table_frame.iterrows()
    ]

    return {
        "title": title,
        "result_note": build_result_note(result_meta or {}, result_kind=result_kind),
        "result_note_level": "warning" if (result_meta or {}).get("used_stale_cache") else "info",
        "summary_items": [{"label": label, "value": format_value(value)} for label, value in summary_items],
        "table_columns": list(table_frame.columns),
        "table_rows": table_rows,
        "export_frame": export_frame,
    }


def build_result_note(result_meta: Dict[str, Any], result_kind: str = "latest") -> Optional[str]:
    """Build the user-facing retrieval message."""
    if not result_meta:
        return None

    provider_name = result_meta.get("provider_name")
    retrieved_at = result_meta.get("retrieved_at")
    prefix = f"Provider: {provider_name}" if provider_name else ""

    if retrieved_at is None:
        return prefix or None

    time_label = format_note_time(retrieved_at)
    source_type = result_meta.get("source_type") or result_meta.get("source")
    data_label = "Historical data" if result_kind == "historical" else "Live data"
    if source_type in {"cache", "fallback"}:
        age_seconds = result_meta.get("cache_age_seconds")
        age_label = f" ({age_seconds} seconds old)" if age_seconds is not None else ""
        qualifier = "historical " if result_kind == "historical" else ""
        note = f"Using cached {qualifier}data from {time_label}{age_label}"
    else:
        note = f"{data_label} retrieved at {time_label}"

    if prefix:
        return f"{prefix} — {note}"
    return note


def format_note_time(value: Any) -> str:
    """Format a short Chicago-local time label for result notices."""
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        timestamp = value.astimezone(CHICAGO_TZ) if value.tzinfo else value.replace(tzinfo=CHICAGO_TZ)
        return timestamp.strftime("%I:%M %p %Z").lstrip("0")
    return str(value)


def append_percent(value: Any) -> str:
    """Format a percentage summary value."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return f"{float(value):,.2f}%"


def format_value(value: Any) -> str:
    """Format a value for display in the HTML table or summary cards."""
    if value is None:
        return "—"
    if isinstance(value, float) and pd.isna(value):
        return "—"
    if isinstance(value, pd.Timestamp):
        timestamp = value.to_pydatetime()
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(CHICAGO_TZ)
        return timestamp.strftime("%Y-%m-%d %I:%M:%S %p %Z")
    if isinstance(value, datetime):
        timestamp = value.astimezone(CHICAGO_TZ) if value.tzinfo else value
        return timestamp.strftime("%Y-%m-%d %I:%M:%S %p %Z") if value.tzinfo else timestamp.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:,.2f}"
    return str(value)


def open_browser() -> None:
    """Open the local app in the default browser."""
    webbrowser.open_new(get_launch_url())


def get_launch_url() -> str:
    """Return the local URL that should open in the browser."""
    if should_use_https():
        parsed = urlparse(APP_CONFIG.schwab_redirect_uri)
        host = parsed.hostname or APP_HOST
        port = parsed.port or APP_PORT
        return f"https://{host}:{port}"
    return f"http://localhost:{APP_PORT}"


def should_use_https() -> bool:
    """Return whether the local app should start with an HTTPS dev server."""
    return APP_CONFIG.market_data_provider == "schwab" and APP_CONFIG.schwab_redirect_uri.startswith("https://")


app = create_app()


if __name__ == "__main__":
    Timer(1, open_browser).start()
    app.run(
        host=APP_HOST,
        port=APP_PORT,
        debug=False,
        use_reloader=False,
        ssl_context=("localhost+2.pem", "localhost+2-key.pem") if should_use_https() else None,
    )
