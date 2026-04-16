import tempfile
import unittest
from pathlib import Path

from app import create_app
from services.runtime.supabase_integration import SupabaseRequestError
from services.runtime.private_access import RequestIdentity


class _FixedIdentityResolver:
    def __init__(self, identity: RequestIdentity):
        self.identity = identity

    def resolve_request_identity(self, request):
        return self.identity


class _FakePerformanceService:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def build_dashboard(self, filters=None):
        self.calls.append(filters)
        return self.payload


class _FakeTradeStore:
    def __init__(self, trades_by_mode, summary_by_mode):
        self.trades_by_mode = trades_by_mode
        self.summary_by_mode = summary_by_mode

    def list_trades(self, trade_mode):
        return list(self.trades_by_mode.get(trade_mode, []))

    def summarize(self, trade_mode):
        return dict(self.summary_by_mode.get(trade_mode, {}))


class _FakeOpenTradeManager:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def evaluate_open_trades(self, *, send_alerts=False):
        self.calls.append(send_alerts)
        return self.payload


class _MissingTablePerformanceService:
    def build_dashboard(self, filters=None):
        raise SupabaseRequestError(
            'Supabase HTTP error 404: {"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table \'public.journal_trades\' in the schema cache"}'
        )


class _MissingTableTradeStore:
    def summarize(self, trade_mode):
        raise SupabaseRequestError(
            'Supabase HTTP error 404: {"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table \'public.journal_trades\' in the schema cache"}'
        )

    def list_trades(self, trade_mode):
        raise AssertionError("list_trades should not be called after summarize fails")


class _MissingTableOpenTradeManager:
    def evaluate_open_trades(self, *, send_alerts=False):
        raise SupabaseRequestError(
            'Supabase HTTP error 404: {"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table \'public.journal_trades\' in the schema cache"}'
        )


class _FakeApolloSnapshotRepository:
    def __init__(self, payload=None):
        self.payload = payload
        self.saved_payloads = []

    def save_snapshot(self, payload):
        self.saved_payloads.append(payload)
        self.payload = payload

    def load_snapshot(self):
        return self.payload


class _FakeKairosService:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get_dashboard_payload(self):
        return self.payload

    def initialize_live_kairos_on_page_load(self, *, force_refresh=False):
        self.calls.append(("initialize", force_refresh))
        return self.payload

    def run_scan_cycle(self, trigger_reason="scheduled", *, force_refresh=False):
        self.calls.append((trigger_reason, force_refresh))
        return self.payload


class _FakeKairosSnapshotRepository:
    def __init__(self, payload=None):
        self.payload = payload
        self.saved_payloads = []

    def save_snapshot(self, payload):
        self.saved_payloads.append(payload)
        self.payload = payload

    def load_snapshot(self):
        return self.payload


class _FakeApolloService:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []
        self.market_data_service = _FakeMarketDataService()

    def run_precheck(self, *, force_refresh=False):
        self.calls.append(force_refresh)
        return self.payload


class _FakeMarketDataService:
    def __init__(self, provider_name="Schwab", authenticated=True):
        self.provider_name = provider_name
        self.authenticated = authenticated
        self.fresh_calls = []

    def get_provider_metadata(self):
        return {
            "live_provider_name": self.provider_name,
            "requires_auth": True,
            "authenticated": self.authenticated,
        }

    def get_fresh_latest_snapshot(self, ticker, query_type="latest"):
        self.fresh_calls.append((ticker, query_type))
        return {"Latest Value": 6000.0}


class HostedActionsTest(unittest.TestCase):
    def _create_hosted_app(self, temp_dir: str):
        return create_app(
            {
                "TESTING": True,
                "RUNTIME_TARGET": "hosted",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                "SUPABASE_SECRET_KEY": "secret-key",
                "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                "TRADE_DATABASE": str(Path(temp_dir) / "hosted-actions.db"),
            }
        )

    def _allow_identity(self, app, *, email="bill@example.com"):
        app.extensions["request_identity_resolver"] = _FixedIdentityResolver(
            RequestIdentity(
                user_id="user-1",
                email=email,
                display_name="Bill",
                authenticated=True,
                auth_source="supabase-hosted",
            )
        )

    def test_local_runtime_keeps_hosted_action_routes_inactive(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app({"TESTING": True, "RUNTIME_TARGET": "local", "TRADE_DATABASE": str(Path(temp_dir) / "local-actions.db")})
            client = app.test_client()

            self.assertEqual(client.get("/hosted/actions/performance-summary").status_code, 404)
            self.assertEqual(client.get("/hosted/actions/journal-trades").status_code, 404)
            self.assertEqual(client.get("/hosted/actions/open-trades").status_code, 404)
            self.assertEqual(client.get("/hosted/actions/apollo").status_code, 404)
            self.assertEqual(client.get("/hosted/actions/kairos").status_code, 404)
            self.assertEqual(client.post("/hosted/actions/apollo/run").status_code, 404)
            self.assertEqual(client.post("/hosted/actions/kairos/run").status_code, 404)

    def test_hosted_performance_action_requires_authenticated_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["request_identity_resolver"] = _FixedIdentityResolver(
                RequestIdentity(authenticated=False, auth_source="supabase-hosted")
            )

            response = app.test_client().get("/hosted/actions/performance-summary")

            self.assertEqual(response.status_code, 401)
            self.assertEqual(response.get_json()["error"], "authentication_required")

    def test_hosted_journal_action_enforces_private_access_allowlist(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app, email="other@example.com")

            response = app.test_client().get("/hosted/actions/journal-trades")

            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.get_json()["error"], "private_access_denied")

    def test_hosted_performance_action_returns_dashboard_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_service = _FakePerformanceService(
                {
                    "filters": {"trade_mode": ["real"], "system": ["apollo"]},
                    "metrics": {"totals": {"total_trades": 3}},
                    "records_total": 3,
                    "records_filtered": 2,
                }
            )
            app.extensions["performance_service"] = fake_service

            response = app.test_client().get("/hosted/actions/performance-summary?trade_mode=real&system=Apollo")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "performance-summary")
            self.assertEqual(payload["payload"]["metrics"]["totals"]["total_trades"], 3)
            self.assertEqual(fake_service.calls, [{"system": ["Apollo"], "profile": [], "result": [], "trade_mode": ["real"], "macro_grade": [], "structure_grade": []}])

    def test_hosted_journal_action_returns_structured_trade_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = _FakeTradeStore(
                {
                    "simulated": [
                        {
                            "id": 42,
                            "trade_number": 7,
                            "status": "open",
                            "derived_status_raw": "open",
                            "candidate_profile": "Standard",
                            "system_name": "Apollo",
                            "trade_date": "2026-04-12",
                            "expiration_date": "2026-04-15",
                            "underlying_symbol": "SPX",
                            "short_strike": 6400,
                            "long_strike": 6395,
                            "contracts": 2,
                            "actual_entry_credit": 1.35,
                            "journal_name": "Sim Journal",
                            "trade_mode": "simulated",
                        }
                    ]
                },
                {
                    "simulated": {
                        "total_trades": 1,
                        "open_trades": 1,
                        "closed_trades": 0,
                        "total_pnl": 0,
                        "average_pnl": 0,
                        "win_count": 0,
                        "loss_count": 0,
                    }
                },
            )

            response = app.test_client().get("/hosted/actions/journal-trades?trade_mode=simulated")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "journal-trades")
            self.assertEqual(payload["trade_mode"], "simulated")
            self.assertEqual(payload["summary"]["total_trades"], 1)
            self.assertEqual(payload["trade_count"], 1)
            self.assertEqual(payload["trades"][0]["id"], 42)
            self.assertEqual(payload["trades"][0]["trade_number_raw"], 7)
            self.assertEqual(payload["trades"][0]["trade_mode"], "simulated")

    def test_hosted_open_trades_action_filters_management_records(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_manager = _FakeOpenTradeManager(
                {
                    "evaluated_at": "2026-04-12T15:30:00-05:00",
                    "evaluated_at_display": "2026-04-12 03:30 PM CDT",
                    "alerts_sent": 0,
                    "alert_failures": [],
                    "notifications_enabled": True,
                    "records": [
                        {"trade_id": 1, "trade_mode": "Real", "status": "Exit Now", "status_severity": 4},
                        {"trade_id": 2, "trade_mode": "Simulated", "status": "Watch", "status_severity": 1},
                    ],
                }
            )
            app.extensions["open_trade_manager"] = fake_manager

            response = app.test_client().get("/hosted/actions/open-trades?trade_mode=real")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "open-trades")
            self.assertEqual(payload["trade_mode"], "real")
            self.assertEqual(payload["records_total"], 2)
            self.assertEqual(payload["records_filtered"], 1)
            self.assertEqual(payload["open_trade_count"], 1)
            self.assertEqual(payload["records"][0]["trade_id"], 1)
            self.assertEqual(payload["status_counts"][0]["status"], "Exit Now")
            self.assertEqual(fake_manager.calls, [False])

    def test_hosted_performance_action_returns_schema_error_when_supabase_tables_are_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["performance_service"] = _MissingTablePerformanceService()

            response = app.test_client().get("/hosted/actions/performance-summary")

            self.assertEqual(response.status_code, 503)
            payload = response.get_json()
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"], "supabase_schema_missing")
            self.assertEqual(payload["missing_table"], "journal_trades")

    def test_hosted_journal_action_returns_schema_error_when_supabase_tables_are_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = _MissingTableTradeStore()

            response = app.test_client().get("/hosted/actions/journal-trades?trade_mode=real")

            self.assertEqual(response.status_code, 503)
            payload = response.get_json()
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"], "supabase_schema_missing")
            self.assertEqual(payload["surface"], "journal")

    def test_hosted_open_trades_action_returns_schema_error_when_supabase_tables_are_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["open_trade_manager"] = _MissingTableOpenTradeManager()

            response = app.test_client().get("/hosted/actions/open-trades?trade_mode=real")

            self.assertEqual(response.status_code, 503)
            payload = response.get_json()
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"], "supabase_schema_missing")
            self.assertEqual(payload["surface"], "manage-trades")

    def test_hosted_apollo_action_returns_last_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["apollo_snapshot_repository"] = _FakeApolloSnapshotRepository(
                {
                    "status": "Allowed",
                    "run_timestamp": "Sat 2026-04-12 08:45 AM CDT",
                    "structure_grade": "Bullish",
                    "macro_grade": "Minor",
                    "trade_candidates_valid_count": 2,
                    "trade_candidates_items": [{"mode_label": "Standard", "available": True, "short_strike": "6400", "long_strike": "6395", "net_credit": "$1.40", "max_loss": "$360.00", "em_multiple": "1.62x"}],
                    "reasons": ["Apollo generated two valid candidates."],
                }
            )

            response = app.test_client().get("/hosted/actions/apollo")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "apollo-summary")
            self.assertTrue(payload["snapshot_available"])
            self.assertEqual(payload["payload"]["structure_grade"], "Bullish")
            self.assertEqual(payload["payload"]["trade_candidates_valid_count"], 2)

    def test_hosted_kairos_action_returns_read_only_summary_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["kairos_live_service"] = _FakeKairosService(
                {
                    "title": "Kairos",
                    "session_status": "Scanning",
                    "session_status_key": "scanning",
                    "current_state_display": "Window Open",
                    "market_session_status": "Open",
                    "last_scan_display": "2026-04-12 10:15 AM CDT",
                    "next_scan_display": "2026-04-12 10:20 AM CDT",
                    "total_scans_completed": 9,
                    "window_found": True,
                    "classification_note": "Momentum is aligned.",
                    "scan_log_count": 9,
                    "latest_scan": {
                        "summary_text": "Kairos window is active.",
                        "structure_status": "Aligned",
                        "momentum_status": "Improving",
                        "timing_status": "Eligible",
                        "kairos_state": "Window Open",
                        "spx_value": "6128.40",
                        "vix_value": "18.20",
                    },
                    "live_workspace": {
                        "summary_text": "Kairos window is active.",
                        "classification_note": "Momentum is aligned.",
                        "stamps": [{"label": "Structure", "value": "Aligned"}],
                        "candidate_cards": [{"slot_label": "Subprime", "tradeable": True, "available": True, "strike_label": "6115 / 6110 Put Spread", "net_credit": "$1.55", "distance_to_short": "13.4 pts", "em_multiple": "1.58x", "message": "Qualified on live chain."}],
                    },
                    "lifecycle_items": [{"label": "Window Found", "value": "Reached"}],
                }
            )

            response = app.test_client().get("/hosted/actions/kairos")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "kairos-summary")
            self.assertEqual(payload["payload"]["current_state_display"], "Window Open")
            self.assertEqual(payload["payload"]["candidate_cards"][0]["slot_label"], "Best Available")

    def test_hosted_apollo_run_action_executes_live_engine_and_persists_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_apollo_service = _FakeApolloService(
                {
                    "title": "Apollo Gate 1 -- SPX Structure",
                    "apollo_status": "allowed",
                    "provider_name": "Schwab",
                    "local_datetime": "2026-04-12T09:15:00-05:00",
                    "spx": {"value": 6125.2, "as_of": "2026-04-12 09:15:00 AM CDT"},
                    "vix": {"value": 18.3, "as_of": "2026-04-12 09:15:00 AM CDT"},
                    "macro": {"grade": "Minor", "source_name": "Macro Feed", "macro_events": []},
                    "structure": {"grade": "Bullish", "available": True, "metrics": {}},
                    "market_calendar": {"next_market_day": "2026-04-13"},
                    "option_chain": {"success": True, "request_diagnostics": {}, "expiration_date": "2026-04-13"},
                    "trade_candidates": {"candidate_count": 1, "valid_mode_count": 1, "candidates": []},
                    "reasons": ["Live SPX data retrieved successfully."],
                }
            )
            fake_snapshot_repository = _FakeApolloSnapshotRepository()
            app.extensions["apollo_service"] = fake_apollo_service
            app.extensions["apollo_snapshot_repository"] = fake_snapshot_repository

            response = app.test_client().post("/hosted/actions/apollo/run")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "apollo-run")
            self.assertTrue(payload["live_execution"])
            self.assertEqual(fake_apollo_service.calls, [True])
            self.assertEqual(fake_snapshot_repository.payload["execution_source_label"], "Hosted Live Execution")

    def test_hosted_kairos_run_action_executes_live_scan_and_persists_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_market_data_service = _FakeMarketDataService()
            fake_kairos_service = _FakeKairosService(
                {
                    "title": "Kairos",
                    "mode": "Live",
                    "session_status": "Scanning",
                    "session_status_key": "scanning",
                    "current_state_display": "Window Open",
                    "market_session_status": "Open",
                    "last_scan_display": "2026-04-12 10:15 AM CDT",
                    "next_scan_display": "2026-04-12 10:20 AM CDT",
                    "total_scans_completed": 9,
                    "window_found": True,
                    "classification_note": "Momentum is aligned.",
                    "latest_scan": {
                        "summary_text": "Kairos window is active.",
                        "structure_status": "Aligned",
                        "momentum_status": "Improving",
                        "timing_status": "Eligible",
                        "kairos_state": "Window Open",
                        "spx_value": "6128.40",
                        "vix_value": "18.20",
                    },
                    "live_workspace": {
                        "summary_text": "Kairos window is active.",
                        "classification_note": "Momentum is aligned.",
                        "stamps": [{"label": "Structure", "value": "Aligned"}],
                        "candidate_cards": [{"slot_label": "Subprime", "tradeable": True, "available": True, "strike_label": "6115 / 6110 Put Spread", "net_credit": "$1.55", "distance_to_short": "13.4 pts", "em_multiple": "1.58x", "message": "Qualified on live chain."}],
                    },
                    "lifecycle_items": [{"label": "Window Found", "value": "Reached"}],
                    "scan_log_count": 9,
                }
            )
            fake_snapshot_repository = _FakeKairosSnapshotRepository()
            app.extensions["market_data_service"] = fake_market_data_service
            app.extensions["kairos_live_service"] = fake_kairos_service
            app.extensions["kairos_service"] = fake_kairos_service
            app.extensions["kairos_snapshot_repository"] = fake_snapshot_repository

            response = app.test_client().post("/hosted/actions/kairos/run")

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["action"], "kairos-run")
            self.assertTrue(payload["live_execution"])
            self.assertEqual(len(fake_market_data_service.fresh_calls), 2)
            self.assertEqual(fake_kairos_service.calls[0][1], True)
            self.assertEqual(fake_snapshot_repository.payload["execution_source_label"], "Hosted live execution")