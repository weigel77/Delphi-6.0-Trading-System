import tempfile
import unittest
from pathlib import Path

from app import create_app
from services.performance_dashboard_service import PerformanceDashboardService
from services.repositories.trade_repository import SupabaseTradeRepository
from services.runtime.supabase_integration import SupabaseConfig, SupabaseRequestError, SupabaseRuntimeContext
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
        self.updated = []
        self.created = []

    def list_trades(self, trade_mode):
        return list(self.trades_by_mode.get(trade_mode, []))

    def summarize(self, trade_mode):
        return dict(self.summary_by_mode.get(trade_mode, {}))

    def next_trade_number(self):
        trade_numbers = [int(trade.get("trade_number") or 0) for trades in self.trades_by_mode.values() for trade in trades]
        return max(trade_numbers, default=0) + 1

    def get_trade(self, trade_id):
        for trades in self.trades_by_mode.values():
            for trade in trades:
                if int(trade.get("id") or 0) == int(trade_id):
                    return dict(trade)
        return None

    def create_trade(self, payload):
        trade_id = max((int(trade.get("id") or 0) for trades in self.trades_by_mode.values() for trade in trades), default=0) + 1
        created_trade = dict(payload)
        created_trade.setdefault("id", trade_id)
        created_trade.setdefault("trade_number", self.next_trade_number())
        created_trade.setdefault("trade_mode", payload.get("trade_mode") or "real")
        self.trades_by_mode.setdefault(created_trade["trade_mode"], []).append(created_trade)
        self.created.append(created_trade)
        return trade_id

    def find_recent_duplicate(self, payload, window_seconds=15):
        return None

    def update_trade(self, trade_id, payload):
        for trades in self.trades_by_mode.values():
            for index, trade in enumerate(trades):
                if int(trade.get("id") or 0) != int(trade_id):
                    continue
                updated_trade = dict(trade)
                updated_trade.update(payload)
                if "close_events" in payload:
                    updated_trade["close_events"] = payload["close_events"]
                trades[index] = updated_trade
                self.updated.append({"trade_id": trade_id, "payload": dict(payload)})
                return
        raise ValueError("Trade not found.")


class _FakeTradeStoreCreateReadbackFailure(_FakeTradeStore):
    def get_trade(self, trade_id):
        raise SupabaseRequestError("Supabase network error: temporary readback failure")


class _FakeOpenTradeManager:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def evaluate_open_trades(self, *, send_alerts=False):
        self.calls.append(send_alerts)
        return self.payload


class _FakeTradeNotificationRepository:
    def __init__(self):
        self.saved = []

    def save_trade_notifications(self, trade_id, notifications):
        self.saved.append({"trade_id": trade_id, "notifications": notifications})


class _FakeApolloSnapshotRepository:
    def __init__(self, payload=None):
        self.payload = payload

    def save_snapshot(self, payload):
        self.payload = payload

    def load_snapshot(self):
        return self.payload


class _FakeKairosService:
    def __init__(self, payload):
        self.payload = payload

    def get_dashboard_payload(self):
        return self.payload


class _MissingTableSupabaseGateway:
    def __init__(self, *missing_tables):
        self.missing_tables = set(missing_tables)

    def select(self, table, *, filters=None, order=None, limit=None, columns="*"):
        if table in self.missing_tables:
            raise SupabaseRequestError(
                f'Supabase HTTP error 404: {{"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table \'public.{table}\' in the schema cache"}}'
            )
        return []

    def insert(self, table, payload):
        raise AssertionError("insert should not be called in hosted shell read tests")

    def update(self, table, payload, *, filters):
        raise AssertionError("update should not be called in hosted shell read tests")

    def delete(self, table, *, filters):
        raise AssertionError("delete should not be called in hosted shell read tests")


class _RepoBackedOpenTradeManager:
    def __init__(self, store):
        self.store = store
        self.calls = []

    def evaluate_open_trades(self, *, send_alerts=False):
        self.calls.append(send_alerts)
        records = []
        for trade_mode in ("real", "simulated"):
            records.extend(
                trade
                for trade in self.store.list_trades(trade_mode)
                if str(trade.get("derived_status_raw") or trade.get("status") or "").strip().lower() in {"open", "reduced"}
            )
        return {
            "evaluated_at": "",
            "evaluated_at_display": "Not yet evaluated",
            "open_trade_count": len(records),
            "alerts_sent": 0,
            "alert_failures": [],
            "notifications_enabled": True,
            "status_counts": [],
            "records": records,
        }


class HostedShellTest(unittest.TestCase):
    def _create_hosted_app(self, temp_dir: str):
        return create_app(
            {
                "TESTING": True,
                "RUNTIME_TARGET": "hosted",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                "SUPABASE_SECRET_KEY": "secret-key",
                "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                "TRADE_DATABASE": str(Path(temp_dir) / "hosted-shell.db"),
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

    def _build_missing_table_repository(self, temp_dir: str, *missing_tables: str) -> SupabaseTradeRepository:
        return SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=_MissingTableSupabaseGateway(*missing_tables),
            database_path=str(Path(temp_dir) / "hosted-shell.db"),
        )

    def test_local_runtime_keeps_hosted_shell_routes_inactive_and_local_pages_work(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app({"TESTING": True, "RUNTIME_TARGET": "local", "TRADE_DATABASE": str(Path(temp_dir) / "local-shell.db")})
            client = app.test_client()

            self.assertEqual(client.get("/hosted").status_code, 404)
            self.assertEqual(client.get("/hosted/login").status_code, 404)
            self.assertEqual(client.get("/hosted/performance").status_code, 404)
            self.assertEqual(client.get("/hosted/journal").status_code, 404)
            self.assertEqual(client.get("/hosted/open-trades").status_code, 404)
            self.assertEqual(client.get("/hosted/manage-trades").status_code, 404)
            self.assertEqual(client.get("/hosted/mobile").status_code, 404)
            self.assertEqual(client.get("/hosted/apollo").status_code, 404)
            self.assertEqual(client.get("/hosted/kairos").status_code, 404)
            self.assertEqual(client.get("/performance").status_code, 200)

    def test_hosted_shell_page_requires_authentication(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            app.extensions["request_identity_resolver"] = _FixedIdentityResolver(
                RequestIdentity(authenticated=False, auth_source="supabase-hosted")
            )

            response = app.test_client().get("/hosted/performance")

            self.assertEqual(response.status_code, 302)
            self.assertIn("/hosted/login?next=/hosted/performance", response.headers["Location"])

    def test_hosted_shell_home_renders_for_allowed_bill_identity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)

            response = app.test_client().get("/hosted")

            self.assertEqual(response.status_code, 200)
        self.assertIn(b"Delphi 6.3", response.data)
        self.assertIn(b"/hosted/research", response.data)
        self.assertIn(b"/hosted/performance", response.data)
        self.assertIn(b"/hosted/journal?trade_mode=real", response.data)
        self.assertIn(b"/hosted/manage-trades", response.data)
        self.assertNotIn(b"Text Status", response.data)

    def test_hosted_runtime_redirects_canonical_local_pages_to_hosted_shell_routes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            client = app.test_client()

            self.assertEqual(client.get("/", follow_redirects=False).headers["Location"], "/hosted/launch")
            self.assertEqual(client.get("/research", follow_redirects=False).headers["Location"], "/hosted/research")
            self.assertEqual(client.get("/performance?system=Apollo", follow_redirects=False).headers["Location"], "/hosted/performance?system=Apollo")
            self.assertEqual(client.get("/trades/real", follow_redirects=False).headers["Location"], "/hosted/journal?trade_mode=real")
            self.assertEqual(client.get("/management/open-trades", follow_redirects=False).headers["Location"], "/hosted/manage-trades")

    def test_hosted_mobile_shell_renders_phone_navigation_and_quick_actions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["performance_service"] = _FakePerformanceService(
                {
                    "filters": {"system": [], "profile": [], "result": [], "trade_mode": ["real"], "macro_grade": [], "structure_grade": [], "timeframe": ["all"]},
                    "records_total": 3,
                    "records_filtered": 2,
                    "metrics": {
                        "totals": {"total_trades": 2},
                        "win_rate": {"value": 50.0},
                        "expectancy": {"value": 88.5},
                        "net_pnl": {"value": 140.0},
                    },
                }
            )
            app.extensions["open_trade_manager"] = _FakeOpenTradeManager(
                {
                    "evaluated_at": "2026-04-16T11:45:00-05:00",
                    "evaluated_at_display": "2026-04-16 11:45 AM CDT",
                    "alerts_sent": 0,
                    "alert_failures": [],
                    "notifications_enabled": True,
                    "records": [
                        {"trade_id": 11, "trade_number": 301, "trade_mode": "real", "status": "Watch", "system_name": "Apollo", "candidate_profile": "Standard", "strike_pair": "6400 / 6395", "expiration_date": "2026-04-18", "gross_pnl": "$120.00", "action_recommendation": "Review"}
                    ],
                }
            )
            app.extensions["trade_store"] = _FakeTradeStore(
                {
                    "real": [{"id": 11, "trade_number": 301, "status": "open", "derived_status_raw": "open", "candidate_profile": "Standard", "system_name": "Apollo", "trade_date": "2026-04-16", "expiration_date": "2026-04-18", "short_strike": 6400, "long_strike": 6395, "contracts": 1, "actual_entry_credit": 1.4, "gross_pnl": 120.0, "trade_mode": "real"}],
                    "simulated": [],
                },
                {
                    "real": {"total_trades": 1, "open_trades": 1, "closed_trades": 0, "total_pnl": 120.0, "average_pnl": 120.0, "win_count": 0, "loss_count": 0},
                    "simulated": {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "total_pnl": 0.0, "average_pnl": 0.0, "win_count": 0, "loss_count": 0},
                },
            )
            app.extensions["apollo_snapshot_repository"] = _FakeApolloSnapshotRepository(
                {
                    "status": "Allowed",
                    "run_timestamp": "Thu 2026-04-16 11:40 AM CDT",
                    "structure_grade": "Bullish",
                    "macro_grade": "Minor",
                    "trade_candidates_valid_count": 1,
                    "trade_candidates_items": [{"mode_label": "Standard", "available": True, "short_strike": "6400", "long_strike": "6395", "net_credit": "$1.40", "em_multiple": "1.62x", "prefill_fields": {"candidate_profile": "Standard"}}],
                }
            )
            app.extensions["kairos_live_service"] = _FakeKairosService(
                {
                    "title": "Kairos",
                    "mode": "Live",
                    "session_status": "Armed",
                    "current_state_display": "Window Open",
                    "market_session_status": "Open",
                    "last_scan_display": "Thu 2026-04-16 11:41 AM CDT",
                    "total_scans_completed": 4,
                    "latest_scan": {"structure_status": "Developing", "timing_status": "Eligible", "spx_value": "6,123.45", "vix_value": "18.76"},
                    "live_workspace": {"summary_text": "Kairos sees a tradable live window.", "candidate_cards": [{"slot_label": "Subprime", "available": True, "tradeable": True, "strike_label": "6115 / 6110 Put Spread", "net_credit": "$1.55", "prefill_fields": {"candidate_profile": "Subprime"}}], "stamps": []},
                }
            )
            app.extensions["kairos_snapshot_repository"] = _FakeApolloSnapshotRepository(None)

            client = app.test_client()

            home_response = client.get("/hosted/mobile")
            self.assertEqual(home_response.status_code, 200)
            self.assertIn(b"Delphi Mobile", home_response.data)
            self.assertIn(b"Run Apollo", home_response.data)
            self.assertIn(b"Run Kairos", home_response.data)
            self.assertIn(b"Open Trades", home_response.data)
            self.assertIn(b">Home<", home_response.data)
            self.assertIn(b">Runs<", home_response.data)
            self.assertIn(b">Trades<", home_response.data)
            self.assertIn(b">Journal<", home_response.data)
            self.assertIn(b">More<", home_response.data)

            journal_response = client.get("/hosted/mobile/journal")
            self.assertEqual(journal_response.status_code, 200)
            self.assertIn(b"Quick Add Form", journal_response.data)

            more_response = client.get("/hosted/mobile/more")
            self.assertEqual(more_response.status_code, 200)
            self.assertIn(b"Switch to Desktop", more_response.data)

    def test_hosted_performance_page_uses_delphi_template_and_hosted_data_url(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["performance_service"] = _FakePerformanceService(
                {
                    "filters": {"system": ["apollo"], "profile": [], "result": [], "trade_mode": ["real"], "macro_grade": [], "structure_grade": []},
                    "records_total": 8,
                    "records_filtered": 3,
                    "metrics": {
                        "totals": {"total_trades": 3, "open_trades": 1, "closed_trades": 2, "wins": 2, "loss_outcomes": 0, "black_swan_count": 0, "scratched_count": 0},
                        "win_rate": {"value": 100.0},
                        "expectancy": {"value": 145.5},
                        "net_pnl": {"value": 310.0},
                        "credit_efficiency": {"value": 41.2},
                    },
                    "learning": {"overview": {"avg_safety_ratio": 1.74}},
                }
            )

            response = app.test_client().get("/hosted/performance?system=Apollo")

            self.assertEqual(response.status_code, 200)
        self.assertIn(b'Performance | ', response.data)
        self.assertIn(b'/hosted/performance/data?system=Apollo', response.data)
        self.assertIn(b'Equity Curve', response.data)
        self.assertIn(b'145.5', response.data)
        self.assertIn(b'310', response.data)

    def test_hosted_journal_page_uses_trade_template_with_hosted_links(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = _FakeTradeStore(
                {
                    "simulated": [
                        {
                            "id": 7,
                            "trade_number": 22,
                            "status": "closed",
                            "derived_status_raw": "closed",
                            "candidate_profile": "Subprime",
                            "system_name": "Apollo",
                            "trade_date": "2026-04-11",
                            "expiration_date": "2026-04-14",
                            "underlying_symbol": "SPX",
                            "short_strike": 6420,
                            "long_strike": 6415,
                            "contracts": 1,
                            "actual_entry_credit": 1.45,
                            "gross_pnl": 95.0,
                            "journal_name": "Hosted Sim",
                            "trade_mode": "simulated",
                        }
                    ]
                },
                {
                    "simulated": {
                        "total_trades": 1,
                        "open_trades": 0,
                        "closed_trades": 1,
                        "total_pnl": 95.0,
                        "average_pnl": 95.0,
                        "win_count": 1,
                        "loss_count": 0,
                    }
                },
            )

            response = app.test_client().get("/hosted/journal?trade_mode=simulated")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Hosted Delphi 6.3 journal mirrors the live Supabase trade store and supports draft review, editing, and deleting directly in hosted mode.', response.data)
            self.assertIn(b'/hosted/journal?trade_mode=real', response.data)
            self.assertIn(b'/hosted/journal?trade_mode=simulated', response.data)
            self.assertIn(b'/hosted/journal/simulated/7/edit', response.data)
            self.assertIn(b'/hosted/journal/simulated/7/delete', response.data)
            self.assertIn(b'SubPrime', response.data)
            self.assertIn(b'22', response.data)
            self.assertIn(b'$95', response.data)
            self.assertNotIn(b'Max Loss', response.data)
            self.assertNotIn(b'Jump to Manual Entry Form', response.data)

    def test_hosted_apollo_prefill_redirects_into_hosted_journal_draft(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = _FakeTradeStore({"real": []}, {"real": {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "total_pnl": 0.0, "average_pnl": 0.0, "win_count": 0, "loss_count": 0}})
            client = app.test_client()

            response = client.post(
                "/hosted/apollo/prefill-candidate",
                data={
                    "target_mode": "real",
                    "candidate_profile": "Standard",
                    "system_name": "Apollo",
                    "system_version": "6.2",
                    "trade_date": "2026-04-12",
                    "entry_datetime": "2026-04-12T09:35",
                    "expiration_date": "2026-04-13",
                    "underlying_symbol": "SPX",
                    "spx_at_entry": "6129.20",
                    "vix_at_entry": "18.30",
                    "structure_grade": "Bullish",
                    "macro_grade": "Minor",
                    "expected_move": "38.0",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6400",
                    "long_strike": "6395",
                    "spread_width": "5",
                    "contracts": "1",
                    "candidate_credit_estimate": "1.40",
                    "actual_entry_credit": "1.40",
                    "distance_to_short": "20.8",
                    "pass_type": "standard_strict",
                    "premium_per_contract": "140",
                    "total_premium": "140",
                    "max_theoretical_risk": "360",
                    "risk_efficiency": "0.3889",
                    "target_em": "1.8",
                    "short_delta": "0.14",
                    "notes_entry": "Prefilled from Apollo candidate card.",
                },
                follow_redirects=False,
            )

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/journal?trade_mode=real&prefill=1#trade-entry-form")

            draft_response = client.get("/hosted/journal?trade_mode=real&prefill=1")

            self.assertEqual(draft_response.status_code, 200)
            self.assertIn(b'Jump to Draft Form', draft_response.data)
            self.assertIn(b'Apollo candidate data is loaded into this draft.', draft_response.data)
            self.assertIn(b'value="6.2"', draft_response.data)
            self.assertIn(b'value="standard_strict"', draft_response.data)
            self.assertIn(b'value="140"', draft_response.data)
            self.assertIn(b'value="360"', draft_response.data)

    def test_hosted_kairos_prefill_redirects_into_hosted_journal_draft(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = _FakeTradeStore({"real": []}, {"real": {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "total_pnl": 0.0, "average_pnl": 0.0, "win_count": 0, "loss_count": 0}})
            client = app.test_client()

            response = client.post(
                "/hosted/kairos/prefill-candidate",
                data={
                    "target_mode": "real",
                    "candidate_profile": "Subprime",
                    "journal_name": "Horme",
                    "system_version": "6.2",
                    "expiration_date": "2026-04-13",
                    "underlying_symbol": "SPX",
                    "spx_at_entry": "6129.20",
                    "vix_at_entry": "17.89",
                    "structure_grade": "Prime",
                    "macro_grade": "Improving",
                    "expected_move": "38.0",
                    "expected_move_used": "38.0",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6115",
                    "long_strike": "6110",
                    "spread_width": "5",
                    "contracts": "1",
                    "candidate_credit_estimate": "1.40",
                    "actual_entry_credit": "1.40",
                    "distance_to_short": "14.2",
                    "actual_distance_to_short": "14.2",
                    "actual_em_multiple": "1.55",
                    "pass_type": "kairos_candidate",
                    "premium_per_contract": "140",
                    "total_premium": "140",
                    "max_theoretical_risk": "360",
                    "risk_efficiency": "0.3889",
                    "target_em": "1.55",
                    "short_delta": "0.14",
                    "notes_entry": "Prefilled from Kairos best candidate card.",
                },
                follow_redirects=False,
            )

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/journal?trade_mode=real&prefill=1#trade-entry-form")

            draft_response = client.get("/hosted/journal?trade_mode=real&prefill=1")

            self.assertEqual(draft_response.status_code, 200)
            self.assertIn(b'Jump to Draft Form', draft_response.data)
            self.assertIn(b'Kairos candidate data is loaded into this draft.', draft_response.data)
            self.assertIn(b'value="6.2"', draft_response.data)
            self.assertIn(b'value="kairos_candidate"', draft_response.data)
            self.assertIn(b'value="140"', draft_response.data)
            self.assertIn(b'value="360"', draft_response.data)

    def test_hosted_journal_edit_page_posts_changes_through_hosted_store(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_store = _FakeTradeStore(
                {
                    "real": [
                        {
                            "id": 11,
                            "trade_number": 61,
                            "status": "open",
                            "derived_status_raw": "open",
                            "candidate_profile": "Prime",
                            "system_name": "Apollo",
                            "system_version": "5.1",
                            "trade_date": "2026-04-11",
                            "entry_datetime": "2026-04-11T09:35",
                            "expiration_date": "2026-04-14",
                            "underlying_symbol": "SPX",
                            "short_strike": 6420,
                            "long_strike": 6415,
                            "spread_width": 5,
                            "contracts": 1,
                            "actual_entry_credit": 1.45,
                            "candidate_credit_estimate": 1.45,
                            "distance_to_short": 18.4,
                            "gross_pnl": 0.0,
                            "journal_name": "Hosted Real",
                            "trade_mode": "real",
                            "close_events": [],
                            "remaining_contracts": 1,
                            "closed_contracts": 0,
                            "original_contracts": 1,
                            "realized_pnl": 0.0,
                            "total_max_loss": 355.0,
                        }
                    ]
                },
                {
                    "real": {
                        "total_trades": 1,
                        "open_trades": 1,
                        "closed_trades": 0,
                        "total_pnl": 0.0,
                        "average_pnl": 0.0,
                        "win_count": 0,
                        "loss_count": 0,
                    }
                },
            )
            app.extensions["trade_store"] = fake_store
            client = app.test_client()

            response = client.get("/hosted/journal/real/11/edit")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Hosted saves write directly to the Supabase journal tables.', response.data)
            self.assertIn(b'Edit Trade #61', response.data)

            post_response = client.post(
                "/hosted/journal/real/11/edit",
                data={
                    "trade_number": "61",
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "journal_name": "Hosted Real",
                    "system_version": "5.1",
                    "candidate_profile": "Prime",
                    "status": "closed",
                    "trade_date": "2026-04-11",
                    "entry_datetime": "2026-04-11T09:35",
                    "expiration_date": "2026-04-14",
                    "underlying_symbol": "SPX",
                    "spx_at_entry": "6100",
                    "vix_at_entry": "18.4",
                    "structure_grade": "A",
                    "macro_grade": "Minor",
                    "expected_move": "42",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6420",
                    "long_strike": "6415",
                    "spread_width": "5",
                    "contracts": "1",
                    "candidate_credit_estimate": "1.45",
                    "actual_entry_credit": "1.45",
                    "distance_to_short": "18.4",
                    "short_delta": "0.16",
                    "close_reason": "Target hit",
                    "notes_entry": "Hosted edit entry note",
                    "notes_exit": "Hosted edit exit note",
                    "close_events_present": "1",
                    "close_event_id": "",
                    "close_event_contracts_closed": "1",
                    "close_event_actual_exit_value": "0.30",
                    "close_event_method": "Close",
                    "close_event_event_datetime": "2026-04-11T14:00",
                    "close_event_notes_exit": "Closed from hosted journal",
                },
                follow_redirects=False,
            )

            self.assertEqual(post_response.status_code, 302)
            self.assertEqual(post_response.headers["Location"], "/hosted/journal?trade_mode=real")
            self.assertEqual(len(fake_store.updated), 1)
            self.assertEqual(fake_store.updated[0]["trade_id"], 11)
            self.assertEqual(fake_store.updated[0]["payload"]["status"], "closed")
            self.assertEqual(fake_store.updated[0]["payload"]["close_events"][0]["close_method"], "Close")

    def test_hosted_journal_manual_create_posts_through_hosted_store(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_store = _FakeTradeStore(
                {"real": []},
                {"real": {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "total_pnl": 0.0, "average_pnl": 0.0, "win_count": 0, "loss_count": 0}},
            )
            app.extensions["trade_store"] = fake_store
            client = app.test_client()

            response = client.post(
                "/hosted/journal?trade_mode=real",
                data={
                    "trade_number": "1",
                    "trade_mode": "real",
                    "system_name": "Kairos",
                    "journal_name": "Hosted Real",
                    "system_version": "6.2",
                    "candidate_profile": "Subprime",
                    "status": "open",
                    "trade_date": "2026-04-11",
                    "entry_datetime": "2026-04-11T09:35",
                    "expiration_date": "2026-04-11",
                    "underlying_symbol": "SPX",
                    "spx_at_entry": "6129.20",
                    "vix_at_entry": "17.89",
                    "structure_grade": "Bullish Confirmation",
                    "macro_grade": "Improving",
                    "expected_move": "38.0",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6115",
                    "long_strike": "6110",
                    "spread_width": "5",
                    "contracts": "1",
                    "candidate_credit_estimate": "1.40",
                    "actual_entry_credit": "1.40",
                    "distance_to_short": "14.2",
                    "short_delta": "0.14",
                    "notes_entry": "Manual hosted trade save.",
                },
                follow_redirects=False,
            )

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/journal?trade_mode=real")
            self.assertEqual(len(fake_store.created), 1)
            self.assertEqual(fake_store.created[0]["system_name"], "Kairos")

    def test_hosted_journal_create_does_not_require_immediate_readback_after_write(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            fake_store = _FakeTradeStoreCreateReadbackFailure(
                {"real": []},
                {"real": {"total_trades": 0, "open_trades": 0, "closed_trades": 0, "total_pnl": 0.0, "average_pnl": 0.0, "win_count": 0, "loss_count": 0}},
            )
            app.extensions["trade_store"] = fake_store
            client = app.test_client()

            response = client.post(
                "/hosted/journal?trade_mode=real",
                data={
                    "trade_number": "1",
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "journal_name": "Hosted Real",
                    "system_version": "6.2",
                    "candidate_profile": "Standard",
                    "status": "open",
                    "trade_date": "2026-04-11",
                    "entry_datetime": "2026-04-11T09:35",
                    "expiration_date": "2026-04-11",
                    "underlying_symbol": "SPX",
                    "spx_at_entry": "6129.20",
                    "vix_at_entry": "17.89",
                    "structure_grade": "Bullish",
                    "macro_grade": "Minor",
                    "expected_move": "38.0",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6115",
                    "long_strike": "6110",
                    "spread_width": "5",
                    "contracts": "1",
                    "candidate_credit_estimate": "1.40",
                    "actual_entry_credit": "1.40",
                    "distance_to_short": "14.2",
                    "short_delta": "0.14",
                    "notes_entry": "Hosted save without immediate readback.",
                },
                follow_redirects=False,
            )

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/journal?trade_mode=real")
            self.assertEqual(len(fake_store.created), 1)

    def test_hosted_open_trades_redirects_to_manage_trades(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            response = app.test_client().get("/hosted/open-trades?trade_mode=real")

            self.assertEqual(response.status_code, 302)
            self.assertIn("/hosted/manage-trades", response.headers["Location"])

    def test_hosted_manage_trades_page_restores_hosted_actions_and_trimmed_columns(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["open_trade_manager"] = _FakeOpenTradeManager(
                {
                    "evaluated_at": "2026-04-12T15:30:00-05:00",
                    "evaluated_at_display": "2026-04-12 03:30 PM CDT",
                    "alerts_sent": 0,
                    "alert_failures": [],
                    "notifications_enabled": True,
                    "live_expected_move_display": "10.20",
                    "status_counts": [{"label": "Watch", "count": 1, "key": "watch"}, {"label": "Exit Now", "count": 0, "key": "exit-now"}],
                    "records": [
                        {
                            "trade_id": 9,
                            "trade_number": 44,
                            "trade_mode": "Real",
                            "system_name": "Apollo",
                            "profile_label": "Subprime",
                            "contracts_display": "1",
                            "total_premium_display": "$180.00",
                            "status": "Watch",
                            "status_severity": 1,
                            "action_recommendation": "Hold",
                            "distance_to_short_display": "22.0 pts",
                            "current_total_close_cost_display": "$210.00",
                            "unrealized_pnl_display": "-$30.00",
                            "send_close_to_journal_enabled": True,
                            "status_key": "watch",
                            "next_trigger": "Trim below 1.5x EM",
                            "reason": "Monitor short strike",
                            "trigger_source": "Live EM",
                            "current_structure_grade": "Good",
                            "alert_state": {"last_alert_type": "watch-status"},
                        }
                    ],
                }
            )

            response = app.test_client().get("/hosted/manage-trades")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Hosted Delphi 6.3 pulls the same live open-trade evaluation data', response.data)
            self.assertIn(b'Watch', response.data)
            self.assertIn(b'Hold', response.data)
            self.assertIn(b'Send Real Status Update', response.data)
            self.assertIn(b'Send Simulated Status Update', response.data)
            self.assertIn(b'Send to Close', response.data)
            self.assertIn(b'/hosted/notifications', response.data)
            self.assertIn(b'Remaining Premium', response.data)
            self.assertNotIn(b'Net Credit', response.data)
            self.assertNotIn(b'Live EM x', response.data)
            self.assertNotIn(b'Mark', response.data)
            self.assertNotIn(b'Exit Now', response.data)
            self.assertNotIn(b'Save Notifications', response.data)

    def test_hosted_manage_trades_can_save_trade_notifications(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = _FakeTradeStore(
                {
                    "real": [
                        {
                            "id": 9,
                            "trade_number": 44,
                            "trade_mode": "real",
                            "status": "open",
                        }
                    ]
                },
                {"real": {"total_trades": 1, "open_trades": 1, "closed_trades": 0, "total_pnl": 0.0, "average_pnl": 0.0, "win_count": 0, "loss_count": 0}},
            )
            repository = _FakeTradeNotificationRepository()
            app.extensions["trade_notification_repository"] = repository

            response = app.test_client().post(
                "/hosted/manage-trades/9/notifications",
                data={
                    "notification_enabled_SHORT_STRIKE_PROXIMITY": "1",
                    "notification_threshold_SHORT_STRIKE_PROXIMITY": "7.5",
                    "notification_description_SHORT_STRIKE_PROXIMITY": "Watch the short strike",
                },
                follow_redirects=False,
            )

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.headers["Location"], "/hosted/manage-trades")
            self.assertEqual(len(repository.saved), 1)
            self.assertEqual(repository.saved[0]["trade_id"], 9)
            short_rule = next(item for item in repository.saved[0]["notifications"] if item["type"] == "SHORT_STRIKE_PROXIMITY")
            self.assertTrue(short_rule["enabled"])
            self.assertAlmostEqual(short_rule["threshold"], 7.5)
            self.assertEqual(short_rule["description"], "Watch the short strike")

    def test_hosted_performance_page_returns_admin_visible_error_when_supabase_trade_table_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            repository = self._build_missing_table_repository(temp_dir, "journal_trades")
            app.extensions["performance_service"] = PerformanceDashboardService(repository)

            response = app.test_client().get("/hosted/performance")

            self.assertEqual(response.status_code, 503)
            self.assertIn(b'Delphi 6.3 cannot load performance', response.data)
            self.assertIn(b'journal_trades', response.data)

    def test_hosted_journal_page_returns_admin_visible_error_when_supabase_trade_table_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["trade_store"] = self._build_missing_table_repository(temp_dir, "journal_trades")

            response = app.test_client().get("/hosted/journal?trade_mode=real")

            self.assertEqual(response.status_code, 503)
            self.assertIn(b'Delphi 6.3 cannot load journal', response.data)
            self.assertIn(b'journal_trade_close_events', response.data)

    def test_hosted_manage_trades_page_returns_admin_visible_error_when_supabase_trade_table_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            repository = self._build_missing_table_repository(temp_dir, "journal_trades")
            manager = _RepoBackedOpenTradeManager(repository)
            app.extensions["open_trade_manager"] = manager

            response = app.test_client().get("/hosted/manage-trades")

            self.assertEqual(response.status_code, 503)
            self.assertEqual(manager.calls, [])
            self.assertIn(b'Delphi 6.3 cannot load manage-trades', response.data)
            self.assertIn(b'active_trades', response.data)

    def test_hosted_apollo_page_renders_last_snapshot(self):
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
                    "next_market_day": "2026-04-13",
                    "provider_name": "Schwab",
                    "option_chain_status": "Ready",
                    "spx_value": "6129.20",
                    "vix_value": "18.30",
                    "hosted_result_heading": "Approved for next market day",
                    "hosted_plain_english": "Apollo generated two valid candidates.",
                    "reasons": ["Apollo generated two valid candidates."],
                    "trade_candidates_items": [{"mode_label": "Standard", "available": True, "short_strike": "6400", "long_strike": "6395", "net_credit": "$1.40", "max_loss": "$360.00", "em_multiple": "1.62x", "prefill_fields": {"candidate_profile": "Standard"}}],
                }
            )

            response = app.test_client().get("/hosted/apollo")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Apollo: Greek God of Prophecy and Part-Time Options Trader', response.data)
            self.assertIn(b'/hosted/apollo', response.data)
            self.assertIn(b'/hosted/apollo/prefill-candidate', response.data)
            self.assertIn(b'Send to Real Trades', response.data)
            self.assertIn(b'Send to Simulated Trades', response.data)
            self.assertIn(b'Approved for next market day', response.data)
            self.assertIn(b'Bullish', response.data)
            self.assertIn(b'6400 / 6395', response.data)
            self.assertNotIn(b'Run Apollo Live', response.data)
            self.assertNotIn(b'View Raw Snapshot', response.data)

    def test_hosted_kairos_page_renders_live_summary_cards(self):
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
                        "candidate_cards": [{"slot_label": "Subprime", "tradeable": True, "available": True, "strike_label": "6115 / 6110 Put Spread", "net_credit": "$1.55", "distance_to_short": "13.4 pts", "em_multiple": "1.58x", "message": "Qualified on live chain.", "prefill_enabled": True, "prefill_fields": {"candidate_profile": "Subprime"}}],
                    },
                    "lifecycle_items": [{"label": "Window Found", "value": "Reached"}],
                    "scan_log_count": 9,
                }
            )

            response = app.test_client().get("/hosted/kairos")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Run Kairos', response.data)
            self.assertIn(b'Kairos Watchtower', response.data)
            self.assertIn(b'Window Open', response.data)
            self.assertIn(b'Kairos Session Tape', response.data)
            self.assertIn(b'Kairos Credit Map', response.data)
            self.assertIn(b'Kairos Candidates', response.data)
            self.assertIn(b'Intraday Scan Log', response.data)
            self.assertIn(b'Best Available', response.data)
            self.assertIn(b'6115 / 6110 Put Spread', response.data)
            self.assertIn(b'/hosted/kairos/prefill-candidate', response.data)
            self.assertIn(b'/hosted/actions/kairos-workspace/live/status', response.data)
            self.assertNotIn(b'/kairos/live/status', response.data)

    def test_hosted_kairos_sim_page_renders_sim_workspace_without_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["kairos_sim_service"] = _FakeKairosService(
                {
                    "title": "Kairos",
                    "mode": "Simulation",
                    "mode_key": "simulation",
                    "mode_badge_text": "Simulation",
                    "session_status": "Ready",
                    "session_status_key": "ready",
                    "last_scan_display": "2026-04-12 10:15 AM CDT",
                    "latest_scan": {
                        "summary_text": "Simulator ready.",
                        "structure_status": "Loaded",
                        "momentum_status": "Neutral",
                        "timing_status": "Queued",
                        "kairos_state": "Ready",
                        "spx_value": "6128.40",
                        "vix_value": "18.20",
                    },
                    "simulation_controls": {
                        "historical_replay": {
                            "catalog_entries": [
                                {
                                    "scenario_key": "live-spx-tape-2026-04-13",
                                    "label": "Live SPX Tape 2026-04-13",
                                }
                            ]
                        }
                    },
                    "bar_map": {
                        "mode_key": "simulation",
                        "mode_label": "Simulation",
                        "processed_bars": 0,
                        "total_bars": 78,
                        "progress_percent": 0,
                        "latest_timestamp": "--",
                        "tape_source": "Supabase replay catalog",
                        "note": "Replay tape loaded from hosted storage.",
                    },
                    "lifecycle_items": [{"label": "Tape Source", "value": "Supabase"}],
                    "scan_log_count": 0,
                }
            )

            response = app.test_client().get("/hosted/kairos/sim")

            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Kairos Live', response.data)
            self.assertIn(b'Kairos Sim', response.data)
            self.assertIn(b'Kairos Simulation Lab', response.data)
            self.assertIn(b'live-spx-tape-2026-04-13', response.data)
            self.assertIn(b'/hosted/actions/kairos-workspace/sim/status', response.data)
            self.assertNotIn(b'/kairos/sim/status', response.data)

    def test_hosted_kairos_workspace_status_action_uses_hosted_route(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = self._create_hosted_app(temp_dir)
            self._allow_identity(app)
            app.extensions["kairos_sim_service"] = _FakeKairosService(
                {
                    "title": "Kairos",
                    "mode": "Simulation",
                    "mode_key": "simulation",
                    "current_state": "Stopped",
                    "bar_map": {"note": "Supabase replay catalog"},
                }
            )

            response = app.test_client().get("/hosted/actions/kairos-workspace/sim/status")

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["mode"], "Simulation")