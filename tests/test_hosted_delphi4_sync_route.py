import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import create_app
from services.runtime.private_access import RequestIdentity


class _FixedIdentityResolver:
    def __init__(self, identity: RequestIdentity):
        self.identity = identity

    def resolve_request_identity(self, request):
        return self.identity


class _FakeSyncResult:
    def __init__(self, *, dry_run: bool):
        self.dry_run = dry_run

    def to_payload(self):
        return {
            "dry_run": self.dry_run,
            "new_trade_numbers": [701],
            "new_close_event_count": 2,
            "new_tape_session_dates": ["2026-04-11"],
            "new_tape_scenario_keys": ["live-spx-tape-2026-04-11"],
            "had_changes": True,
            "completed_at": "2026-04-14T20:15:00+00:00",
            "last_successful_sync_at": "2026-04-14T20:15:00+00:00",
            "source_paths": {
                "source_root": "C:/Users/weige/Delphi-4.3-Production",
                "trade_database_path": "C:/Users/weige/Delphi-4.3-Production/instance/horme_trades.db",
                "kairos_replay_dir": "C:/Users/weige/Delphi-4.3-Production/instance/kairos_replays",
            },
        }


class HostedDelphi4SyncRouteTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.app = create_app(
            {
                "TESTING": True,
                "RUNTIME_TARGET": "hosted",
                "SUPABASE_URL": "https://project.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                "SUPABASE_SECRET_KEY": "secret-key",
                "DELPHI_HOSTED_ALLOWED_EMAILS": "bill@example.com",
                "TRADE_DATABASE": str(Path(self.temp_dir.name) / "hosted.db"),
            }
        )
        self.app.extensions["request_identity_resolver"] = _FixedIdentityResolver(
            RequestIdentity(
                user_id="user-1",
                email="bill@example.com",
                display_name="Bill",
                authenticated=True,
                auth_source="supabase-hosted",
            )
        )
        self.client = self.app.test_client()

    def test_hosted_home_shows_sync_panel_only_on_local_dev_host(self):
        with patch("app.resolve_delphi4_source_paths") as source_paths_mock, patch("app.load_last_successful_sync_timestamp", return_value=None):
            source_paths_mock.return_value = type("Paths", (), {
                "source_root": Path("C:/Users/weige/Delphi-4.3-Production"),
                "trade_database_path": Path("C:/Users/weige/Delphi-4.3-Production/instance/horme_trades.db"),
                "kairos_replay_dir": Path("C:/Users/weige/Delphi-4.3-Production/instance/kairos_replays"),
            })()
            local_response = self.client.get("/hosted", base_url="https://127.0.0.1:5015")
            remote_response = self.client.get("/hosted", base_url="https://delphi.example.com")

        self.assertEqual(local_response.status_code, 200)
        self.assertIn(b"Sync New 4.3 Data", local_response.data)
        self.assertEqual(remote_response.status_code, 200)
        self.assertNotIn(b"Sync New 4.3 Data", remote_response.data)

    def test_sync_post_runs_dry_run_and_redirects_home(self):
        with patch("app.resolve_delphi4_source_paths") as source_paths_mock, patch("app.run_incremental_delphi4_sync", return_value=_FakeSyncResult(dry_run=True)) as sync_mock:
            source_paths_mock.return_value = type("Paths", (), {
                "source_root": Path("C:/Users/weige/Delphi-4.3-Production"),
                "trade_database_path": Path("C:/Users/weige/Delphi-4.3-Production/instance/horme_trades.db"),
                "kairos_replay_dir": Path("C:/Users/weige/Delphi-4.3-Production/instance/kairos_replays"),
            })()
            response = self.client.post(
                "/hosted/dev/sync-delphi4",
                data={"sync_action": "dry-run"},
                base_url="https://127.0.0.1:5015",
                follow_redirects=True,
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Dry run found 1 trades, 2 close events, and 1 live Kairos tapes.", response.data)
        self.assertIn(b"Trade Numbers: 701", response.data)
        sync_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()