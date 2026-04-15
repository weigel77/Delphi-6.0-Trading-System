import tempfile
import unittest
from pathlib import Path
from urllib.error import URLError

from config import AppConfig
from app import create_app
from services.runtime.supabase_integration import SupabaseConfig, SupabaseProjectIntegration


class _StubResponse:
    def __init__(self, status_code=200):
        self.status = status_code

    def getcode(self):
        return self.status

    def close(self):
        return None


class SupabaseIntegrationTest(unittest.TestCase):
    def test_supabase_config_resolves_from_app_config_and_app_overrides(self):
        config = AppConfig(
            supabase_url="https://project.supabase.co",
            supabase_publishable_key="publishable-key",
            supabase_secret_key="secret-key",
        )
        app = create_app(
            {
                "TESTING": True,
                "SUPABASE_URL": "https://override.supabase.co",
                "SUPABASE_PUBLISHABLE_KEY": "override-publishable",
                "TRADE_DATABASE": str(Path(tempfile.gettempdir()) / "supabase-config.db"),
            }
        )

        resolved = SupabaseConfig.resolve(app, config)

        self.assertEqual(resolved.url, "https://override.supabase.co")
        self.assertEqual(resolved.publishable_key, "override-publishable")
        self.assertEqual(resolved.secret_key, "secret-key")

    def test_supabase_connectivity_check_succeeds_against_safe_health_path(self):
        config = SupabaseConfig(
            url="https://project.supabase.co",
            publishable_key="publishable-key",
            secret_key="secret-key",
        )
        observed = []

        def requester(request, timeout=0):
            observed.append((request.full_url, timeout, dict(request.header_items())))
            return _StubResponse(200)

        integration = SupabaseProjectIntegration(config, requester=requester)
        result = integration.check_connectivity(timeout_seconds=3.5)

        self.assertTrue(result.ok)
        self.assertEqual(result.endpoint, "/auth/v1/settings")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(observed[0][0], "https://project.supabase.co/auth/v1/settings")
        self.assertEqual(observed[0][1], 3.5)
        self.assertIn(("Apikey", "secret-key"), observed[0][2].items())

    def test_supabase_connectivity_check_reports_network_failure_cleanly(self):
        config = SupabaseConfig(
            url="https://project.supabase.co",
            publishable_key="publishable-key",
            secret_key="",
        )

        def requester(request, timeout=0):
            raise URLError("offline")

        integration = SupabaseProjectIntegration(config, requester=requester)
        result = integration.check_connectivity()

        self.assertFalse(result.ok)
        self.assertIn("network error", result.detail.lower())

    def test_hosted_create_app_initializes_supabase_foundation_context(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "hosted-supabase.db"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "TRADE_DATABASE": str(database_path),
                }
            )

            context = app.extensions["supabase_context"]
            integration = app.extensions["supabase_integration"]

            self.assertIsNotNone(integration)
            self.assertTrue(context.configured)
            self.assertEqual(context.rest_url, "https://project.supabase.co/rest/v1")
            self.assertEqual(context.auth_url, "https://project.supabase.co/auth/v1")
            self.assertEqual(context.config.project_ref, "project")