import tempfile
import unittest
from pathlib import Path

from config import AppConfig
from app import create_app
from services.providers.schwab_provider import SchwabProvider
from services.repositories.hosted_runtime_state_repository import SupabaseApolloSnapshotRepository, SupabaseImportPreviewRepository, SupabaseTokenRepository
from services.repositories.management_state_repository import SupabaseOpenTradeManagementStateRepository
from services.runtime.hosted_auth import SupabaseHostedIdentityResolver, SupabasePrivateAccessGate, SupabaseSessionInvalidator
from services.runtime.auth_composition import HostedSupabaseAuthComposer, LocalAuthComposer
from services.runtime.hosted_composition import HostedRuntimeServiceComposerSkeleton
from services.runtime.private_access import LocalPrivateAccessGate, LocalRequestIdentityResolver, NoopSessionInvalidator
from services.runtime.provider_composition import LocalProviderComposer
from services.repositories.trade_repository import SQLiteTradeRepository, SupabaseTradeRepository
from services.runtime.service_composition import LocalRuntimeServiceComposer


class RuntimeCompositionTest(unittest.TestCase):
    def test_local_auth_composer_creates_schwab_auth_service_with_json_token_repository(self):
        config = AppConfig(schwab_token_path="instance/test-token.json")

        composer = LocalAuthComposer(config)
        auth_service = composer.create_schwab_auth_service()

        self.assertEqual(auth_service.token_store.file_path.as_posix().replace("\\", "/"), "instance/test-token.json")

    def test_local_provider_composer_preserves_schwab_live_and_historical_bootstrap(self):
        config = AppConfig(market_data_provider="schwab")

        composer = LocalProviderComposer(config)
        live_provider = composer.create_live_provider()
        historical_providers = composer.create_historical_providers()

        self.assertIsInstance(live_provider, SchwabProvider)
        self.assertEqual(historical_providers["^GSPC"].get_metadata()["provider_key"], "schwab")

    def test_create_app_exposes_composition_boundary_and_composed_bundle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "composed-runtime.db"
            app = create_app({"TESTING": True, "TRADE_DATABASE": str(database_path)})

            self.assertIn("auth_composer", app.extensions)
            self.assertIn("host_infrastructure", app.extensions)
            self.assertIn("provider_composer", app.extensions)
            self.assertIn("service_composer", app.extensions)
            self.assertIn("service_bundle", app.extensions)

            bundle = app.extensions["service_bundle"]
            self.assertIs(bundle.host_infrastructure, app.extensions["host_infrastructure"])
            self.assertIs(app.extensions["market_data_service"], bundle.market_data_service)
            self.assertIs(app.extensions["open_trade_manager"], bundle.open_trade_manager)
            self.assertIs(app.extensions["kairos_live_service"], bundle.kairos_live_service)
            self.assertIs(bundle.apollo_service.options_chain_service.provider, bundle.market_data_service.live_provider)
            self.assertIs(bundle.kairos_live_service.options_chain_service, bundle.apollo_service.options_chain_service)
            self.assertIs(bundle.kairos_sim_service.options_chain_service, bundle.apollo_service.options_chain_service)

            self.assertIsInstance(app.extensions["auth_composer"], LocalAuthComposer)
            self.assertIsInstance(app.extensions["provider_composer"], LocalProviderComposer)
            self.assertIsInstance(app.extensions["service_composer"], LocalRuntimeServiceComposer)
            self.assertIsInstance(app.extensions["trade_store"], SQLiteTradeRepository)
            self.assertIsInstance(app.extensions["request_identity_resolver"], LocalRequestIdentityResolver)
            self.assertIsInstance(app.extensions["private_access_gate"], LocalPrivateAccessGate)
            self.assertIsInstance(app.extensions["session_invalidator"], NoopSessionInvalidator)

    def test_create_app_can_select_hosted_runtime_composer_skeleton(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "hosted-skeleton.db"
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

            self.assertEqual(app.extensions["host_infrastructure"].host_kind, "hosted")
            self.assertEqual(app.extensions["runtime_profile"].profile_name, "hosted-testing")
            self.assertIsInstance(app.extensions["service_composer"], HostedRuntimeServiceComposerSkeleton)
            self.assertIsInstance(app.extensions["auth_composer"], HostedSupabaseAuthComposer)
            self.assertIsInstance(app.extensions["trade_store"], SupabaseTradeRepository)
            self.assertIs(app.extensions["trade_store_backend"], app.extensions["trade_store"])
            self.assertIsInstance(app.extensions["apollo_snapshot_repository"], SupabaseApolloSnapshotRepository)
            self.assertIsInstance(app.extensions["import_preview_repository"], SupabaseImportPreviewRepository)
            self.assertIsInstance(app.extensions["auth_composer"].create_token_repository(), SupabaseTokenRepository)
            self.assertIsInstance(app.extensions["open_trade_manager"].state_repository, SupabaseOpenTradeManagementStateRepository)
            self.assertIsInstance(app.extensions["request_identity_resolver"], SupabaseHostedIdentityResolver)
            self.assertIsInstance(app.extensions["private_access_gate"], SupabasePrivateAccessGate)
            self.assertIsInstance(app.extensions["session_invalidator"], SupabaseSessionInvalidator)