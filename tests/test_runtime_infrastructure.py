import tempfile
import unittest
from pathlib import Path

from app import create_app


class RuntimeInfrastructureTest(unittest.TestCase):
    def test_local_host_infrastructure_preserves_local_filesystem_defaults(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "local-infra.db"
            replay_path = Path(temp_dir) / "kairos-replays"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "local",
                    "TRADE_DATABASE": str(database_path),
                    "KAIROS_REPLAY_STORAGE_DIR": str(replay_path),
                }
            )

            infrastructure = app.extensions["host_infrastructure"]

            self.assertEqual(infrastructure.host_kind, "local")
            self.assertEqual(infrastructure.settings.runtime_target, "local")
            self.assertEqual(infrastructure.storage.trade_database_path, database_path)
            self.assertEqual(infrastructure.storage.kairos_replay_storage_dir, replay_path)
            self.assertEqual(infrastructure.storage.import_preview_root, Path(app.instance_path))
            self.assertEqual(app.config["TRADE_DATABASE"], str(database_path))
            self.assertEqual(app.config["KAIROS_REPLAY_STORAGE_DIR"], str(replay_path))
            self.assertEqual(app.config["APP_DISPLAY_NAME"], "Delphi 7.2.0 Local")
            self.assertEqual(app.config["APP_VERSION_LABEL"], "Version 7.2.0")
            self.assertIn("talos_service", app.extensions)

    def test_unified_runtime_ignores_hosted_overrides_and_keeps_local_surface(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "unified-infra.db"
            app = create_app(
                {
                    "TESTING": True,
                    "RUNTIME_TARGET": "hosted",
                    "HOSTED_PUBLIC_BASE_URL": "https://hosted.example.test",
                    "SUPABASE_URL": "https://project.supabase.co",
                    "SUPABASE_PUBLISHABLE_KEY": "publishable-key",
                    "SUPABASE_SECRET_KEY": "secret-key",
                    "TRADE_DATABASE": str(database_path),
                }
            )

            infrastructure = app.extensions["host_infrastructure"]
            profile = app.extensions["runtime_profile"]

            self.assertEqual(infrastructure.host_kind, "local")
            self.assertEqual(infrastructure.settings.runtime_target, "local")
            self.assertEqual(app.config["RUNTIME_TARGET"], "local")
            self.assertEqual(app.config["HOSTED_PUBLIC_BASE_URL"], "")
            self.assertEqual(app.config["APP_PORT"], 5001)
            self.assertEqual(app.config["APP_DISPLAY_NAME"], "Delphi 7.2.0 Local")
            self.assertEqual(app.config["APP_VERSION_LABEL"], "Version 7.2.0")
            self.assertEqual(profile.host, "127.0.0.1")
            self.assertEqual(profile.port, 5001)
            self.assertTrue(profile.use_https)

            client = app.test_client()
            self.assertEqual(client.get("/").status_code, 302)
            self.assertEqual(client.get("/hosted").status_code, 404)
            self.assertEqual(client.get("/hosted/manage-trades").status_code, 404)
            self.assertEqual(client.get("/hosted/kairos/live").status_code, 404)
            self.assertEqual(client.get("/sign-in").status_code, 404)