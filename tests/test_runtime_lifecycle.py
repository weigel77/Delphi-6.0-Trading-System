import tempfile
import unittest
from pathlib import Path

from config import get_app_config
from app import create_app, get_launch_url, get_runtime_lifecycle, get_runtime_profile, should_use_https
from services.runtime.lifecycle import LocalRuntimeLifecycleCoordinator, RuntimeComponent
from services.runtime.profile import select_runtime_profile
from services.runtime.scheduler import ThreadingTimerScheduler


class FakeTimer:
    instances = []

    def __init__(self, interval, callback):
        self.interval = interval
        self.callback = callback
        self.daemon = False
        self.started = False
        self.cancelled = False
        FakeTimer.instances.append(self)

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True


class RuntimeLifecycleTest(unittest.TestCase):
    def test_runtime_profile_selection_preserves_local_testing_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app({"TESTING": True, "TRADE_DATABASE": str(Path(temp_dir) / "runtime-profile.db")})

            profile = select_runtime_profile(app, get_app_config())

            self.assertTrue(profile.testing)
            self.assertEqual(profile.profile_name, "local-testing")
            self.assertFalse(profile.auto_open_browser)
            self.assertFalse(profile.auto_start_background_services)
            self.assertFalse(profile.register_shutdown_hooks)

    def test_local_runtime_lifecycle_coordinator_starts_stops_and_schedules_launch(self):
        started = []
        stopped = []
        launch_urls = []
        shutdown_hooks = []
        FakeTimer.instances = []

        class StubLaunchBehavior:
            def launch(self, profile):
                launch_urls.append(profile.launch_url)

        profile = type("Profile", (), {
            "launch_url": "http://localhost:5000",
            "auto_open_browser": True,
            "browser_launch_delay_seconds": 1.0,
            "auto_start_background_services": True,
            "register_shutdown_hooks": True,
        })()
        scheduler = ThreadingTimerScheduler(FakeTimer)
        coordinator = LocalRuntimeLifecycleCoordinator(
            profile,
            launch_behavior=StubLaunchBehavior(),
            scheduler=scheduler,
            shutdown_registrar=shutdown_hooks.append,
        )
        coordinator.register_component(RuntimeComponent("alpha", startup=lambda: started.append("alpha"), shutdown=lambda: stopped.append("alpha")))
        coordinator.register_component(RuntimeComponent("beta", shutdown=lambda: stopped.append("beta")))

        coordinator.start_runtime()
        coordinator.schedule_launch()

        self.assertEqual(started, ["alpha"])
        self.assertEqual(len(shutdown_hooks), 1)
        self.assertEqual(len(FakeTimer.instances), 1)
        FakeTimer.instances[-1].callback()
        self.assertEqual(launch_urls, ["http://localhost:5000"])

        coordinator.stop_runtime()
        self.assertEqual(stopped, ["beta", "alpha"])

    def test_create_app_exposes_runtime_boundary_and_testing_profile(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            app = create_app({"TESTING": True, "TRADE_DATABASE": str(Path(temp_dir) / "runtime-app.db")})

            self.assertIn("runtime_profile", app.extensions)
            self.assertIn("runtime_lifecycle", app.extensions)
            self.assertIn("runtime_components", app.extensions)

            profile = get_runtime_profile(app)
            lifecycle = get_runtime_lifecycle(app)

            self.assertTrue(profile.testing)
            self.assertFalse(profile.auto_open_browser)
            self.assertFalse(profile.auto_start_background_services)
            with app.app_context():
                self.assertEqual(get_launch_url(), get_runtime_profile(app).launch_url)
                self.assertEqual(should_use_https(), get_runtime_profile(app).use_https)
            self.assertIs(lifecycle, app.extensions["runtime_lifecycle"])
            self.assertEqual([component.name for component in app.extensions["runtime_components"]], [
                "open_trade_manager",
                "kairos_live_service",
                "kairos_sim_service",
            ])