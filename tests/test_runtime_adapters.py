import unittest

from flask import Flask

from services.runtime.notifications import PushoverNotificationDelivery
from services.runtime.scheduler import ThreadingTimerScheduler
from services.runtime.workflow_state import FlaskSessionWorkflowState


class StubPushoverService:
    def __init__(self):
        self.calls = []

    def send_notification(self, **kwargs):
        self.calls.append(("send_notification", kwargs))
        return {"ok": True, "request_id": "req-1"}

    def send_kairos_window_open_alert(self, **kwargs):
        self.calls.append(("send_kairos_window_open_alert", kwargs))
        return {"ok": True, "request_id": "req-2"}


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


class RuntimeAdapterTest(unittest.TestCase):
    def test_pushover_notification_delivery_delegates_to_service(self):
        backend = StubPushoverService()
        delivery = PushoverNotificationDelivery(backend)

        notification_result = delivery.send_notification(title="Title", message="Body", priority=1)
        kairos_result = delivery.send_kairos_window_open_alert(
            scan_result={"state": "Window Open"},
            best_trade_payload={"candidate": {"strike_pair": "6000/5990"}},
        )

        self.assertTrue(notification_result["ok"])
        self.assertTrue(kairos_result["ok"])
        self.assertEqual(backend.calls[0][0], "send_notification")
        self.assertEqual(backend.calls[0][1]["priority"], 1)
        self.assertEqual(backend.calls[1][0], "send_kairos_window_open_alert")

    def test_threading_timer_scheduler_starts_and_cancels_fake_timer(self):
        FakeTimer.instances = []
        scheduler = ThreadingTimerScheduler(FakeTimer)

        handle = scheduler.schedule(30.0, lambda: None, daemon=True)

        self.assertEqual(len(FakeTimer.instances), 1)
        timer = FakeTimer.instances[0]
        self.assertEqual(timer.interval, 30.0)
        self.assertTrue(timer.daemon)
        self.assertTrue(timer.started)

        handle.cancel()

        self.assertTrue(timer.cancelled)

    def test_flask_session_workflow_state_preserves_current_prefill_and_status_behavior(self):
        app = Flask(__name__)
        app.secret_key = "test-secret"
        workflow_state = FlaskSessionWorkflowState(
            trade_prefill_key="apollo_trade_prefill",
            trade_close_prefill_key="management_close_prefill",
            trade_form_fields=("trade_mode", "system_name", "candidate_profile"),
            trade_mode_resolver=lambda value: str(value or "").strip().lower() or "real",
        )

        with app.test_request_context("/"):
            workflow_state.set_status_message("Saved", level="info")
            workflow_state.store_trade_prefill(
                "REAL",
                {
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                    "notes_entry": "ignored",
                },
            )
            workflow_state.store_trade_close_prefill(42, {"close_method": "market", "contracts_closed": 1})
            workflow_state.put("oauth_state", "abc123")

            self.assertEqual(workflow_state.pop_status_message(), {"text": "Saved", "level": "info"})
            self.assertEqual(
                workflow_state.get_trade_prefill("real"),
                {
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "candidate_profile": "Standard",
                },
            )
            self.assertEqual(workflow_state.get_trade_close_prefill(42), {"close_method": "market", "contracts_closed": 1})
            self.assertEqual(workflow_state.pop("oauth_state"), "abc123")

            workflow_state.clear_trade_prefill("real")
            workflow_state.clear_trade_close_prefill(42)

            self.assertIsNone(workflow_state.get_trade_prefill("real"))
            self.assertIsNone(workflow_state.get_trade_close_prefill(42))