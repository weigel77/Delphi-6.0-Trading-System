import unittest

from services.repositories.management_state_repository import (
    ACTIVE_TRADE_ALERT_LOG_TABLE,
    ACTIVE_TRADES_TABLE,
    JOURNAL_TRADES_TABLE,
    MANAGEMENT_RUNTIME_SETTINGS_TABLE,
    SupabaseOpenTradeManagementStateRepository,
)


class InMemoryGateway:
    def __init__(self):
        self.tables = {
            ACTIVE_TRADES_TABLE: [],
            ACTIVE_TRADE_ALERT_LOG_TABLE: [],
            MANAGEMENT_RUNTIME_SETTINGS_TABLE: [],
            JOURNAL_TRADES_TABLE: [],
        }

    def select(self, table, *, filters=None, order=None, limit=None, columns="*"):
        rows = [dict(row) for row in self.tables[table]]
        for key, expression in (filters or {}).items():
            operator, _, raw_value = str(expression).partition(".")
            if operator == "eq":
                rows = [row for row in rows if str(row.get(key)) == raw_value]
        if limit is not None:
            rows = rows[: int(limit)]
        return rows

    def insert(self, table, payload):
        row = dict(payload)
        self.tables[table].append(row)
        return [dict(row)]

    def update(self, table, payload, *, filters):
        rows = self.select(table, filters=filters)
        updated = []
        for candidate in self.tables[table]:
            if candidate not in rows:
                continue
            candidate.update(payload)
            updated.append(dict(candidate))
        return updated

    def delete(self, table, *, filters):
        raise AssertionError("delete should not be called in this test")


class SupabaseManagementStateRepositoryTest(unittest.TestCase):
    def test_repository_tracks_runtime_settings_and_trade_state(self):
        gateway = InMemoryGateway()
        gateway.insert(
            JOURNAL_TRADES_TABLE,
            {
                "id": 7,
                "last_status": None,
                "last_action_sent": None,
                "last_alert_timestamp": None,
                "updated_at": "2026-04-13T09:30:00",
            },
        )
        repository = SupabaseOpenTradeManagementStateRepository(gateway)

        repository.initialize()
        repository.set_notifications_enabled(False)
        repository.upsert_management_state(
            {
                "trade_id": 7,
                "system_name": "Apollo",
                "trade_mode": "real",
                "status": "Watch",
                "thesis_status": "Intact",
                "reason": "Watching short strike distance.",
                "reason_code": "watch-distance",
                "next_trigger": "Touch short strike",
                "trigger_source": "distance",
                "current_underlying_price": 6120.4,
                "distance_to_short": 14.2,
                "distance_to_long": 19.2,
                "percent_buffer_remaining": 22.4,
            },
            {},
            {"sent": False, "sent_count": 0},
            __import__("datetime").datetime(2026, 4, 13, 10, 15),
        )
        repository.record_alert(
            trade_id=7,
            system_name="Apollo",
            trade_mode="real",
            alert_type="watch-status",
            alert_priority=0,
            alert_priority_label="Normal",
            reason_code="watch-distance",
            title="Watch",
            body="Watching trade.",
            sent_at="2026-04-13T10:15:00",
        )
        repository.update_trade_notification_state(
            trade_id=7,
            last_status="Watch",
            last_action_sent="Review",
            last_alert_timestamp="2026-04-13T10:15:00",
        )

        settings = repository.load_runtime_settings()
        state = repository.load_management_state(7)
        trade = repository.load_trade(7)

        self.assertFalse(settings["notifications_enabled"])
        self.assertEqual(state["current_management_status"], "Watch")
        self.assertEqual(state["reason_code"], "watch-distance")
        self.assertEqual(trade["last_status"], "Watch")
        self.assertEqual(trade["last_action_sent"], "Review")
        self.assertEqual(len(gateway.tables[ACTIVE_TRADE_ALERT_LOG_TABLE]), 1)