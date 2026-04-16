import tempfile
import unittest
from pathlib import Path

from services.repositories.trade_notification_repository import SQLiteTradeNotificationRepository
from services.trade_notifications import evaluate_trade_notifications, normalize_trade_notifications


class TradeNotificationTests(unittest.TestCase):
    def test_evaluate_trade_notifications_fires_supported_rules(self):
        trade = {
            "notifications": [
                {"type": "SHORT_STRIKE_PROXIMITY", "enabled": True, "threshold": 8, "description": "Short strike proximity"},
                {"type": "LONG_STRIKE_TOUCH", "enabled": True, "description": "Long strike touch"},
                {"type": "VWAP_BREAK", "enabled": True, "description": "VWAP break"},
                {"type": "STRUCTURE_BREAK", "enabled": True, "description": "Structure break"},
                {"type": "TIME_WINDOW", "enabled": True, "threshold": 1.5, "description": "Time window"},
            ]
        }
        market_data = {
            "distance_to_short": 5.0,
            "distance_to_long": 0.0,
            "long_proximity_trigger_fired": True,
            "vwap_trigger_fired": True,
            "structure_trigger_fired": True,
            "time_remaining_to_expiration": 3600,
        }

        triggered = evaluate_trade_notifications(trade, market_data)

        self.assertEqual(
            {item["type"] for item in triggered},
            {
                "SHORT_STRIKE_PROXIMITY",
                "LONG_STRIKE_TOUCH",
                "VWAP_BREAK",
                "STRUCTURE_BREAK",
                "TIME_WINDOW",
            },
        )

    def test_sqlite_trade_notification_repository_persists_thresholds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            repository = SQLiteTradeNotificationRepository(Path(temp_dir) / "trade_notifications.db")
            repository.initialize()
            repository.save_trade_notifications(
                12,
                [
                    {"type": "SHORT_STRIKE_PROXIMITY", "enabled": True, "threshold": 6.5, "description": "Watch short"},
                    {"type": "TIME_WINDOW", "enabled": True, "threshold": 0.75, "description": "Expiration window"},
                ],
            )

            notifications = repository.load_trade_notifications(12)

        short_rule = next(item for item in notifications if item["type"] == "SHORT_STRIKE_PROXIMITY")
        time_rule = next(item for item in notifications if item["type"] == "TIME_WINDOW")
        disabled_rule = next(item for item in notifications if item["type"] == "VWAP_BREAK")

        self.assertTrue(short_rule["enabled"])
        self.assertAlmostEqual(short_rule["threshold"], 6.5)
        self.assertEqual(short_rule["description"], "Watch short")
        self.assertTrue(time_rule["enabled"])
        self.assertAlmostEqual(time_rule["threshold"], 0.75)
        self.assertEqual(time_rule["description"], "Expiration window")
        self.assertTrue(disabled_rule["enabled"])

    def test_normalize_trade_notifications_restores_default_rows(self):
        notifications = normalize_trade_notifications([{"type": "VWAP_BREAK", "enabled": True, "description": "VWAP alert"}])

        self.assertEqual(len(notifications), 5)
        vwap_rule = next(item for item in notifications if item["type"] == "VWAP_BREAK")
        short_rule = next(item for item in notifications if item["type"] == "SHORT_STRIKE_PROXIMITY")

        self.assertTrue(vwap_rule["enabled"])
        self.assertEqual(vwap_rule["description"], "VWAP alert")
        self.assertTrue(short_rule["enabled"])
        self.assertAlmostEqual(short_rule["threshold"], 10.0)


if __name__ == "__main__":
    unittest.main()