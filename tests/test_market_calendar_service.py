from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from services.market_calendar_service import MarketCalendarService


CHICAGO = ZoneInfo("America/Chicago")


class MarketCalendarServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MarketCalendarService()

    def test_after_close_skips_good_friday_2026(self) -> None:
        current_time = datetime(2026, 4, 2, 15, 30, tzinfo=CHICAGO)

        context = self.service.get_next_market_day_context(current_time)

        self.assertEqual(context["candidate_date_considered"].isoformat(), "2026-04-03")
        self.assertEqual(context["next_market_day"].isoformat(), "2026-04-06")
        self.assertTrue(context["holiday_filter_applied"])
        self.assertEqual(context["skipped_holiday_name"], "Good Friday")

    def test_weekend_rolls_to_monday(self) -> None:
        current_time = datetime(2026, 4, 4, 10, 0, tzinfo=CHICAGO)

        context = self.service.get_next_market_day_context(current_time)

        self.assertEqual(context["candidate_date_considered"].isoformat(), "2026-04-04")
        self.assertEqual(context["next_market_day"].isoformat(), "2026-04-06")
        self.assertTrue(context["weekend_filter_applied"])
        self.assertFalse(context["holiday_filter_applied"])

    def test_ordinary_weekday_rollover(self) -> None:
        current_time = datetime(2026, 4, 1, 16, 0, tzinfo=CHICAGO)

        context = self.service.get_next_market_day_context(current_time)

        self.assertEqual(context["candidate_date_considered"].isoformat(), "2026-04-02")
        self.assertEqual(context["next_market_day"].isoformat(), "2026-04-02")
        self.assertFalse(context["weekend_filter_applied"])
        self.assertFalse(context["holiday_filter_applied"])


if __name__ == "__main__":
    unittest.main()