"""Market-calendar helpers for Apollo next-market-day workflows."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict
from zoneinfo import ZoneInfo

from config import AppConfig, get_app_config


LOGGER = logging.getLogger(__name__)


class MarketCalendarService:
    """Determine Apollo market-calendar dates using local Chicago time."""

    MARKET_CLOSE_HOUR = 15
    MARKET_CLOSE_MINUTE = 0

    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or get_app_config()
        self.display_timezone = ZoneInfo(self.config.app_timezone)

    def get_next_market_day_context(self, current_time: datetime | None = None) -> Dict[str, Any]:
        """Return the next tradable Apollo market day and related diagnostics."""
        local_now = current_time.astimezone(self.display_timezone) if current_time else datetime.now(self.display_timezone)
        return {
            "local_datetime": local_now,
            **self.get_next_tradable_market_day(local_now),
        }

    def get_next_tradable_market_day(self, current_time: datetime | None = None) -> Dict[str, Any]:
        """Return the next tradable day after applying weekend and holiday filters."""
        local_now = current_time.astimezone(self.display_timezone) if current_time else datetime.now(self.display_timezone)
        base_date = local_now.date()
        base_status = self._get_market_day_status(base_date)
        after_close = (
            local_now.hour > self.MARKET_CLOSE_HOUR
            or (local_now.hour == self.MARKET_CLOSE_HOUR and local_now.minute >= self.MARKET_CLOSE_MINUTE)
        )

        candidate = base_date + timedelta(days=1) if base_status["is_tradable"] else base_date
        candidate_date_considered = candidate
        weekend_filter_triggered = False
        holiday_filter_triggered = False
        skipped_holidays: list[Dict[str, Any]] = []

        while True:
            status = self._get_market_day_status(candidate)
            if status["is_tradable"]:
                break
            if status["is_weekend"]:
                weekend_filter_triggered = True
            if status["holiday_name"]:
                holiday_filter_triggered = True
                skipped_holidays.append({"date": candidate, "name": status["holiday_name"]})
            candidate += timedelta(days=1)

        LOGGER.info(
            "Apollo market-day selection | candidate_date_considered=%s | weekend_filter_triggered=%s | holiday_filter_triggered=%s | final_selected_market_day=%s",
            candidate_date_considered.isoformat(),
            weekend_filter_triggered,
            holiday_filter_triggered,
            candidate.isoformat(),
        )

        note = (
            f"Apollo considered {candidate_date_considered.isoformat()} and confirmed it as the next tradable market day."
            if candidate == candidate_date_considered
            else f"Apollo considered {candidate_date_considered.isoformat()} and rolled forward to {candidate.isoformat()}."
        )
        if weekend_filter_triggered:
            note += " Weekend filter applied."
        if holiday_filter_triggered:
            skipped_names = ", ".join(str(item["name"]) for item in skipped_holidays)
            note += f" Holiday filter applied. Skipped holiday: {skipped_names}."
        if after_close and base_status["is_tradable"]:
            note += " Current local time is after the regular market close."

        return {
            "next_market_day": candidate,
            "candidate_date_considered": candidate_date_considered,
            "weekend_filter_applied": weekend_filter_triggered,
            "holiday_filter_applied": holiday_filter_triggered,
            "holiday_filter_applied_label": "Yes" if holiday_filter_triggered else "No",
            "skipped_holidays": skipped_holidays,
            "skipped_holiday_name": skipped_holidays[0]["name"] if skipped_holidays else None,
            "note": note,
        }

    def is_tradable_market_day(self, value: date) -> bool:
        """Return whether the exchange is open for a tradable market session."""
        return self._get_market_day_status(value)["is_tradable"]

    def get_next_or_same_tradable_market_day(self, value: date) -> date:
        """Roll forward to the next tradable market day, preserving valid inputs."""
        candidate = value
        while not self.is_tradable_market_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def get_holiday_name(self, value: date) -> str | None:
        """Return the observed holiday name for a closed exchange date."""
        return self._get_market_day_status(value)["holiday_name"]

    @staticmethod
    def _is_trading_weekday(value: date) -> bool:
        return value.weekday() < 5

    def _get_market_day_status(self, value: date) -> Dict[str, Any]:
        is_weekend = not self._is_trading_weekday(value)
        holiday_name = self._get_exchange_holiday_name(value)
        return {
            "is_tradable": not is_weekend and holiday_name is None,
            "is_weekend": is_weekend,
            "holiday_name": holiday_name,
        }

    def _get_exchange_holiday_name(self, value: date) -> str | None:
        return self._get_exchange_holidays(value.year).get(value)

    def _get_exchange_holidays(self, year: int) -> Dict[date, str]:
        good_friday = self._calculate_easter_sunday(year) - timedelta(days=2)
        return {
            self._observed_date(date(year, 1, 1)): "New Year's Day",
            self._nth_weekday_of_month(year, 1, 0, 3): "Martin Luther King Jr. Day",
            self._nth_weekday_of_month(year, 2, 0, 3): "Presidents Day",
            good_friday: "Good Friday",
            self._last_weekday_of_month(year, 5, 0): "Memorial Day",
            self._observed_date(date(year, 6, 19)): "Juneteenth National Independence Day",
            self._observed_date(date(year, 7, 4)): "Independence Day",
            self._nth_weekday_of_month(year, 9, 0, 1): "Labor Day",
            self._nth_weekday_of_month(year, 11, 3, 4): "Thanksgiving Day",
            self._observed_date(date(year, 12, 25)): "Christmas Day",
        }

    @staticmethod
    def _observed_date(value: date) -> date:
        if value.weekday() == 5:
            return value - timedelta(days=1)
        if value.weekday() == 6:
            return value + timedelta(days=1)
        return value

    @staticmethod
    def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> date:
        candidate = date(year, month, 1)
        candidate += timedelta(days=(weekday - candidate.weekday()) % 7)
        candidate += timedelta(weeks=occurrence - 1)
        return candidate

    @staticmethod
    def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
        if month == 12:
            candidate = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            candidate = date(year, month + 1, 1) - timedelta(days=1)
        while candidate.weekday() != weekday:
            candidate -= timedelta(days=1)
        return candidate

    @staticmethod
    def _calculate_easter_sunday(year: int) -> date:
        a = year % 19
        b = year // 100
        c = year % 100
        d = b // 4
        e = b % 4
        f = (b + 8) // 25
        g = (b - f + 1) // 3
        h = (19 * a + b - d - g + 15) % 30
        i = c // 4
        k = c % 4
        l = (32 + 2 * e + 2 * i - h - k) % 7
        m = (a + 11 * h + 22 * l) // 451
        month = (h + l - 7 * m + 114) // 31
        day = ((h + l - 7 * m + 114) % 31) + 1
        return date(year, month, day)
