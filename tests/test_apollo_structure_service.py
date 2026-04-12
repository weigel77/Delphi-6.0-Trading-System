from __future__ import annotations

import unittest
from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pandas as pd

from config import AppConfig
from services.apollo_structure_service import ApolloStructureService
from services.market_data import MarketDataError


class _FakeMarketDataService:
    def __init__(self) -> None:
        self.calls: list[date] = []
        self.history_error: str | None = None

    def get_provider_metadata(self):
        return {"live_provider_name": "Fake Provider"}

    def get_intraday_candles_for_date(self, ticker, target_date, interval_minutes=5, query_type="intraday_date"):
        self.calls.append(target_date)
        if target_date == date(2026, 4, 3):
            raise MarketDataError("market closed")
        if target_date == date(2026, 4, 2):
            rows = []
            chicago = ZoneInfo("America/Chicago")
            start = datetime(2026, 4, 2, 8, 30, tzinfo=chicago)
            for idx in range(18):
                timestamp = start + pd.Timedelta(minutes=5 * idx)
                price = 6500 + idx
                rows.append(
                    {
                        "Datetime": timestamp,
                        "Open": price,
                        "High": price + 2,
                        "Low": price - 2,
                        "Close": price + 1,
                        "Volume": 1000 + idx,
                    }
                )
            return pd.DataFrame(rows)
        raise MarketDataError("no data")

    def get_history_with_changes(self, ticker, start_date, end_date, query_type="history"):
        if self.history_error:
            raise MarketDataError(self.history_error)
        rows = []
        start = date(2026, 2, 20)
        for index in range(40):
            close_value = 4200 - index * 12
            rows.append(
                {
                    "Date": start + pd.Timedelta(days=index),
                    "Open": close_value + 4,
                    "High": close_value + 8,
                    "Low": close_value - 8,
                    "Close": close_value,
                }
            )
        return pd.DataFrame(rows)


class ApolloStructureFallbackTests(unittest.TestCase):
    def test_falls_back_to_prior_trading_day_when_requested_day_closed(self) -> None:
        service = _FakeMarketDataService()
        config = AppConfig(app_timezone="America/Chicago")
        structure_service = ApolloStructureService(market_data_service=service, config=config)

        fake_now = datetime(2026, 4, 3, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        with patch("services.apollo_structure_service.datetime") as mock_datetime:
            mock_datetime.now.return_value = fake_now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            result = structure_service.analyze_same_day_spx_structure()

        self.assertTrue(result["available"])
        self.assertEqual(service.calls[:2], [date(2026, 4, 3), date(2026, 4, 2)])
        self.assertEqual(result["session_date_used"], date(2026, 4, 2))
        self.assertIn("Using prior trading session", result["session_note"])
        self.assertNotEqual(result["grade"], "Not available")

    def test_daily_rsi_oversold_upgrades_base_structure_by_one_level(self) -> None:
        service = _FakeMarketDataService()
        config = AppConfig(app_timezone="America/Chicago")
        structure_service = ApolloStructureService(market_data_service=service, config=config)

        fake_now = datetime(2026, 4, 2, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        patched_classification = {
            "grade": "Poor",
            "summary": "Structure graded Poor: bearish continuation remained in force.",
            "trend_classification": "Bearish",
            "damage_classification": "Breakdown",
            "rules": [],
        }

        with patch("services.apollo_structure_service.datetime") as mock_datetime, patch.object(
            structure_service,
            "_classify_structure",
            return_value=patched_classification,
        ):
            mock_datetime.now.return_value = fake_now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            result = structure_service.analyze_same_day_spx_structure()

        self.assertEqual(result["base_grade"], "Poor")
        self.assertEqual(result["grade"], "Neutral")
        self.assertEqual(result["final_grade"], "Neutral")
        self.assertEqual(result["rsi_modifier_label"], "Oversold Boost")
        self.assertTrue(result["rsi_modifier_applied"])
        self.assertLess(result["rsi_value"], 30.0)

    def test_daily_rsi_unavailable_keeps_base_structure_unchanged(self) -> None:
        service = _FakeMarketDataService()
        service.history_error = "daily history unavailable"
        config = AppConfig(app_timezone="America/Chicago")
        structure_service = ApolloStructureService(market_data_service=service, config=config)

        fake_now = datetime(2026, 4, 2, 10, 0, tzinfo=ZoneInfo("America/Chicago"))
        with patch("services.apollo_structure_service.datetime") as mock_datetime:
            mock_datetime.now.return_value = fake_now
            mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            result = structure_service.analyze_same_day_spx_structure()

        self.assertEqual(result["base_grade"], result["grade"])
        self.assertEqual(result["rsi_modifier_label"], "None")
        self.assertFalse(result["rsi_modifier_applied"])
        self.assertIsNone(result["rsi_value"])

    def test_apply_rsi_modifier_is_one_way_only(self) -> None:
        self.assertEqual(ApolloStructureService.apply_rsi_modifier("Poor", 25.0), "Neutral")
        self.assertEqual(ApolloStructureService.apply_rsi_modifier("Neutral", 25.0), "Good")
        self.assertEqual(ApolloStructureService.apply_rsi_modifier("Good", 25.0), "Good")
        self.assertEqual(ApolloStructureService.apply_rsi_modifier("Good", 82.0), "Good")
        self.assertEqual(ApolloStructureService.apply_rsi_modifier("Poor", 82.0), "Poor")


if __name__ == "__main__":
    unittest.main()
