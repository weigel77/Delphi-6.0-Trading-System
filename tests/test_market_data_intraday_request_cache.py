from __future__ import annotations

import unittest
from datetime import date

import pandas as pd
from flask import Flask

from config import AppConfig
from services.market_data import MarketDataService


class _FakeIntradayProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, date, int]] = []

    def get_metadata(self):
        return {"provider_key": "fake", "provider_name": "Fake Provider"}

    def get_intraday_candles_for_date(self, ticker: str, target_date: date, interval_minutes: int = 5):
        self.calls.append((ticker, target_date, interval_minutes))
        return pd.DataFrame(
            [
                {
                    "Datetime": pd.Timestamp("2026-04-17 08:30:00", tz="America/Chicago"),
                    "Open": 1.0,
                    "High": 2.0,
                    "Low": 0.5,
                    "Close": 1.5,
                    "Volume": 100,
                }
            ]
        )


class MarketDataIntradayRequestCacheTests(unittest.TestCase):
    def test_request_scope_reuses_same_intraday_session_across_query_types(self) -> None:
        provider = _FakeIntradayProvider()
        service = MarketDataService(config=AppConfig(), provider=provider, historical_providers={"^GSPC": provider, "^VIX": provider})
        app = Flask(__name__)

        with app.test_request_context("/hosted/mobile"):
            first = service.get_intraday_candles_for_date("^GSPC", date(2026, 4, 17), interval_minutes=1, query_type="kairos_latest_state")
            second = service.get_intraday_candles_for_date(
                "^GSPC",
                date(2026, 4, 17),
                interval_minutes=1,
                query_type="open_trade_management_kairos_intraday",
            )

        self.assertEqual(len(provider.calls), 1)
        self.assertEqual(len(first.index), 1)
        self.assertEqual(len(second.index), 1)


if __name__ == "__main__":
    unittest.main()