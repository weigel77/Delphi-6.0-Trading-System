from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from config import AppConfig
from services.provider_factory import ProviderFactory
from services.providers.schwab_provider import SchwabProvider


class _StubAuthService:
    def is_authenticated(self) -> bool:
        return True

    def get_valid_access_token(self) -> str:
        return "stub-token"


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self) -> dict:
        return self._payload


class SchwabHistoricalSupportTests(unittest.TestCase):
    def test_spx_historical_defaults_to_schwab_when_live_provider_is_schwab(self) -> None:
        config = AppConfig(market_data_provider="schwab")

        providers = ProviderFactory.create_historical_providers(config)

        self.assertEqual(providers["^GSPC"].get_metadata()["provider_key"], "schwab")

    def test_explicit_schwab_historical_provider_is_supported(self) -> None:
        config = AppConfig(market_data_provider="yahoo", spx_historical_provider="schwab")

        providers = ProviderFactory.create_historical_providers(config)

        self.assertEqual(providers["^GSPC"].get_metadata()["provider_key"], "schwab")

    @patch("services.providers.schwab_provider.requests.get")
    def test_historical_range_uses_period_params_and_retries_other_candidates_after_empty_response(self, mock_get) -> None:
        mock_get.side_effect = [
            _FakeResponse(200, {"symbol": "SPX", "empty": True, "candles": []}),
            _FakeResponse(
                200,
                {
                    "symbol": "$SPX",
                    "empty": False,
                    "candles": [
                        {
                            "datetime": 1740981600000,
                            "open": 5968.33,
                            "high": 5986.09,
                            "low": 5810.91,
                            "close": 5849.72,
                        }
                    ],
                },
            ),
        ]
        provider = SchwabProvider(
            config=AppConfig(
                market_data_provider="schwab",
                schwab_history_period_type="year",
                schwab_history_period="20",
                schwab_history_frequency_type="daily",
                schwab_history_frequency="1",
            ),
            auth_service=_StubAuthService(),
        )

        frame = provider.get_historical_range("^GSPC", date(2025, 3, 3), date(2025, 3, 3))

        self.assertEqual(len(frame.index), 1)
        self.assertEqual(frame.iloc[0]["Date"].isoformat(), "2025-03-03")
        self.assertEqual(mock_get.call_count, 2)

        first_request_params = mock_get.call_args_list[0].kwargs["params"]
        second_request_params = mock_get.call_args_list[1].kwargs["params"]
        self.assertEqual(first_request_params["symbol"], "$SPX")
        self.assertEqual(second_request_params["symbol"], "SPX")
        self.assertEqual(first_request_params["periodType"], "year")
        self.assertEqual(first_request_params["period"], "20")
        self.assertEqual(first_request_params["frequencyType"], "daily")
        self.assertEqual(first_request_params["frequency"], "1")


if __name__ == "__main__":
    unittest.main()