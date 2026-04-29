from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from config import AppConfig
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


def _epoch_millis(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000)


class _FrozenSundayDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = cls(2026, 4, 19, 10, 0, tzinfo=tz)
        return current if tz is not None else current.replace(tzinfo=None)


class _FrozenMondayPreOpenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        current = cls(2026, 4, 20, 8, 0, tzinfo=tz)
        return current if tz is not None else current.replace(tzinfo=None)


class SchwabIntradaySupportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = SchwabProvider(
            config=AppConfig(market_data_provider="schwab", schwab_base_url="https://api.schwabapi.com/marketdata/v1"),
            auth_service=_StubAuthService(),
        )

    @patch("services.providers.schwab_provider.requests.get")
    def test_same_day_intraday_request_clamps_weekend_to_previous_trading_session_and_logs_caller(self, mock_get) -> None:
        mock_get.return_value = _FakeResponse(
            200,
            {
                "symbol": "$SPX",
                "empty": False,
                "candles": [
                    {
                        "datetime": _epoch_millis(2026, 4, 17, 15, 0),
                        "open": 5300.0,
                        "high": 5310.0,
                        "low": 5290.0,
                        "close": 5305.0,
                        "volume": 1000,
                    }
                ],
            },
        )

        with patch("services.providers.schwab_provider.datetime", _FrozenSundayDateTime):
            with self.assertLogs("services.providers.schwab_provider", level="WARNING") as captured:
                def invoke_lookup() -> None:
                    frame = self.provider.get_same_day_intraday_candles("^GSPC", interval_minutes=5)
                    self.assertEqual(len(frame.index), 1)

                invoke_lookup()

        params = mock_get.call_args.kwargs["params"]
        self.assertGreaterEqual(params["endDate"], params["startDate"])
        combined_logs = "\n".join(captured.output)
        self.assertIn("requested_session_date=2026-04-19", combined_logs)
        self.assertIn("resolved_session_date=2026-04-17", combined_logs)
        self.assertIn("invoke_lookup", combined_logs)

    def test_pre_open_same_day_session_clamps_to_previous_trading_day(self) -> None:
        with patch("services.providers.schwab_provider.datetime", _FrozenMondayPreOpenDateTime):
            with self.assertLogs("services.providers.schwab_provider", level="WARNING") as captured:
                session_window = self.provider._get_session_window_for_date(
                    date(2026, 4, 20),
                    caller_context={"caller": "tests:test_pre_open_same_day_session", "chain": "tests:test_pre_open_same_day_session"},
                )

        self.assertEqual(session_window["requested_session_date"], date(2026, 4, 20))
        self.assertEqual(session_window["session_date"], date(2026, 4, 17))
        self.assertGreaterEqual(session_window["end_utc"], session_window["start_utc"])
        combined_logs = "\n".join(captured.output)
        self.assertIn("requested_session_date=2026-04-20", combined_logs)
        self.assertIn("resolved_session_date=2026-04-17", combined_logs)


if __name__ == "__main__":
    unittest.main()