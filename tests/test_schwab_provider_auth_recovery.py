import unittest
from unittest.mock import patch

from app import build_startup_menu_payload
from config import AppConfig
from services.market_data import MarketDataAuthenticationError
from services.providers.schwab_provider import SchwabProvider


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


class _FakeAuthService:
    def __init__(self):
        self.recover_calls = 0
        self.clear_calls = 0

    def get_valid_access_token(self):
        return "stale-token"

    def recover_from_unauthorized_response(self):
        self.recover_calls += 1
        return "fresh-token"

    def clear_tokens(self):
        self.clear_calls += 1

    def is_authenticated(self):
        return True


class _AuthBlockedMarketDataService:
    def get_provider_metadata(self):
        return {
            "provider_name": "Schwab",
            "requires_auth": True,
            "authenticated": True,
            "live_provider_name": "Schwab",
        }

    def get_latest_snapshot(self, ticker, query_type="latest"):
        raise MarketDataAuthenticationError("Click login to connect to Schwab")


class SchwabProviderAuthRecoveryTest(unittest.TestCase):
    def test_quote_request_refreshes_token_after_401_and_retries(self):
        provider = SchwabProvider(
            AppConfig(schwab_base_url="https://api.schwabapi.com/marketdata/v1"),
            auth_service=_FakeAuthService(),
        )
        observed_headers = []

        def fake_get(url, params=None, headers=None, timeout=30):
            observed_headers.append(dict(headers or {}))
            if len(observed_headers) == 1:
                return _FakeResponse(401, {})
            return _FakeResponse(
                200,
                {
                    "$SPX": {
                        "quote": {
                            "lastPrice": 6123.45,
                            "closePrice": 6100.0,
                            "quoteTime": 1713193200000,
                        }
                    }
                },
            )

        with patch("services.providers.schwab_provider.requests.get", side_effect=fake_get):
            snapshot = provider.get_latest_snapshot("^GSPC")

        self.assertEqual(snapshot["Latest Value"], 6123.45)
        self.assertEqual(provider.auth_service.recover_calls, 1)
        self.assertEqual(observed_headers[0]["Authorization"], "Bearer stale-token")
        self.assertEqual(observed_headers[1]["Authorization"], "Bearer fresh-token")

    def test_startup_menu_marks_connection_login_required_when_quotes_are_auth_blocked(self):
        payload = build_startup_menu_payload(_AuthBlockedMarketDataService())

        self.assertEqual(payload["connection_label"], "Login required")
        self.assertTrue(payload["connection_requires_login"])
        self.assertEqual(payload["cards"][2]["value"], "Login required")
