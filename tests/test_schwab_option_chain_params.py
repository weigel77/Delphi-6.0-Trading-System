from __future__ import annotations

import unittest
from datetime import date

from config import AppConfig
from services.providers.schwab_provider import SchwabProvider


class _DummyAuthService:
    def get_valid_access_token(self) -> str:
        return "token"

    def is_authenticated(self) -> bool:
        return True


class _FakeResponse:
    def __init__(self, status_code: int, payload=None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class SchwabOptionChainParamTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = SchwabProvider(config=AppConfig(), auth_service=_DummyAuthService())

    def test_build_schwab_option_chain_params_uses_minimal_iso_request(self) -> None:
        params = self.provider.build_schwab_option_chain_params("^GSPC", date(2026, 4, 6))

        self.assertEqual(
            params,
            {
                "symbol": "$SPX",
                "contractType": "PUT",
                "fromDate": "2026-04-06",
                "toDate": "2026-04-06",
            },
        )

    def test_build_schwab_option_chain_params_removes_empty_values(self) -> None:
        params = self.provider._clean_option_chain_params(
            {
                "symbol": "$SPX",
                "contractType": "PUT",
                "fromDate": "2026-04-06",
                "toDate": "2026-04-06",
                "strikeCount": None,
                "includeUnderlyingQuote": "",
                "extra": [],
            }
        )

        self.assertEqual(
            params,
            {
                "symbol": "$SPX",
                "contractType": "PUT",
                "fromDate": "2026-04-06",
                "toDate": "2026-04-06",
            },
        )

    def test_option_chain_attempt_ladder_is_minimal_first(self) -> None:
        attempts = self.provider._build_option_chain_attempts("^GSPC", date(2026, 4, 6))

        self.assertEqual([label for label, _ in attempts], ["Attempt A", "Attempt B", "Attempt C"])
        self.assertEqual(
            attempts[0][1],
            {
                "symbol": "$SPX",
                "contractType": "PUT",
                "fromDate": "2026-04-06",
                "toDate": "2026-04-06",
            },
        )
        self.assertEqual(
            attempts[1][1],
            {
                "symbol": "$SPX",
                "fromDate": "2026-04-06",
                "toDate": "2026-04-06",
            },
        )
        self.assertEqual(attempts[2][1], {"symbol": "$SPX"})

    def test_option_chain_failure_detail_marks_503_as_upstream_unavailable(self) -> None:
        detail = self.provider._build_option_chain_failure_detail(
            _FakeResponse(503, payload={"message": "Service unavailable"}, text="Service unavailable")
        )

        self.assertEqual(detail["failure_category"], "upstream-unavailable")
        self.assertEqual(detail["failure_label"], "Upstream unavailable")
        self.assertEqual(detail["error_detail"], "Service unavailable")

    def test_option_chain_failure_detail_marks_400_as_malformed_request(self) -> None:
        detail = self.provider._build_option_chain_failure_detail(
            _FakeResponse(400, payload={"error": "Invalid fromDate"}, text="Invalid fromDate")
        )

        self.assertEqual(detail["failure_category"], "malformed-request")
        self.assertEqual(detail["failure_label"], "Malformed request")
        self.assertEqual(detail["error_detail"], "Invalid fromDate")


if __name__ == "__main__":
    unittest.main()