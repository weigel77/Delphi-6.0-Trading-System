from __future__ import annotations

import unittest
from datetime import date

from app import build_apollo_result_payload
from config import AppConfig
from services.options_chain_service import OptionsChainService
from services.providers.base_provider import ProviderError


class _AlwaysOpenCalendar:
    def is_tradable_market_day(self, target_date: date) -> bool:
        return True

    def get_holiday_name(self, target_date: date) -> str | None:
        return None


class _RaisingProvider:
    provider_name = "Schwab"

    def __init__(self, diagnostics: dict, error: Exception) -> None:
        self.last_option_chain_diagnostics = diagnostics
        self.error = error

    def get_option_chain(self, symbol: str, target_date: date | None = None):
        raise self.error


class _StaticProvider:
    provider_name = "Schwab"

    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.last_option_chain_diagnostics = payload.get("request_diagnostics", {})

    def get_option_chain(self, symbol: str, target_date: date | None = None):
        return self.payload


class ApolloReliabilityTests(unittest.TestCase):
    def _build_service(self, provider) -> OptionsChainService:
        service = OptionsChainService(config=AppConfig(), provider=provider)
        service.market_calendar_service = _AlwaysOpenCalendar()
        return service

    def test_option_chain_service_classifies_503_as_upstream_unavailable(self) -> None:
        provider = _RaisingProvider(
            diagnostics={
                "final_symbol": "$SPX",
                "final_expiration": "2026-04-06",
                "failure_category": "upstream-unavailable",
                "failure_label": "Upstream unavailable",
                "error_detail": "Service unavailable",
                "attempts": [],
            },
            error=ProviderError("Schwab rejected the option-chain request: Service unavailable", is_transient=True),
        )
        summary = self._build_service(provider).get_spx_option_chain_summary(date(2026, 4, 6))

        self.assertFalse(summary["success"])
        self.assertEqual(summary["failure_category"], "upstream-unavailable")
        self.assertEqual(summary["failure_label"], "Upstream unavailable")

    def test_option_chain_service_classifies_400_as_malformed_request(self) -> None:
        provider = _RaisingProvider(
            diagnostics={
                "final_symbol": "$SPX",
                "final_expiration": "2026-04-06",
                "failure_category": "malformed-request",
                "failure_label": "Malformed request",
                "error_detail": "Invalid fromDate",
                "attempts": [],
            },
            error=ProviderError("Schwab rejected the option-chain request: Invalid fromDate"),
        )
        summary = self._build_service(provider).get_spx_option_chain_summary(date(2026, 4, 6))

        self.assertFalse(summary["success"])
        self.assertEqual(summary["failure_category"], "malformed-request")
        self.assertEqual(summary["failure_label"], "Malformed request")
        self.assertEqual(summary["failure_status_class"], "poor")

    def test_option_chain_service_classifies_empty_response(self) -> None:
        provider = _StaticProvider(
            {
                "requested_symbol": "$SPX",
                "expiration_target": date(2026, 4, 6),
                "expiration_date": date(2026, 4, 6),
                "expiration_count": 1,
                "underlying_price": 6500.0,
                "calls": [],
                "puts": [],
                "request_diagnostics": {
                    "final_symbol": "$SPX",
                    "final_expiration": "2026-04-06",
                    "attempt_used": "Attempt A",
                    "raw_params_sent": {"symbol": "$SPX"},
                    "attempts": [],
                },
            }
        )
        summary = self._build_service(provider).get_spx_option_chain_summary(date(2026, 4, 6))

        self.assertFalse(summary["success"])
        self.assertEqual(summary["failure_category"], "empty-response")
        self.assertEqual(summary["failure_label"], "Empty response")

    def test_payload_distinguishes_no_candidates_from_option_chain_failure(self) -> None:
        payload = build_apollo_result_payload(
            {
                "title": "Apollo Gate 1 -- SPX Structure",
                "provider_name": "Schwab",
                "apollo_status": "allowed",
                "spx": {"value": 6500.0, "as_of": "Now"},
                "vix": {"value": 20.0, "as_of": "Now"},
                "macro": {"grade": "None", "available": True},
                "structure": {"grade": "Good", "available": True, "metrics": {}},
                "market_calendar": {"next_market_day": date(2026, 4, 6)},
                "option_chain": {
                    "success": True,
                    "source_name": "Schwab",
                    "symbol_requested": "$SPX",
                    "expiration_target": date(2026, 4, 6),
                    "expiration_date": date(2026, 4, 6),
                    "expiration_count": 1,
                    "puts_count": 12,
                    "calls_count": 12,
                    "rows_displayed": 0,
                    "strike_range": "6200 to 6800",
                    "preview_rows": [],
                    "request_diagnostics": {},
                    "message": "SPX option chain retrieved successfully.",
                },
                "trade_candidates": {
                    "title": "Apollo Gate 3 -- Trade Candidates",
                    "status": "Stand Aside",
                    "status_class": "poor",
                    "message": "No Gate 3 mode produced a valid SPX 1DTE trade for this expiration.",
                    "candidate_count": 0,
                    "valid_mode_count": 0,
                    "diagnostics": {},
                    "candidates": [],
                },
            },
            trigger_source="button",
        )

        self.assertEqual(payload["option_chain_status"], "Ready")
        self.assertEqual(payload["trade_candidates_outcome_category"], "no-candidates")
        self.assertEqual(payload["trade_candidates_outcome_label"], "No candidates")


if __name__ == "__main__":
    unittest.main()
