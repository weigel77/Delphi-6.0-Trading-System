"""Apollo option-chain retrieval and summarization services."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from config import AppConfig, get_app_config

from .provider_factory import ProviderFactory
from .runtime.auth_composition import AuthComposer, LocalAuthComposer
from .runtime.provider_composition import LocalProviderComposer, ProviderComposer
from .repositories.token_repository import JsonFileTokenRepository
from .schwab_auth_service import SchwabAuthService
from .market_calendar_service import MarketCalendarService
from .providers.base_provider import ProviderError
from .providers.schwab_provider import SchwabProvider


class OptionsChainService:
    """Retrieve and summarize SPX option-chain data for Apollo."""

    PREVIEW_WIDTHS = (5.0, 10.0, 15.0, 20.0, 25.0, 30.0)
    MIN_PREVIEW_NET_CREDIT = 1.0

    def __init__(
        self,
        config: AppConfig | None = None,
        provider: Any | None = None,
        provider_composer: ProviderComposer | None = None,
        auth_composer: AuthComposer | None = None,
    ) -> None:
        self.config = config or get_app_config()
        self.provider = provider
        self.auth_composer = auth_composer or LocalAuthComposer(self.config)
        self.provider_composer = provider_composer or LocalProviderComposer(self.config, self.auth_composer)
        self.market_calendar_service = MarketCalendarService(self.config)

    def get_spx_option_chain_summary(self, expiration_date: date) -> Dict[str, Any]:
        """Return a compact normalized next-market-day SPX option-chain summary."""
        provider = None
        try:
            if not self.market_calendar_service.is_tradable_market_day(expiration_date):
                holiday_name = self.market_calendar_service.get_holiday_name(expiration_date) or "Weekend"
                return {
                    "success": False,
                    "source_name": "Schwab",
                    "failure_category": "exchange-closed",
                    "failure_label": "Exchange closed",
                    "failure_status_class": "neutral",
                    "symbol_requested": self.config.schwab_spx_option_chain_symbol,
                    "expiration_target": expiration_date,
                    "expiration_date": expiration_date,
                    "expiration_count": 0,
                    "underlying_price": None,
                    "puts_count": 0,
                    "calls_count": 0,
                    "rows_displayed": 0,
                    "strike_range": "—",
                    "puts": [],
                    "calls": [],
                    "preview_rows": [],
                    "request_diagnostics": {
                        "final_symbol": self.config.schwab_spx_option_chain_symbol,
                        "final_expiration": expiration_date.isoformat(),
                        "attempt_used": "Skipped",
                        "raw_params_sent": {},
                        "error_detail": f"Exchange closed ({holiday_name})",
                        "failure_category": "exchange-closed",
                        "failure_label": "Exchange closed",
                        "attempts": [],
                    },
                    "message": f"Skipped SPX option-chain request because {expiration_date.isoformat()} is exchange-closed ({holiday_name}).",
                }

            provider = self._resolve_provider()
            chain = provider.get_option_chain("^GSPC", target_date=expiration_date)
            request_diagnostics = chain.get("request_diagnostics") or getattr(provider, "last_option_chain_diagnostics", {})
            calls = list(chain.get("calls") or [])
            puts = list(chain.get("puts") or [])
            all_options = calls + puts
            strikes = [self._coerce_float(option.get("strike")) for option in all_options]
            strikes = [value for value in strikes if value is not None]
            preview_rows = self._build_preview_rows(
                calls=calls,
                puts=puts,
                underlying_price=self._coerce_float(chain.get("underlying_price")),
            )

            if not calls and not puts:
                return {
                    "success": False,
                    "source_name": getattr(provider, "provider_name", "Unknown Provider"),
                    "failure_category": "empty-response",
                    "failure_label": "Empty response",
                    "failure_status_class": "not-available",
                    "symbol_requested": chain.get("requested_symbol", self.config.schwab_spx_option_chain_symbol),
                    "expiration_target": chain.get("expiration_target") or expiration_date,
                    "expiration_date": expiration_date,
                    "expiration_count": chain.get("expiration_count", 0),
                    "underlying_price": chain.get("underlying_price"),
                    "puts_count": 0,
                    "calls_count": 0,
                    "rows_displayed": 0,
                    "strike_range": "—",
                    "puts": [],
                    "calls": [],
                    "preview_rows": [],
                    "request_diagnostics": request_diagnostics,
                    "message": "Schwab returned a response, but no usable SPX option contracts were available for the requested expiration.",
                }

            strike_range = (
                f"{min(strikes):,.0f} to {max(strikes):,.0f}"
                if strikes
                else "—"
            )
            return {
                "success": True,
                "source_name": getattr(provider, "provider_name", "Unknown Provider"),
                "failure_category": "",
                "failure_label": "",
                "failure_status_class": "good",
                "symbol_requested": chain.get("requested_symbol", self.config.schwab_spx_option_chain_symbol),
                "expiration_target": chain.get("expiration_target") or expiration_date,
                "expiration_date": chain.get("expiration_date") or expiration_date,
                "expiration_count": chain.get("expiration_count", 0),
                "underlying_price": chain.get("underlying_price"),
                "puts_count": len(puts),
                "calls_count": len(calls),
                "rows_displayed": len(preview_rows),
                "strike_range": strike_range,
                "puts": puts,
                "calls": calls,
                "preview_rows": preview_rows,
                "request_diagnostics": request_diagnostics,
                "message": "SPX option chain retrieved successfully.",
            }
        except Exception as exc:
            request_diagnostics = getattr(provider, "last_option_chain_diagnostics", {}) if provider is not None else {}
            failure_category, failure_label, failure_status_class = self._resolve_failure_metadata(
                request_diagnostics=request_diagnostics,
                message=str(exc),
            )
            return {
                "success": False,
                "source_name": "Schwab",
                "failure_category": failure_category,
                "failure_label": failure_label,
                "failure_status_class": failure_status_class,
                "symbol_requested": self.config.schwab_spx_option_chain_symbol,
                "expiration_target": expiration_date,
                "expiration_date": expiration_date,
                "expiration_count": 0,
                "underlying_price": None,
                "puts_count": 0,
                "calls_count": 0,
                "rows_displayed": 0,
                "strike_range": "—",
                "puts": [],
                "calls": [],
                "preview_rows": [],
                "request_diagnostics": request_diagnostics,
                "message": str(exc),
            }

    @staticmethod
    def _resolve_failure_metadata(
        *,
        request_diagnostics: Dict[str, Any],
        message: str,
    ) -> tuple[str, str, str]:
        """Return a normalized Apollo-friendly failure category."""
        failure_category = str(request_diagnostics.get("failure_category") or "").strip().lower()
        failure_label = str(request_diagnostics.get("failure_label") or "").strip()

        if not failure_category:
            normalized_message = message.lower()
            if any(code in normalized_message for code in ("503", "502", "504", "429")):
                failure_category = "upstream-unavailable"
            elif any(code in normalized_message for code in ("400", "404", "422")):
                failure_category = "malformed-request"
            else:
                failure_category = "unknown-error"

        if not failure_label:
            failure_label = {
                "upstream-unavailable": "Upstream unavailable",
                "malformed-request": "Malformed request",
                "exchange-closed": "Exchange closed",
                "empty-response": "Empty response",
            }.get(failure_category, "Unavailable")

        failure_status_class = "poor" if failure_category == "malformed-request" else "not-available"
        if failure_category == "exchange-closed":
            failure_status_class = "neutral"
        return failure_category, failure_label, failure_status_class

    def _resolve_provider(self) -> Any:
        if self.provider is not None:
            return self.provider

        if self.config.apollo_option_chain_source != "schwab":
            raise ProviderError(
                f"Unsupported Apollo option-chain source '{self.config.apollo_option_chain_source}'. Use 'schwab'."
            )

        provider = ProviderFactory.create_live_provider(self.config, provider_composer=self.provider_composer)
        if isinstance(provider, SchwabProvider):
            return provider

        auth_service = self.auth_composer.create_schwab_auth_service()
        return SchwabProvider(config=self.config, display_timezone=self.config.app_timezone, auth_service=auth_service)

    def _build_preview_rows(
        self,
        calls: List[Dict[str, Any]],
        puts: List[Dict[str, Any]],
        underlying_price: float | None,
    ) -> List[Dict[str, Any]]:
        if not calls and not puts:
            return []

        rows = self._build_credit_spread_preview_rows(side="PUT", contracts=puts, underlying_price=underlying_price)
        rows.extend(self._build_credit_spread_preview_rows(side="CALL", contracts=calls, underlying_price=underlying_price))
        return rows

    def _build_credit_spread_preview_rows(
        self,
        side: str,
        contracts: List[Dict[str, Any]],
        underlying_price: float | None,
    ) -> List[Dict[str, Any]]:
        if not contracts:
            return []

        normalized = sorted(
            [
                {
                    "strike": self._coerce_float(item.get("strike")),
                    "bid": self._coerce_float(item.get("bid")) or 0.0,
                    "ask": self._coerce_float(item.get("ask")) or 0.0,
                    "last": self._coerce_float(item.get("last")) or 0.0,
                    "delta": self._coerce_float(item.get("delta")),
                }
                for item in contracts
                if self._coerce_float(item.get("strike")) is not None
            ],
            key=lambda item: item["strike"],
        )
        if not normalized:
            return []

        strike_lookup = {round(item["strike"], 2): item for item in normalized}
        preview_rows: List[Dict[str, Any]] = []

        for short_leg in normalized:
            if underlying_price is not None:
                if side == "PUT" and short_leg["strike"] >= underlying_price:
                    continue
                if side == "CALL" and short_leg["strike"] <= underlying_price:
                    continue

            for width in self.PREVIEW_WIDTHS:
                long_strike = short_leg["strike"] - width if side == "PUT" else short_leg["strike"] + width
                long_leg = strike_lookup.get(round(long_strike, 2))
                if long_leg is None:
                    continue

                conservative_credit = short_leg["bid"] - long_leg["ask"]
                mid_credit = self._premium_anchor(short_leg) - self._premium_anchor(long_leg)
                net_credit = max(conservative_credit, mid_credit)
                if net_credit <= self.MIN_PREVIEW_NET_CREDIT:
                    continue

                distance_points = None if underlying_price is None else abs(underlying_price - short_leg["strike"])
                preview_rows.append(
                    {
                        "side": side,
                        "short_strike": self._format_numeric(short_leg["strike"]),
                        "long_strike": self._format_numeric(long_leg["strike"]),
                        "width": self._format_numeric(width),
                        "net_credit": self._format_numeric(net_credit),
                        "short_delta": self._format_numeric(abs(short_leg["delta"])) if short_leg["delta"] is not None else "—",
                        "distance": self._format_numeric(distance_points) if distance_points is not None else "—",
                    }
                )

        preview_rows.sort(
            key=lambda item: (
                0 if item["side"] == "PUT" else 1,
                float(str(item["distance"]).replace(",", "")) if item["distance"] not in {"—", None, ""} else 0.0,
                float(str(item["short_strike"]).replace(",", "")),
                float(str(item["width"]).replace(",", "")),
            )
        )
        return preview_rows

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _format_numeric(self, value: Any) -> str:
        numeric = self._coerce_float(value)
        if numeric is None:
            return "—"
        return f"{numeric:,.2f}"

    @staticmethod
    def _premium_anchor(contract: Dict[str, Any]) -> float:
        bid = float(contract.get("bid") or 0.0)
        ask = float(contract.get("ask") or 0.0)
        last = float(contract.get("last") or 0.0)
        if bid > 0 and ask > 0:
            return (bid + ask) / 2.0
        return max(last, bid, ask)
