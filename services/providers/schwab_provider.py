"""Schwab provider implementation for OAuth-based quote and price-history access."""

from __future__ import annotations

import logging
import json
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
import requests

from config import AppConfig

from ..calculations import calculate_percent_change, calculate_point_change
from ..schwab_auth_service import SchwabAuthService
from .base_provider import BaseMarketDataProvider, ProviderAuthRequiredError, ProviderError, ProviderNotImplementedError

LOGGER = logging.getLogger(__name__)


class SchwabProvider(BaseMarketDataProvider):
    """Placeholder provider for a future Schwab API integration.

    This scaffold is intentionally not fully implemented yet. It exists so the
    codebase can switch from Yahoo Finance to Schwab once API approval, OAuth
    credentials, and token storage details are available.
    """

    provider_key = "schwab"
    provider_name = "Schwab"
    QUOTE_SYMBOL_MAP = {
        "^GSPC": "$SPX",
        "SPX": "$SPX",
        "/ES": "/ES",
        "ES": "/ES",
        "^VIX": "$VIX",
        "VIX": "$VIX",
    }
    HISTORICAL_SYMBOL_MAP = {
        "^GSPC": ("$SPX", "SPX", "$SPX.X"),
        "SPX": ("$SPX", "SPX", "$SPX.X"),
        "/ES": ("/ES", "ES"),
        "ES": ("/ES", "ES"),
        "^VIX": ("$VIX", "VIX"),
        "VIX": ("$VIX", "VIX"),
    }
    OPTION_CHAIN_SYMBOL_MAP = {
        "^GSPC": ("$SPX",),
        "SPX": ("$SPX",),
    }

    def __init__(
        self,
        config: AppConfig,
        display_timezone: str = "America/Chicago",
        auth_service: SchwabAuthService | None = None,
    ) -> None:
        super().__init__(display_timezone=display_timezone)
        self.config = config
        self.auth_service = auth_service or SchwabAuthService(config)
        self.last_option_chain_diagnostics: Dict[str, Any] = {}

    def get_metadata(self) -> Dict[str, Any]:
        """Return provider metadata including Schwab auth state."""
        metadata = super().get_metadata()
        metadata["requires_auth"] = True
        metadata["authenticated"] = self.auth_service.is_authenticated()
        return metadata

    def get_latest_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Fetch the latest quote snapshot from the Schwab quotes endpoint."""
        schwab_symbol = self._map_quote_symbol(symbol)
        access_token = self.auth_service.get_valid_access_token()
        response = requests.get(
            f"{self.config.schwab_base_url}/quotes",
            params={"symbols": schwab_symbol},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )

        if response.status_code == 401:
            raise ProviderAuthRequiredError("Click login to connect to Schwab")
        if response.status_code >= 400:
            raise ProviderError(f"Unable to retrieve Schwab quotes right now ({response.status_code}).")

        payload = response.json()
        quote_container = self._extract_quote_container(payload, schwab_symbol)
        quote_data = quote_container.get("quote", quote_container)

        last_price = self._coerce_float(
            quote_data.get("lastPrice")
            or quote_data.get("last")
            or quote_container.get("lastPrice")
            or quote_container.get("last")
        )
        close_price = self._coerce_float(
            quote_data.get("closePrice")
            or quote_data.get("previousClose")
            or quote_container.get("closePrice")
            or quote_container.get("previousClose")
        )
        open_price = self._coerce_float(quote_data.get("openPrice") or quote_container.get("openPrice") or last_price)
        high_price = self._coerce_float(quote_data.get("highPrice") or quote_container.get("highPrice") or last_price)
        low_price = self._coerce_float(quote_data.get("lowPrice") or quote_container.get("lowPrice") or last_price)
        market_timestamp = self._parse_timestamp(
            quote_data.get("quoteTime")
            or quote_data.get("tradeTime")
            or quote_container.get("quoteTime")
            or quote_container.get("tradeTime")
        )

        if last_price is None:
            raise ProviderError(f"Schwab returned no latest price for {symbol}.")

        point_change = self._coerce_float(
            quote_data.get("netChange") or quote_container.get("netChange")
        )
        if point_change is None and close_price is not None:
            point_change = calculate_point_change(last_price, close_price)

        percent_change = self._coerce_float(
            quote_data.get("netPercentChangeInDouble")
            or quote_data.get("percentChange")
            or quote_container.get("netPercentChangeInDouble")
            or quote_container.get("percentChange")
        )
        if percent_change is None and close_price is not None:
            percent_change = calculate_percent_change(last_price, close_price)

        market_datetime = market_timestamp or datetime.now(self.display_timezone)
        return {
            "Ticker": symbol,
            "Market Date": market_datetime.date(),
            "Latest Value": round(last_price, 2),
            "Open": round(open_price if open_price is not None else last_price, 2),
            "High": round(high_price if high_price is not None else last_price, 2),
            "Low": round(low_price if low_price is not None else last_price, 2),
            "Close": round(last_price, 2),
            "Prior Close": round(close_price, 2) if close_price is not None else None,
            "Daily Point Change": round(point_change, 2) if point_change is not None else None,
            "Daily Percent Change": round(percent_change, 2) if percent_change is not None else None,
            "As Of": market_datetime.strftime("%Y-%m-%d %I:%M:%S %p %Z"),
        }

    def get_historical_range(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch historical daily price data from the Schwab price-history endpoint."""
        access_token = self.auth_service.get_valid_access_token()
        endpoint = f"{self.config.schwab_base_url}/pricehistory"

        last_failure: ProviderError | None = None
        for schwab_symbol in self._get_historical_symbol_candidates(symbol):
            params = self._build_historical_params(schwab_symbol, start_date, end_date)
            LOGGER.info(
                "Schwab historical request | symbol=%s | requested_start=%s | requested_end=%s | endpoint=%s | params=%s",
                schwab_symbol,
                start_date.isoformat(),
                end_date.isoformat(),
                endpoint,
                params,
            )

            response = requests.get(
                endpoint,
                params=params,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30,
            )

            if response.status_code == 401:
                raise ProviderAuthRequiredError("Click login to connect to Schwab")

            if response.status_code >= 400:
                safe_message = self._extract_safe_error_message(response)
                LOGGER.warning(
                    "Schwab historical request failed | symbol=%s | requested_start=%s | requested_end=%s | endpoint=%s | params=%s | status=%s | body=%s",
                    schwab_symbol,
                    start_date.isoformat(),
                    end_date.isoformat(),
                    endpoint,
                    params,
                    response.status_code,
                    self._safe_response_text(response),
                )
                last_failure = ProviderError(
                    f"Schwab rejected the historical request: {safe_message}",
                    is_transient=response.status_code >= 500,
                )
                continue

            payload = response.json()
            candles = payload.get("candles", [])
            if payload.get("empty") is True or not candles:
                last_failure = ProviderError(
                    f"Schwab returned no historical price data for {symbol} between {start_date.isoformat()} and {end_date.isoformat()}."
                )
                continue

            frame = pd.DataFrame([self._normalize_candle(candle) for candle in candles])
            if frame.empty:
                last_failure = ProviderError(
                    f"Schwab returned no usable historical price data for {symbol} between {start_date.isoformat()} and {end_date.isoformat()}."
                )
                continue

            frame = frame.sort_values("Date").reset_index(drop=True)
            filtered = frame.loc[(frame["Date"] >= start_date) & (frame["Date"] <= end_date)].copy()
            if filtered.empty:
                last_failure = ProviderError(
                    f"No trading data exists for {symbol} between {start_date.isoformat()} and {end_date.isoformat()}."
                )
                continue

            return filtered

        if last_failure is not None:
            raise last_failure

        raise ProviderError("Schwab historical lookup failed before a request could be completed.")

    def get_single_date(self, symbol: str, target_date: date) -> Dict[str, Any]:
        """Retrieve a single historical daily bar through the Schwab history API."""
        history_frame = self.get_historical_range(symbol, target_date, target_date)
        matching_rows = history_frame.loc[history_frame["Date"] == target_date]
        if matching_rows.empty:
            raise ProviderError(
                f"No trading data exists for {symbol} on {target_date.isoformat()}. The market may have been closed."
            )
        return self._row_to_dict(matching_rows.iloc[0])

    def get_same_day_intraday_candles(self, symbol: str, interval_minutes: int = 5) -> pd.DataFrame:
        """Fetch same-day intraday candles for the exact requested SPX symbol."""
        return self.get_intraday_candles_for_date(symbol, target_date=self._get_same_day_session_window()["session_date"], interval_minutes=interval_minutes)

    def get_intraday_candles_for_date(self, symbol: str, target_date: date, interval_minutes: int = 5) -> pd.DataFrame:
        """Fetch intraday candles for the requested trading date."""
        access_token = self.auth_service.get_valid_access_token()
        endpoint = f"{self.config.schwab_base_url}/pricehistory"
        session_window = self._get_session_window_for_date(target_date)

        last_failure: ProviderError | None = None
        for schwab_symbol in self._get_intraday_symbol_candidates(symbol):
            params = self._build_intraday_params(
                schwab_symbol=schwab_symbol,
                interval_minutes=interval_minutes,
                start_at=session_window["start_utc"],
                end_at=session_window["end_utc"],
            )
            LOGGER.info(
                "Schwab intraday request | symbol=%s | endpoint=%s | params=%s",
                schwab_symbol,
                endpoint,
                params,
            )

            response = requests.get(
                endpoint,
                params=params,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30,
            )

            if response.status_code == 401:
                raise ProviderAuthRequiredError("Click login to connect to Schwab")

            if response.status_code >= 400:
                safe_message = self._extract_safe_error_message(response)
                LOGGER.warning(
                    "Schwab intraday request failed | symbol=%s | endpoint=%s | params=%s | status=%s | body=%s",
                    schwab_symbol,
                    endpoint,
                    params,
                    response.status_code,
                    self._safe_response_text(response),
                )
                last_failure = ProviderError(
                    f"Schwab rejected the same-day {interval_minutes}-minute request: {safe_message}",
                    is_transient=response.status_code >= 500,
                )
                continue

            payload = response.json()
            candles = payload.get("candles", [])
            if payload.get("empty") is True or not candles:
                last_failure = ProviderError(f"Schwab returned no same-day {interval_minutes}-minute candles for {symbol}.")
                continue

            frame = pd.DataFrame([self._normalize_intraday_candle(candle) for candle in candles])
            if frame.empty:
                last_failure = ProviderError(f"Schwab returned no usable same-day {interval_minutes}-minute candles for {symbol}.")
                continue

            frame = frame.sort_values("Datetime").reset_index(drop=True)
            frame = frame.loc[
                (frame["Datetime"].dt.date == session_window["session_date"])
                & (frame["Datetime"].dt.time >= time(8, 30))
                & (frame["Datetime"].dt.time <= time(15, 0))
            ].reset_index(drop=True)
            if frame.empty:
                last_failure = ProviderError(f"Schwab returned no regular-session same-day {interval_minutes}-minute candles for {symbol}.")
                continue
            return frame

        if last_failure is not None:
            raise last_failure
        raise ProviderError(f"Schwab same-day {interval_minutes}-minute lookup failed before a request could be completed.")

    def get_option_chain(self, symbol: str, target_date: date | None = None) -> Any:
        """Retrieve a Schwab option chain and normalize the requested expiration."""
        if target_date is None:
            raise ProviderError("Apollo option-chain retrieval requires an explicit target expiration date.")

        access_token = self.auth_service.get_valid_access_token()
        endpoint = f"{self.config.schwab_base_url}/chains"
        final_symbol = self._resolve_option_chain_symbol(symbol)
        diagnostics: Dict[str, Any] = {
            "endpoint": endpoint,
            "final_symbol": final_symbol,
            "final_expiration": target_date.isoformat(),
            "attempt_used": None,
            "raw_params_sent": {},
            "attempts": [],
            "error_detail": None,
            "failure_category": None,
            "failure_label": None,
        }
        self.last_option_chain_diagnostics = diagnostics

        for attempt_label, params in self._build_option_chain_attempts(symbol=final_symbol, expiration_date=target_date):
            diagnostics["raw_params_sent"] = dict(params)
            print("SCHWAB OPTION CHAIN PARAMS:", params)
            LOGGER.info(
                "Schwab option-chain request | symbol=%s | endpoint=%s | expiration_target=%s | attempt=%s | params=%s",
                final_symbol,
                endpoint,
                target_date.isoformat(),
                attempt_label,
                params,
            )

            response = requests.get(
                endpoint,
                params=params,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30,
            )

            if response.status_code == 401:
                raise ProviderAuthRequiredError("Click login to connect to Schwab")

            if response.status_code >= 400:
                failure_detail = self._build_option_chain_failure_detail(response)
                diagnostics["attempts"].append(
                    {
                        "label": attempt_label,
                        "status": "failed",
                        "params": dict(params),
                        "status_code": response.status_code,
                        "response_text": failure_detail["response_text"],
                        "response_json": failure_detail["response_json"],
                        "error_detail": failure_detail["error_detail"],
                        "failure_category": failure_detail["failure_category"],
                        "failure_label": failure_detail["failure_label"],
                    }
                )
                diagnostics["error_detail"] = failure_detail["error_detail"]
                diagnostics["failure_category"] = failure_detail["failure_category"]
                diagnostics["failure_label"] = failure_detail["failure_label"]
                self.last_option_chain_diagnostics = diagnostics
                self._log_option_chain_failure(
                    response=response,
                    params=params,
                    symbol=final_symbol,
                    target_date=target_date,
                    attempt_label=attempt_label,
                )
                continue

            payload = response.json()
            diagnostics["attempts"].append(
                {
                    "label": attempt_label,
                    "status": "succeeded",
                    "params": dict(params),
                    "status_code": response.status_code,
                    "response_text": self._safe_response_text(response),
                    "response_json": payload,
                    "error_detail": None,
                }
            )
            diagnostics["attempt_used"] = attempt_label
            self.last_option_chain_diagnostics = diagnostics

            normalized = self._normalize_option_chain_payload(payload, target_date, requested_symbol=final_symbol)
            normalized["request_diagnostics"] = diagnostics
            return normalized

        if diagnostics.get("error_detail"):
            raise ProviderError(f"Schwab rejected the option-chain request: {diagnostics['error_detail']}")
        raise ProviderError("Schwab option-chain lookup failed before a request could be completed.")

    def debug_option_chain_request(self, symbol: str, target_date: date, minimal_only: bool = True) -> Dict[str, Any]:
        """Run a raw Schwab option-chain request for debugging and return diagnostics."""
        access_token = self.auth_service.get_valid_access_token()
        endpoint = f"{self.config.schwab_base_url}/chains"
        final_symbol = self._resolve_option_chain_symbol(symbol)
        attempts = self._build_option_chain_attempts(symbol=final_symbol, expiration_date=target_date)
        attempt_label, params = attempts[0] if minimal_only else attempts[0]
        print("SCHWAB OPTION CHAIN PARAMS:", params)
        response = requests.get(
            endpoint,
            params=params,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=30,
        )
        try:
            payload = response.json()
        except Exception:
            payload = None
        return {
            "endpoint": endpoint,
            "symbol": final_symbol,
            "expiration": target_date.isoformat(),
            "attempt": attempt_label,
            "params": params,
            "status_code": response.status_code,
            "response_text": self._safe_response_text(response),
            "response_json": payload,
        }

    @classmethod
    def _map_quote_symbol(cls, symbol: str) -> str:
        """Map an internal symbol to a Schwab quote symbol."""
        return cls.QUOTE_SYMBOL_MAP.get(symbol.upper(), cls.QUOTE_SYMBOL_MAP.get(symbol, symbol))

    @classmethod
    def _get_historical_symbol_candidates(cls, symbol: str) -> tuple[str, ...]:
        """Return one or more Schwab historical symbols to try for a lookup."""
        mapped = cls.HISTORICAL_SYMBOL_MAP.get(symbol.upper(), cls.HISTORICAL_SYMBOL_MAP.get(symbol, (symbol,)))
        return tuple(dict.fromkeys(mapped))

    def _get_intraday_symbol_candidates(self, symbol: str) -> tuple[str, ...]:
        normalized = (symbol or "").upper()
        if normalized in {"/ES", "ES"}:
            return tuple(
                dict.fromkeys(
                    [self.config.schwab_es_primary_symbol.strip(), self.config.schwab_es_fallback_symbol.strip()]
                )
            )
        return self._get_historical_symbol_candidates(symbol)

    def _build_historical_params(self, schwab_symbol: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """Build an exact bounded Schwab historical request for daily candles.

        Direct index history for `$SPX` and `$VIX` requires the configured
        period window to accompany the explicit date bounds. Schwab still honors
        `startDate` and `endDate`, and Horme filters the final frame again as a
        safety check.
        """
        return {
            "symbol": schwab_symbol,
            "periodType": self.config.schwab_history_period_type,
            "period": self.config.schwab_history_period,
            "frequencyType": self.config.schwab_history_frequency_type,
            "frequency": self.config.schwab_history_frequency,
            "startDate": self._date_to_epoch_millis(start_date, end_of_day=False),
            "endDate": self._date_to_epoch_millis(end_date, end_of_day=True),
            "needExtendedHoursData": str(self.config.schwab_history_need_extended_hours).lower(),
        }

    @classmethod
    def _get_option_chain_symbol_candidates(cls, symbol: str) -> tuple[str, ...]:
        mapped = cls.OPTION_CHAIN_SYMBOL_MAP.get(symbol.upper(), cls.OPTION_CHAIN_SYMBOL_MAP.get(symbol, (symbol,)))
        return tuple(dict.fromkeys(mapped))

    def build_schwab_option_chain_params(
        self,
        symbol: str,
        expiration_date: date,
        underlying_price: float | None = None,
        contract_type: str | None = "PUT",
        include_underlying_quote: bool | None = None,
        strike_count: int | None = None,
    ) -> Dict[str, Any]:
        """Build a cleaned Schwab option-chain params dict with ISO dates."""
        normalized_symbol = self._resolve_option_chain_symbol(symbol)
        iso_expiration = self._format_option_expiration_date(expiration_date)
        params: Dict[str, Any] = {
            "symbol": normalized_symbol,
            "contractType": contract_type,
            "fromDate": iso_expiration,
            "toDate": iso_expiration,
        }
        if include_underlying_quote is not None:
            params["includeUnderlyingQuote"] = str(include_underlying_quote).lower()
        if strike_count is not None:
            params["strikeCount"] = str(strike_count)
        return self._clean_option_chain_params(params)

    def _build_option_chain_attempts(self, symbol: str, expiration_date: date) -> List[tuple[str, Dict[str, Any]]]:
        """Return the fallback request ladder for the Schwab chain endpoint."""
        attempt_a = self.build_schwab_option_chain_params(symbol=symbol, expiration_date=expiration_date)
        attempt_b = self._clean_option_chain_params(
            {
                "symbol": self._resolve_option_chain_symbol(symbol),
                "fromDate": self._format_option_expiration_date(expiration_date),
                "toDate": self._format_option_expiration_date(expiration_date),
            }
        )
        attempt_c = self._clean_option_chain_params({"symbol": self._resolve_option_chain_symbol(symbol)})
        return [("Attempt A", attempt_a), ("Attempt B", attempt_b), ("Attempt C", attempt_c)]

    def _resolve_option_chain_symbol(self, symbol: str) -> str:
        """Normalize the chain symbol to the same format used by quotes."""
        return self._map_quote_symbol(symbol)

    @staticmethod
    def _clean_option_chain_params(params: Dict[str, Any]) -> Dict[str, Any]:
        """Remove empty values before sending a Schwab request."""
        return {
            key: value
            for key, value in params.items()
            if value is not None and value != "" and value != [] and value != {}
        }

    @staticmethod
    def _format_option_expiration_date(expiration_date: date) -> str:
        """Format a Schwab option expiration as ISO YYYY-MM-DD."""
        return expiration_date.isoformat()

    def _build_option_chain_failure_detail(self, response: Any) -> Dict[str, Any]:
        """Build a detailed failure payload for diagnostics and UI display."""
        try:
            payload = response.json()
        except Exception:
            payload = None
        error_detail = self._extract_safe_error_message(response)
        failure_category, failure_label = self._classify_option_chain_http_failure(response)
        return {
            "error_detail": error_detail,
            "response_text": self._safe_response_text(response),
            "response_json": payload,
            "failure_category": failure_category,
            "failure_label": failure_label,
        }

    @staticmethod
    def _classify_option_chain_http_failure(response: Any) -> tuple[str, str]:
        """Classify a failed option-chain response into a UI-friendly category."""
        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code == 429 or status_code >= 500:
            return ("upstream-unavailable", "Upstream unavailable")
        if 400 <= status_code < 500:
            return ("malformed-request", "Malformed request")
        return ("unknown-error", "Unavailable")

    def _log_option_chain_failure(
        self,
        response: Any,
        params: Dict[str, Any],
        symbol: str,
        target_date: date,
        attempt_label: str,
    ) -> None:
        """Log the full failed option-chain response details."""
        try:
            parsed_json = response.json()
        except Exception:
            parsed_json = None
        print("SCHWAB OPTION CHAIN FAILURE STATUS:", response.status_code)
        print("SCHWAB OPTION CHAIN FAILURE BODY:", self._safe_response_text(response))
        print("SCHWAB OPTION CHAIN FAILURE JSON:", parsed_json)
        LOGGER.warning(
            "Schwab option-chain request failed | symbol=%s | expiration_target=%s | attempt=%s | params=%s | status=%s | body=%s | json=%s",
            symbol,
            target_date.isoformat(),
            attempt_label,
            params,
            response.status_code,
            self._safe_response_text(response),
            json.dumps(parsed_json, default=str) if parsed_json is not None else "null",
        )

    @staticmethod
    def _build_intraday_params(
        schwab_symbol: str,
        interval_minutes: int,
        start_at: datetime,
        end_at: datetime,
    ) -> Dict[str, Any]:
        """Build a same-day intraday request for 5-minute candles."""
        return {
            "symbol": schwab_symbol,
            "frequencyType": "minute",
            "frequency": str(interval_minutes),
            "startDate": int(start_at.timestamp() * 1000),
            "endDate": int(end_at.timestamp() * 1000),
            "needExtendedHoursData": "false",
        }

    @staticmethod
    def _extract_quote_container(payload: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        """Extract the quote payload for the requested Schwab symbol."""
        if symbol in payload and isinstance(payload[symbol], dict):
            return payload[symbol]
        if "quotes" in payload and isinstance(payload["quotes"], dict) and symbol in payload["quotes"]:
            return payload["quotes"][symbol]
        first_value = next(iter(payload.values()), None)
        if isinstance(first_value, dict):
            return first_value
        raise ProviderError("Schwab returned an unexpected quote response format.")

    def _parse_timestamp(self, value: Any) -> datetime | None:
        """Parse Schwab quote timestamps into the display timezone."""
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            timestamp = datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).astimezone(self.display_timezone)
            return timestamp
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed.astimezone(self.display_timezone) if parsed.tzinfo else parsed.replace(tzinfo=self.display_timezone)
            except ValueError:
                return None
        return None

    def _normalize_candle(self, candle: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a Schwab daily candle into the shared DataFrame shape."""
        candle_date = self._parse_candle_date(candle.get("datetime"))
        if candle_date is None:
            raise ProviderError("Schwab returned a candle without a valid datetime.")

        return {
            "Date": candle_date,
            "Open": round(self._coerce_float(candle.get("open")) or 0.0, 2),
            "High": round(self._coerce_float(candle.get("high")) or 0.0, 2),
            "Low": round(self._coerce_float(candle.get("low")) or 0.0, 2),
            "Close": round(self._coerce_float(candle.get("close")) or 0.0, 2),
        }

    def _normalize_intraday_candle(self, candle: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a Schwab intraday candle into the shared DataFrame shape."""
        candle_timestamp = self._parse_timestamp(candle.get("datetime"))
        if candle_timestamp is None:
            raise ProviderError("Schwab returned an intraday candle without a valid datetime.")

        return {
            "Datetime": candle_timestamp,
            "Open": round(self._coerce_float(candle.get("open")) or 0.0, 2),
            "High": round(self._coerce_float(candle.get("high")) or 0.0, 2),
            "Low": round(self._coerce_float(candle.get("low")) or 0.0, 2),
            "Close": round(self._coerce_float(candle.get("close")) or 0.0, 2),
            "Volume": self._coerce_float(candle.get("volume")),
        }

    def _normalize_option_chain_payload(self, payload: Dict[str, Any], target_date: date, requested_symbol: str) -> Dict[str, Any]:
        expiration_dates = self._extract_expiration_dates(payload)
        calls = self._flatten_option_map(payload.get("callExpDateMap"), target_date, put_call="CALL")
        puts = self._flatten_option_map(payload.get("putExpDateMap"), target_date, put_call="PUT")
        return {
            "underlying_symbol": "SPX",
            "requested_symbol": requested_symbol,
            "expiration_target": target_date,
            "expiration_date": target_date,
            "expiration_dates": expiration_dates,
            "expiration_count": len(expiration_dates),
            "underlying_price": self._coerce_float(payload.get("underlyingPrice")),
            "calls": calls,
            "puts": puts,
        }

    def _extract_expiration_dates(self, payload: Dict[str, Any]) -> list[str]:
        expiration_dates: set[str] = set()
        for key in ("callExpDateMap", "putExpDateMap"):
            option_map = payload.get(key)
            if not isinstance(option_map, dict):
                continue
            for expiration_key in option_map.keys():
                expiration_dates.add(str(expiration_key).split(":", 1)[0])
        return sorted(expiration_dates)

    def _flatten_option_map(self, option_map: Any, target_date: date, put_call: str) -> list[Dict[str, Any]]:
        if not isinstance(option_map, dict):
            return []

        normalized: list[Dict[str, Any]] = []
        target_prefix = target_date.isoformat()
        for expiration_key, strike_map in option_map.items():
            if not str(expiration_key).startswith(target_prefix):
                continue
            if not isinstance(strike_map, dict):
                continue
            for strike_key, contracts in strike_map.items():
                if not isinstance(contracts, list):
                    continue
                for contract in contracts:
                    normalized.append(
                        {
                            "symbol": contract.get("symbol"),
                            "description": contract.get("description"),
                            "put_call": contract.get("putCall") or put_call,
                            "strike": self._coerce_float(contract.get("strikePrice") or strike_key),
                            "bid": self._coerce_float(contract.get("bid")),
                            "ask": self._coerce_float(contract.get("ask")),
                            "last": self._coerce_float(contract.get("last")),
                            "mark": self._coerce_float(contract.get("mark")),
                            "delta": self._coerce_float(contract.get("delta")),
                            "open_interest": self._coerce_float(contract.get("openInterest")),
                            "total_volume": self._coerce_float(contract.get("totalVolume")),
                        }
                    )
        return normalized

    @staticmethod
    def _row_to_dict(row: pd.Series) -> Dict[str, Any]:
        """Convert a pandas row into a plain dictionary."""
        result: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, float):
                result[key] = round(value, 2)
            else:
                result[key] = value
        return result

    @staticmethod
    def _date_to_epoch_millis(value: date, end_of_day: bool = False) -> int:
        """Convert a date boundary to epoch milliseconds in UTC.

        Schwab historical requests expect epoch milliseconds. For inclusive date
        ranges we send the start of the first requested day and the final moment
        of the last requested day.
        """
        boundary = time.max if end_of_day else time.min
        timestamp = datetime.combine(value, boundary, tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)

    @staticmethod
    def _safe_response_text(response: Any, limit: int = 2000) -> str:
        """Return a truncated response body for safe diagnostics."""
        text = getattr(response, "text", "") or ""
        return text[:limit]

    def _extract_safe_error_message(self, response: Any) -> str:
        """Extract a concise safe error message from a non-200 Schwab response."""
        try:
            payload = response.json()
        except Exception:
            payload = None

        if isinstance(payload, dict):
            for key in ("message", "error", "error_description", "details"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
                if isinstance(value, list) and value:
                    first = value[0]
                    if isinstance(first, str) and first.strip():
                        return first.strip()

        response_text = self._safe_response_text(response).strip()
        if response_text:
            return response_text
        return f"HTTP {getattr(response, 'status_code', 'unknown')}"

    @staticmethod
    def _parse_candle_date(value: Any) -> date | None:
        """Parse a daily candle timestamp into its trading-session date."""
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).date()
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc).date()
            except ValueError:
                return None
        return None

    def _get_same_day_session_window(self) -> Dict[str, Any]:
        """Return today's regular-session window in the display timezone and UTC."""
        return self._get_session_window_for_date(datetime.now(self.display_timezone).date())

    def _get_session_window_for_date(self, session_date: date) -> Dict[str, Any]:
        """Return a regular-session window for the requested date."""
        session_start_local = datetime.combine(session_date, time(8, 30), tzinfo=self.display_timezone)
        session_end_local = datetime.combine(session_date, time(15, 0), tzinfo=self.display_timezone)
        local_now = datetime.now(self.display_timezone)
        if session_date == local_now.date():
            session_end_local = min(local_now, session_end_local)
            if session_end_local <= session_start_local:
                session_end_local = session_start_local

        return {
            "session_date": session_date,
            "start_utc": session_start_local.astimezone(timezone.utc),
            "end_utc": session_end_local.astimezone(timezone.utc),
        }

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        """Convert a numeric-like value to float when possible."""
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _not_ready_error(self) -> ProviderNotImplementedError:
        """Return a friendly not-yet-implemented provider error."""
        return ProviderNotImplementedError(
            "Schwab option-chain endpoints are not wired yet. Latest quote and historical price lookups are available after OAuth login."
        )