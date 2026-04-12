"""Yahoo Finance market-data provider backed by yfinance."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict

import pandas as pd
import yfinance as yf

from ..calculations import add_daily_change_columns, calculate_percent_change, calculate_point_change
from .base_provider import BaseMarketDataProvider, ProviderError, ProviderRateLimitError

LOGGER = logging.getLogger(__name__)
RETRY_DELAYS_SECONDS = (2, 5)


class YahooProvider(BaseMarketDataProvider):
    """Yahoo Finance implementation of the provider contract."""

    provider_key = "yahoo"
    provider_name = "Yahoo Finance"

    def get_latest_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Return the latest available daily or intraday snapshot for the symbol."""
        daily_frame = self._fetch_history_by_period(symbol=symbol, period="2mo", interval="1d")
        daily_frame = add_daily_change_columns(daily_frame)

        if daily_frame.empty:
            raise ProviderError(f"Yahoo Finance returned no recent data for {symbol}.")

        latest_row = daily_frame.iloc[-1]
        prior_close = self._get_prior_close(daily_frame, latest_row["Date"])

        latest_value = float(latest_row["Close"])
        timestamp_label = f"{latest_row['Date'].isoformat()} (daily close)"
        session_date = latest_row["Date"]

        intraday_frame = self._fetch_intraday_history(symbol)
        if not intraday_frame.empty:
            intraday_frame = intraday_frame.dropna(subset=["Close"])
            if not intraday_frame.empty:
                latest_tick = intraday_frame.iloc[-1]
                latest_value = round(float(latest_tick["Close"]), 2)
                tick_timestamp = self._to_display_timestamp(intraday_frame.index[-1])
                timestamp_label = tick_timestamp.strftime("%Y-%m-%d %I:%M:%S %p %Z")
                session_date = tick_timestamp.date()
                matching_rows = daily_frame.loc[daily_frame["Date"] == session_date]
                if not matching_rows.empty:
                    latest_row = matching_rows.iloc[-1]
                    prior_close = self._get_prior_close(daily_frame, latest_row["Date"])

        point_change = calculate_point_change(latest_value, prior_close) if prior_close is not None else None
        percent_change = calculate_percent_change(latest_value, prior_close) if prior_close is not None else None

        return {
            "Ticker": symbol,
            "Market Date": session_date,
            "Latest Value": latest_value,
            "Open": round(float(latest_row["Open"]), 2),
            "High": round(float(latest_row["High"]), 2),
            "Low": round(float(latest_row["Low"]), 2),
            "Close": round(float(latest_row["Close"]), 2),
            "Prior Close": round(float(prior_close), 2) if prior_close is not None else None,
            "Daily Point Change": point_change,
            "Daily Percent Change": percent_change,
            "As Of": timestamp_label,
        }

    def get_historical_range(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Return inclusive daily history for the symbol and date range."""
        history_frame = self._fetch_history_by_dates(symbol=symbol, start_date=start_date, end_date=end_date)
        if history_frame.empty:
            raise ProviderError(
                f"Yahoo Finance returned no data for {symbol} between {start_date.isoformat()} and {end_date.isoformat()}."
            )
        return history_frame

    def get_single_date(self, symbol: str, target_date: date) -> Dict[str, Any]:
        """Return a single daily bar for the requested date."""
        history_frame = self._fetch_history_by_dates(symbol=symbol, start_date=target_date, end_date=target_date)
        if history_frame.empty:
            raise ProviderError(f"Yahoo Finance returned no data for {symbol} around {target_date.isoformat()}.")

        matching_rows = history_frame.loc[history_frame["Date"] == target_date]
        if matching_rows.empty:
            raise ProviderError(
                f"No trading data exists for {symbol} on {target_date.isoformat()}. The market may have been closed."
            )

        return self._row_to_dict(matching_rows.iloc[0])

    def get_same_day_intraday_candles(self, symbol: str, interval_minutes: int = 5) -> pd.DataFrame:
        """Return same-day intraday candles for the exact requested symbol."""
        return self.get_intraday_candles_for_date(symbol, target_date=datetime.now(self.display_timezone).date(), interval_minutes=interval_minutes)

    def get_intraday_candles_for_date(self, symbol: str, target_date: date, interval_minutes: int = 5) -> pd.DataFrame:
        """Return intraday candles for the requested trading date."""
        raw_frame = self._execute_with_retry(
            lambda: yf.Ticker(symbol).history(
                start=target_date.isoformat(),
                end=(target_date + timedelta(days=1)).isoformat(),
                interval=f"{interval_minutes}m",
                auto_adjust=False,
                actions=False,
                prepost=False,
            ),
            operation_label=f"{target_date.isoformat()} {interval_minutes}-minute candles for {symbol}",
        )

        if raw_frame is None or raw_frame.empty:
            raise ProviderError(f"Yahoo Finance returned no {target_date.isoformat()} {interval_minutes}-minute candles for {symbol}.")

        frame = raw_frame.reset_index().copy()
        time_column = "Datetime" if "Datetime" in frame.columns else frame.columns[0]
        frame[time_column] = pd.to_datetime(frame[time_column], errors="coerce")
        if pd.api.types.is_datetime64tz_dtype(frame[time_column]):
            frame[time_column] = frame[time_column].dt.tz_convert(self.display_timezone)
        else:
            frame[time_column] = frame[time_column].dt.tz_localize(self.display_timezone)

        keep_columns = [time_column, "Open", "High", "Low", "Close"]
        if "Volume" in frame.columns:
            keep_columns.append("Volume")
        frame = frame[keep_columns].dropna(subset=[time_column, "Open", "High", "Low", "Close"])
        frame = frame.rename(columns={time_column: "Datetime"}).sort_values("Datetime").reset_index(drop=True)

        frame = frame.loc[frame["Datetime"].dt.date == target_date].reset_index(drop=True)

        if frame.empty:
            raise ProviderError(f"Yahoo Finance returned no usable {target_date.isoformat()} {interval_minutes}-minute candles for {symbol}.")

        return frame

    def _fetch_history_by_dates(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch inclusive daily history from Yahoo Finance."""
        end_exclusive = end_date + timedelta(days=1)
        raw_frame = self._execute_with_retry(
            lambda: yf.Ticker(symbol).history(
                start=start_date.isoformat(),
                end=end_exclusive.isoformat(),
                interval="1d",
                auto_adjust=False,
                actions=False,
            ),
            operation_label=f"historical data for {symbol}",
        )
        return self._normalize_history_frame(raw_frame)

    def _fetch_history_by_period(self, symbol: str, period: str, interval: str) -> pd.DataFrame:
        """Fetch recent history using Yahoo period-based queries."""
        raw_frame = self._execute_with_retry(
            lambda: yf.Ticker(symbol).history(
                period=period,
                interval=interval,
                auto_adjust=False,
                actions=False,
            ),
            operation_label=f"recent data for {symbol}",
        )
        return self._normalize_history_frame(raw_frame)

    def _fetch_intraday_history(self, symbol: str) -> pd.DataFrame:
        """Fetch intraday bars when available, falling back silently if unavailable."""
        try:
            return self._execute_with_retry(
                lambda: yf.Ticker(symbol).history(
                    period="1d",
                    interval="1m",
                    auto_adjust=False,
                    actions=False,
                    prepost=False,
                ),
                operation_label=f"intraday data for {symbol}",
            )
        except ProviderError as exc:
            LOGGER.warning("Intraday Yahoo lookup failed for %s. Falling back to daily data. Error: %s", symbol, exc)
            return pd.DataFrame()

    def _execute_with_retry(self, operation: Callable[[], Any], operation_label: str = "Yahoo Finance request") -> Any:
        """Run a Yahoo Finance operation with transient retry handling."""
        label = operation_label
        last_error: ProviderError | None = None

        for attempt_index in range(len(RETRY_DELAYS_SECONDS) + 1):
            try:
                return operation()
            except Exception as exc:  # pragma: no cover - network/provider variability
                last_error = self._normalize_provider_error(exc)

            has_retry_remaining = attempt_index < len(RETRY_DELAYS_SECONDS)
            if last_error.is_transient and has_retry_remaining:
                delay_seconds = RETRY_DELAYS_SECONDS[attempt_index]
                LOGGER.warning(
                    "Transient Yahoo Finance error while requesting %s on attempt %s. Retrying in %s seconds. Error: %s",
                    label,
                    attempt_index + 1,
                    delay_seconds,
                    last_error,
                )
                time.sleep(delay_seconds)
                continue

            break

        raise last_error or ProviderError(f"Unable to retrieve {label} from Yahoo Finance.")

    def _normalize_history_frame(self, raw_frame: pd.DataFrame) -> pd.DataFrame:
        """Normalize Yahoo history into the shared DataFrame shape."""
        if raw_frame is None or raw_frame.empty:
            return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close"])

        frame = raw_frame.reset_index().copy()
        date_column = "Date" if "Date" in frame.columns else frame.columns[0]
        frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce")

        if pd.api.types.is_datetime64tz_dtype(frame[date_column]):
            frame[date_column] = frame[date_column].dt.tz_convert(self.display_timezone).dt.date
        else:
            frame[date_column] = frame[date_column].dt.date

        frame = frame[[date_column, "Open", "High", "Low", "Close"]].dropna(subset=[date_column, "Close"])
        frame = frame.rename(columns={date_column: "Date"})
        frame = frame.sort_values("Date").reset_index(drop=True)

        numeric_columns = frame.select_dtypes(include=["number"]).columns
        for column in numeric_columns:
            frame[column] = frame[column].round(2)

        return frame

    @staticmethod
    def _row_to_dict(row: pd.Series) -> Dict[str, Any]:
        """Convert a pandas row into a clean dictionary."""
        result: Dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, float):
                result[key] = round(value, 2)
            else:
                result[key] = value
        return result

    def _to_display_timestamp(self, value: Any) -> datetime:
        """Convert a provider timestamp to the configured display timezone."""
        timestamp = pd.Timestamp(value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        return timestamp.tz_convert(self.display_timezone).to_pydatetime()

    @staticmethod
    def _get_prior_close(dataframe: pd.DataFrame, market_date: date) -> float | None:
        """Return the previous trading day's close when available."""
        previous_rows = dataframe.loc[dataframe["Date"] < market_date]
        if previous_rows.empty:
            return None
        return float(previous_rows.iloc[-1]["Close"])

    @staticmethod
    def _normalize_provider_error(error: Exception) -> ProviderError:
        """Convert low-level Yahoo errors into consistent provider errors."""
        message = str(error)
        if YahooProvider._is_rate_limit_message(message):
            return ProviderRateLimitError()
        if YahooProvider._is_transient_message(message):
            return ProviderError(f"Unable to retrieve market data from Yahoo Finance right now: {message}", is_transient=True)
        return ProviderError(f"Unable to retrieve market data from Yahoo Finance right now: {message}")

    @staticmethod
    def _is_rate_limit_message(message: str) -> bool:
        """Return whether the provider message indicates rate limiting."""
        lowered = message.lower()
        rate_limit_keywords = (
            "rate-limit",
            "rate limit",
            "rate limited",
            "too many requests",
            "429",
        )
        return any(keyword in lowered for keyword in rate_limit_keywords)

    @staticmethod
    def _is_transient_message(message: str) -> bool:
        """Return whether the provider message looks transient."""
        lowered = message.lower()
        transient_keywords = (
            "rate-limit",
            "rate limit",
            "too many requests",
            "429",
            "timed out",
            "timeout",
            "temporarily",
            "connection",
            "unavailable",
            "server error",
            "bad gateway",
            "gateway timeout",
        )
        return any(keyword in lowered for keyword in transient_keywords)