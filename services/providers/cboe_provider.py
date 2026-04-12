"""Cboe historical provider for VIX daily price history."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict

import pandas as pd

from .base_provider import BaseMarketDataProvider, ProviderConfigurationError, ProviderError

LOGGER = logging.getLogger(__name__)
VIX_HISTORY_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"


class CboeVixHistoricalProvider(BaseMarketDataProvider):
    """Historical-only provider for VIX daily candles published by Cboe."""

    provider_key = "cboe"
    provider_name = "Cboe"

    def get_latest_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Latest quotes are not served by the Cboe fallback provider."""
        raise ProviderConfigurationError("Cboe is configured only for VIX historical data, not live quotes.")

    def get_historical_range(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Return daily VIX candles for the requested date range."""
        if symbol not in {"^VIX", "VIX"}:
            raise ProviderConfigurationError("Cboe historical data is currently configured only for VIX.")

        try:
            LOGGER.info(
                "Cboe VIX historical request | url=%s | requested_start=%s | requested_end=%s",
                VIX_HISTORY_URL,
                start_date.isoformat(),
                end_date.isoformat(),
            )
            frame = pd.read_csv(VIX_HISTORY_URL)
        except Exception as exc:
            raise ProviderError(f"Unable to retrieve Cboe VIX historical data right now: {exc}") from exc

        normalized = frame.rename(
            columns={
                "DATE": "Date",
                "OPEN": "Open",
                "HIGH": "High",
                "LOW": "Low",
                "CLOSE": "Close",
            }
        )
        normalized["Date"] = pd.to_datetime(normalized["Date"], format="%m/%d/%Y", errors="coerce").dt.date
        normalized = normalized[["Date", "Open", "High", "Low", "Close"]].dropna(subset=["Date", "Close"])
        normalized = normalized.sort_values("Date").reset_index(drop=True)
        filtered = normalized.loc[(normalized["Date"] >= start_date) & (normalized["Date"] <= end_date)].copy()

        if filtered.empty:
            raise ProviderError(
                f"Cboe returned no VIX historical data between {start_date.isoformat()} and {end_date.isoformat()}."
            )

        for column in ["Open", "High", "Low", "Close"]:
            filtered[column] = pd.to_numeric(filtered[column], errors="coerce").round(2)

        return filtered

    def get_single_date(self, symbol: str, target_date: date) -> Dict[str, Any]:
        """Return a single VIX daily candle from the Cboe dataset."""
        history = self.get_historical_range(symbol, target_date, target_date)
        row = history.loc[history["Date"] == target_date]
        if row.empty:
            raise ProviderError(f"No VIX historical data exists on {target_date.isoformat()}.")
        data = row.iloc[0]
        return {
            "Date": data["Date"],
            "Open": round(float(data["Open"]), 2),
            "High": round(float(data["High"]), 2),
            "Low": round(float(data["Low"]), 2),
            "Close": round(float(data["Close"]), 2),
        }
