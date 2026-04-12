"""Scaffold provider for SPX historical fallback data."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict

import pandas as pd

from .base_provider import BaseMarketDataProvider, ProviderConfigurationError


class SpxHistoricalUnavailableProvider(BaseMarketDataProvider):
    """Placeholder provider until an SPX historical fallback source is wired."""

    provider_key = "spx-historical-unconfigured"
    provider_name = "SPX Historical Provider (Not Configured)"

    def get_latest_snapshot(self, symbol: str) -> Dict[str, Any]:
        raise ProviderConfigurationError("SPX historical provider not yet configured.")

    def get_historical_range(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        raise ProviderConfigurationError("SPX historical provider not yet configured.")

    def get_single_date(self, symbol: str, target_date: date) -> Dict[str, Any]:
        raise ProviderConfigurationError("SPX historical provider not yet configured.")
