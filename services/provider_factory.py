"""Factory for creating configured live and historical market-data providers."""

from __future__ import annotations

from typing import Dict, Optional

from config import AppConfig, get_app_config

from .runtime.provider_composition import LocalProviderComposer, ProviderComposer
from .providers.base_provider import BaseMarketDataProvider, UnavailableProvider
from .providers.cboe_provider import CboeVixHistoricalProvider
from .providers.schwab_provider import SchwabProvider
from .providers.spx_history_provider import SpxHistoricalUnavailableProvider
from .providers.yahoo_provider import YahooProvider


class ProviderFactory:
    """Instantiate the active market-data provider from configuration."""

    @staticmethod
    def create_provider(config: Optional[AppConfig] = None, provider_composer: ProviderComposer | None = None) -> BaseMarketDataProvider:
        """Backward-compatible alias for the configured live provider."""
        return ProviderFactory.create_live_provider(config, provider_composer=provider_composer)

    @staticmethod
    def create_live_provider(config: Optional[AppConfig] = None, provider_composer: ProviderComposer | None = None) -> BaseMarketDataProvider:
        """Return the configured live/latest provider."""
        app_config = config or get_app_config()
        composer = provider_composer or LocalProviderComposer(app_config)
        return composer.create_live_provider()

    @staticmethod
    def create_historical_providers(
        config: Optional[AppConfig] = None,
        provider_composer: ProviderComposer | None = None,
    ) -> Dict[str, BaseMarketDataProvider]:
        """Return configured historical providers keyed by ticker family."""
        app_config = config or get_app_config()
        composer = provider_composer or LocalProviderComposer(app_config)
        return composer.create_historical_providers()

    @staticmethod
    def _create_historical_provider(capability: str, config: AppConfig) -> BaseMarketDataProvider:
        """Create a historical provider for a symbol family."""
        provider_key = ProviderFactory._resolve_historical_provider_key(capability, config)

        if provider_key == "yahoo":
            return YahooProvider(display_timezone=config.app_timezone)
        if provider_key == "schwab":
            return ProviderFactory._create_schwab_provider(config)
        if provider_key == "cboe" and capability == "vix":
            return CboeVixHistoricalProvider(display_timezone=config.app_timezone)
        if provider_key in {"none", "unconfigured", "spx_stub"} and capability == "spx":
            return SpxHistoricalUnavailableProvider(display_timezone=config.app_timezone)

        return UnavailableProvider(
            provider_key=f"{capability}-historical-{provider_key}",
            message=(
                f"Unsupported historical provider '{provider_key}' for {capability.upper()}. "
                "Update your historical provider configuration."
            ),
            display_timezone=config.app_timezone,
        )

    @staticmethod
    def _resolve_live_provider_key(config: AppConfig) -> str:
        """Resolve the configured live provider key."""
        return (config.market_data_live_provider or config.market_data_provider or "yahoo").strip().lower()

    @staticmethod
    def _resolve_historical_provider_key(capability: str, config: AppConfig) -> str:
        """Resolve the configured historical provider key for a capability."""
        live_provider = ProviderFactory._resolve_live_provider_key(config)

        if capability == "vix":
            configured = (config.vix_historical_provider or "").strip().lower()
            if configured:
                return configured
            return "cboe" if live_provider == "schwab" else live_provider

        if capability == "spx":
            configured = (config.spx_historical_provider or "").strip().lower()
            if configured:
                return configured
            return "schwab" if live_provider == "schwab" else live_provider

        return live_provider