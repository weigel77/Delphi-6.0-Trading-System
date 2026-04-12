from __future__ import annotations

import os
import unittest
from dataclasses import replace
from pathlib import Path

from config import get_app_config
from services.apollo_service import ApolloService
from services.market_data import MarketDataService


class ApolloLiveSmokeTest(unittest.TestCase):
    def test_live_apollo_smoke_returns_structured_result(self) -> None:
        enabled = str(os.getenv("APOLLO_LIVE_SMOKE", "")).strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            self.skipTest("Set APOLLO_LIVE_SMOKE=1 to enable the live Apollo smoke test.")

        config = replace(
            get_app_config(),
            market_data_provider="schwab",
            market_data_live_provider="schwab",
        )
        token_path = Path(config.schwab_token_path)
        if not token_path.exists():
            self.skipTest(f"Live Apollo smoke skipped because no Schwab token file exists at {token_path}.")
        if not config.schwab_client_id or not config.schwab_client_secret or not config.schwab_redirect_uri:
            self.skipTest("Live Apollo smoke skipped because Schwab OAuth settings are incomplete.")

        market_data_service = MarketDataService(config=config)
        apollo_service = ApolloService(market_data_service=market_data_service, config=config)
        payload = apollo_service.run_precheck()

        self.assertIn("option_chain", payload)
        self.assertIn("trade_candidates", payload)
        option_chain = payload["option_chain"]
        self.assertIn(option_chain.get("success"), {True, False})
        self.assertIn("request_diagnostics", option_chain)
        self.assertIn(option_chain.get("failure_category", ""), {"", "upstream-unavailable", "malformed-request", "empty-response", "exchange-closed", "unknown-error"})


if __name__ == "__main__":
    unittest.main()
