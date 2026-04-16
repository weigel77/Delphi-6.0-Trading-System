import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.trade_store import (
    DISTANCE_SOURCE_DERIVED,
    DISTANCE_SOURCE_ESTIMATED,
    DISTANCE_SOURCE_ORIGINAL,
    EXPECTED_MOVE_SOURCE_ESTIMATED,
    EXPECTED_MOVE_SOURCE_ORIGINAL,
    EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE,
    TradeStore,
    resolve_trade_credit_model,
    resolve_trade_distance,
    resolve_trade_expected_move,
)


class _HistoricalStub:
    def get_history_with_changes(self, ticker, start_date, end_date, query_type="history"):
        close_values = [
            {"Date": pd.Timestamp("2026-04-02"), "Close": 6380.0 if ticker == "^GSPC" else 19.5},
            {"Date": pd.Timestamp("2026-04-03"), "Close": 6390.0 if ticker == "^GSPC" else 20.0},
        ]
        return pd.DataFrame(
            close_values
        )


class TradeStoreDistanceTest(unittest.TestCase):
    def test_resolve_trade_distance_prefers_stored_value_and_flags_material_discrepancy(self):
        metadata = resolve_trade_distance(
            {
                "system_name": "Apollo",
                "option_type": "Put Credit Spread",
                "spx_at_entry": 6500.0,
                "short_strike": 6450.0,
                "distance_to_short": 35.0,
            }
        )

        self.assertEqual(metadata["source"], DISTANCE_SOURCE_ORIGINAL)
        self.assertAlmostEqual(metadata["value"], 35.0)
        self.assertAlmostEqual(metadata["derived_value"], 50.0)
        self.assertTrue(metadata["discrepancy_is_material"])

    def test_resolve_trade_distance_uses_apollo_historical_fallback_when_needed(self):
        metadata = resolve_trade_distance(
            {
                "system_name": "Apollo",
                "option_type": "Put Credit Spread",
                "expiration_date": "2026-04-04",
                "short_strike": 6400.0,
            },
            historical_price_lookup=lambda values: {"reference_price": 6390.0, "reference_date": "2026-04-03"},
        )

        self.assertEqual(metadata["source"], DISTANCE_SOURCE_ESTIMATED)
        self.assertAlmostEqual(metadata["value"], 10.0)
        self.assertEqual(metadata["reference_date"], "2026-04-03")

    def test_backfill_distance_sources_updates_existing_apollo_trade(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "horme_trades.db"
            store = TradeStore(database_path)
            store.initialize()
            trade_id = store.create_trade(
                {
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "journal_name": "Apollo Main",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "trade_date": "2026-04-01",
                    "expiration_date": "2026-04-04",
                    "underlying_symbol": "SPX",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6400",
                    "long_strike": "6395",
                    "spread_width": "5",
                    "contracts": "1",
                    "actual_entry_credit": "1.5",
                    "actual_exit_value": "0.5",
                    "close_reason": "Target",
                }
            )

            unresolved_trade = store.get_trade(trade_id)
            self.assertIsNone(unresolved_trade["distance_to_short"])

            store.market_data_service = _HistoricalStub()
            report = store.backfill_distance_sources()
            updated_trade = store.get_trade(trade_id)

            self.assertEqual(report["updated_trade_count"], 1)
            self.assertEqual(report["source_counts"][DISTANCE_SOURCE_ESTIMATED], 1)
            self.assertAlmostEqual(updated_trade["distance_to_short"], 10.0)
            self.assertEqual(updated_trade["distance_source"], DISTANCE_SOURCE_ESTIMATED)

    def test_resolve_trade_expected_move_prefers_stored_original_value(self):
        metadata = resolve_trade_expected_move(
            {
                "system_name": "Apollo",
                "expected_move": 32.5,
            }
        )

        self.assertEqual(metadata["source"], EXPECTED_MOVE_SOURCE_ORIGINAL)
        self.assertAlmostEqual(metadata["value"], 32.5)
        self.assertAlmostEqual(metadata["used_value"], 32.5)

    def test_resolve_trade_expected_move_recovers_kairos_candidate_from_spx_vix(self):
        metadata = resolve_trade_expected_move(
            {
                "system_name": "Kairos",
                "spx_at_entry": 6400.0,
                "vix_at_entry": 20.0,
            }
        )

        self.assertEqual(metadata["source"], EXPECTED_MOVE_SOURCE_RECOVERED_CANDIDATE)
        self.assertAlmostEqual(metadata["value"], 80.0)

    def test_backfill_expected_move_sources_updates_existing_trade(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "horme_trades.db"
            store = TradeStore(database_path, market_data_service=_HistoricalStub())
            store.initialize()
            trade_id = store.create_trade(
                {
                    "trade_mode": "real",
                    "system_name": "Apollo",
                    "journal_name": "Apollo Main",
                    "candidate_profile": "Standard",
                    "status": "closed",
                    "trade_date": "2026-04-03",
                    "expiration_date": "2026-04-04",
                    "underlying_symbol": "SPX",
                    "option_type": "Put Credit Spread",
                    "short_strike": "6400",
                    "long_strike": "6395",
                    "spread_width": "5",
                    "contracts": "1",
                    "actual_entry_credit": "1.5",
                    "actual_exit_value": "0.5",
                    "close_reason": "Target",
                }
            )

            unresolved_trade = store.get_trade(trade_id)
            self.assertIsNone(unresolved_trade["expected_move"])

            report = store.backfill_expected_move_sources()
            updated_trade = store.get_trade(trade_id)

            self.assertEqual(report["updated_trade_count"], 1)
            self.assertEqual(report["source_counts"][EXPECTED_MOVE_SOURCE_ESTIMATED], 1)
            self.assertAlmostEqual(updated_trade["expected_move"], 79.88, places=2)
            self.assertEqual(updated_trade["expected_move_source"], EXPECTED_MOVE_SOURCE_ESTIMATED)
            self.assertAlmostEqual(updated_trade["expected_move_raw_estimate"], 79.88, places=2)
            self.assertAlmostEqual(updated_trade["expected_move_calibrated"], 54.32, places=2)
            self.assertAlmostEqual(updated_trade["expected_move_used"], 54.32, places=2)
            self.assertEqual(updated_trade["expected_move_confidence"], "low")

    def test_resolve_trade_credit_model_recomputes_hidden_credit_totals_when_entry_credit_changes(self):
        credit_model = resolve_trade_credit_model(
            {
                "contracts": "2",
                "spread_width": "5",
                "actual_entry_credit": "1.74",
                "premium_per_contract": "173.00",
                "total_premium": "346.00",
                "max_theoretical_risk": "654.00",
            }
        )

        self.assertEqual(credit_model["source"], "per_contract_credit")
        self.assertAlmostEqual(credit_model["net_credit_per_contract"], 1.74)
        self.assertAlmostEqual(credit_model["premium_per_contract"], 174.0)
        self.assertAlmostEqual(credit_model["total_premium"], 348.0)
        self.assertAlmostEqual(credit_model["max_theoretical_risk"], 652.0)


if __name__ == "__main__":
    unittest.main()