import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from app import create_app
from services.performance_engine import PerformanceEngine


class PerformanceEngineTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temp_dir.name) / "horme_trades.db"
        self.app = create_app({"TESTING": True, "TRADE_DATABASE": str(self.database_path)})
        self.client = self.app.test_client()
        self._seed_trades()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_trade(self, trade_mode: str, **overrides):
        payload = {
            "trade_number": overrides.pop("trade_number", ""),
            "trade_mode": trade_mode,
            "system_name": overrides.pop("system_name", "Apollo"),
            "journal_name": overrides.pop("journal_name", "Apollo Main"),
            "system_version": overrides.pop("system_version", "4.0"),
            "candidate_profile": overrides.pop("candidate_profile", "Legacy"),
            "status": overrides.pop("status", "closed"),
            "trade_date": overrides.pop("trade_date", "2026-04-08"),
            "entry_datetime": overrides.pop("entry_datetime", ""),
            "expiration_date": overrides.pop("expiration_date", "2026-04-10"),
            "underlying_symbol": overrides.pop("underlying_symbol", "SPX"),
            "spx_at_entry": overrides.pop("spx_at_entry", "6500"),
            "vix_at_entry": overrides.pop("vix_at_entry", "20"),
            "structure_grade": overrides.pop("structure_grade", "Good"),
            "macro_grade": overrides.pop("macro_grade", "None"),
            "expected_move": overrides.pop("expected_move", "40"),
            "option_type": overrides.pop("option_type", "Put Credit Spread"),
            "short_strike": overrides.pop("short_strike", "6450"),
            "long_strike": overrides.pop("long_strike", "6445"),
            "spread_width": overrides.pop("spread_width", "5"),
            "contracts": overrides.pop("contracts", "1"),
            "candidate_credit_estimate": overrides.pop("candidate_credit_estimate", ""),
            "actual_entry_credit": overrides.pop("actual_entry_credit", "1.5"),
            "distance_to_short": overrides.pop("distance_to_short", "50"),
            "short_delta": overrides.pop("short_delta", ""),
            "notes_entry": overrides.pop("notes_entry", ""),
            "prefill_source": overrides.pop("prefill_source", ""),
            "exit_datetime": overrides.pop("exit_datetime", ""),
            "spx_at_exit": overrides.pop("spx_at_exit", ""),
            "actual_exit_value": overrides.pop("actual_exit_value", "0.5"),
            "close_method": overrides.pop("close_method", "Close"),
            "close_reason": overrides.pop("close_reason", "Closed for validation"),
            "notes_exit": overrides.pop("notes_exit", ""),
        }
        payload.update(overrides)
        response = self.client.post(f"/trades/{trade_mode}/new", data=payload, follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def _seed_trades(self):
        self._create_trade("real", system_name="Apollo", vix_at_entry="17", structure_grade="Good", actual_entry_credit="2.0", actual_exit_value="0.5", distance_to_short="45")
        self._create_trade("real", system_name="Kairos", vix_at_entry="23", structure_grade="Neutral", actual_entry_credit="1.2", actual_exit_value="2.4", distance_to_short="30")
        self._create_trade("simulated", system_name="Aegis", vix_at_entry="28", structure_grade="Poor", actual_entry_credit="1.8", actual_exit_value="0.3", distance_to_short="60")

    def test_trade_migration_adds_learning_columns(self):
        legacy_path = Path(self.temp_dir.name) / "legacy.db"
        with closing(sqlite3.connect(legacy_path)) as connection:
            connection.execute("DROP TABLE IF EXISTS trades")
            connection.execute(
                """
                CREATE TABLE trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    trade_mode TEXT NOT NULL,
                    system_name TEXT NOT NULL,
                    journal_name TEXT,
                    system_version TEXT,
                    candidate_profile TEXT,
                    status TEXT NOT NULL,
                    trade_date TEXT,
                    entry_datetime TEXT,
                    expiration_date TEXT,
                    underlying_symbol TEXT,
                    spx_at_entry REAL,
                    vix_at_entry REAL,
                    structure_grade TEXT,
                    macro_grade TEXT,
                    expected_move REAL,
                    option_type TEXT,
                    short_strike REAL,
                    long_strike REAL,
                    spread_width REAL,
                    contracts INTEGER,
                    candidate_credit_estimate REAL,
                    actual_entry_credit REAL,
                    distance_to_short REAL,
                    short_delta REAL,
                    notes_entry TEXT,
                    exit_datetime TEXT,
                    spx_at_exit REAL,
                    actual_exit_value REAL,
                    close_method TEXT,
                    close_reason TEXT,
                    notes_exit TEXT,
                    gross_pnl REAL,
                    max_risk REAL,
                    roi_on_risk REAL,
                    hours_held REAL,
                    win_loss_result TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO trades (
                    created_at, updated_at, trade_mode, system_name, journal_name, system_version,
                    candidate_profile, status, trade_date, expiration_date, underlying_symbol,
                    spx_at_entry, vix_at_entry, structure_grade, macro_grade, expected_move,
                    short_strike, long_strike, spread_width, contracts, actual_entry_credit,
                    distance_to_short, actual_exit_value, close_method, gross_pnl, max_risk, win_loss_result
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-04-08T09:30:00",
                    "2026-04-08T10:30:00",
                    "real",
                    "Aegis",
                    "Apollo Main",
                    "4.0",
                    "Legacy",
                    "closed",
                    "2026-04-08",
                    "2026-04-10",
                    "SPX",
                    6500,
                    21,
                    "Good",
                    "Minor",
                    42,
                    6450,
                    6445,
                    5,
                    1,
                    1.5,
                    50,
                    0.5,
                    "Close",
                    100.0,
                    350.0,
                    "Win",
                ),
            )
            connection.commit()

        migrated_app = create_app({"TESTING": True, "TRADE_DATABASE": str(legacy_path)})
        self.assertIsNotNone(migrated_app)
        with closing(sqlite3.connect(legacy_path)) as connection:
            connection.row_factory = sqlite3.Row
            columns = {row[1] for row in connection.execute("PRAGMA table_info(trades)").fetchall()}
            row = connection.execute("SELECT system, spx_entry, vix_entry, structure, credit_received, max_loss, result, pnl, exit_type, macro_flag FROM trades").fetchone()

        self.assertTrue({"system", "spx_entry", "vix_entry", "structure", "credit_received", "max_loss", "result", "pnl", "max_drawdown", "exit_type", "macro_flag"}.issubset(columns))
        self.assertEqual(row["system"], "Aegis")
        self.assertEqual(row["structure"], "Good")
        self.assertEqual(row["result"], "win")
        self.assertAlmostEqual(row["credit_received"], 1.5)
        self.assertAlmostEqual(row["max_loss"], 350.0)
        self.assertAlmostEqual(row["pnl"], 100.0)
        self.assertEqual(row["exit_type"], "Close")
        self.assertEqual(row["macro_flag"], "Minor")

    def test_performance_engine_computes_grouped_summaries_and_features(self):
        with self.app.app_context():
            engine = PerformanceEngine(self.app.extensions["trade_store"])
            trades = engine.load_trades()
            overall = engine.get_overall_performance()
            by_system = engine.get_performance_by_system()
            by_structure = engine.get_performance_by_structure()
            by_vix_bucket = engine.get_performance_by_vix_bucket()

        self.assertEqual(len(trades), 3)
        self.assertAlmostEqual(trades[0]["risk_efficiency"], trades[0]["credit_received"] / trades[0]["max_loss"], places=6)
        self.assertEqual(overall["total_trades"], 3)
        self.assertAlmostEqual(overall["win_rate"], 2 / 3, places=4)
        self.assertAlmostEqual(overall["total_pnl"], 180.0)
        self.assertEqual(by_system["Apollo"]["total_trades"], 1)
        self.assertEqual(by_system["Kairos"]["total_trades"], 1)
        self.assertEqual(by_system["Aegis"]["total_trades"], 1)
        self.assertEqual(by_structure["Good"]["total_trades"], 1)
        self.assertEqual(by_structure["Neutral"]["total_trades"], 1)
        self.assertEqual(by_structure["Poor"]["total_trades"], 1)
        self.assertEqual(by_vix_bucket["<18"]["total_trades"], 1)
        self.assertEqual(by_vix_bucket["22-26"]["total_trades"], 1)
        self.assertEqual(by_vix_bucket["26+"]["total_trades"], 1)

    def test_performance_summary_route_renders_simple_sections(self):
        response = self.client.get("/performance-summary")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Performance Summary", response.data)
        self.assertIn(b"Overall Stats", response.data)
        self.assertIn(b"By System", response.data)
        self.assertIn(b"By Structure", response.data)
        self.assertIn(b"By VIX Bucket", response.data)
        self.assertIn(b"Aegis", response.data)