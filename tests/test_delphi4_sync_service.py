import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path

from services.delphi4_sync_service import Delphi4SourcePaths, run_incremental_delphi4_sync
from services.kairos_scenario_repository import KairosScenarioRepository
from services.repositories.scenario_repository import SupabaseKairosScenarioRepository
from services.runtime.supabase_integration import SupabaseConfig, SupabaseRuntimeContext
from services.trade_history_migration import TARGET_CLOSE_EVENTS_TABLE, TARGET_TRADES_TABLE


class InMemoryGateway:
    def __init__(self):
        self.tables = {
            TARGET_TRADES_TABLE: [],
            TARGET_CLOSE_EVENTS_TABLE: [],
            "kairos_simulation_tapes": [],
            "hosted_runtime_state": [],
        }

    def select(self, table, *, filters=None, order=None, limit=None, columns="*"):
        rows = [dict(row) for row in self.tables[table]]
        if filters:
            for key, expression in filters.items():
                operator, _, raw_value = str(expression).partition(".")
                if operator == "eq":
                    rows = [row for row in rows if str(row.get(key)) == raw_value]
        if limit is not None:
            rows = rows[: int(limit)]
        if columns != "*":
            requested_columns = [column.strip() for column in str(columns).split(",")]
            rows = [{column: row.get(column) for column in requested_columns} for row in rows]
        return rows

    def insert(self, table, payload):
        self.tables[table].append(dict(payload))
        return [dict(payload)]

    def update(self, table, payload, *, filters):
        selected = self.select(table, filters=filters)
        for selected_row in selected:
            for index, row in enumerate(self.tables[table]):
                if row == selected_row:
                    self.tables[table][index] = {**row, **payload}
        return selected

    def delete(self, table, *, filters):
        raise AssertionError("delete should not be used in Delphi 4.3 sync tests")


class InMemoryStatusStore:
    def __init__(self):
        self.payloads = {}

    def load_object(self, object_key):
        payload = self.payloads.get(object_key)
        return dict(payload) if isinstance(payload, dict) else None

    def save_object(self, object_key, payload, *, expires_at=None):
        self.payloads[object_key] = dict(payload)


class Delphi4SyncServiceTest(unittest.TestCase):
    def setUp(self):
        self.workspace_root = Path(tempfile.mkdtemp())
        self.addCleanup(lambda: shutil.rmtree(self.workspace_root, ignore_errors=True))
        self.trade_database_path = self.workspace_root / "horme_trades.db"
        self.kairos_dir = self.workspace_root / "kairos_replays"
        self._build_trade_database(self.trade_database_path)
        self._build_kairos_source(self.kairos_dir)
        self.gateway = InMemoryGateway()
        self.status_store = InMemoryStatusStore()
        config = SupabaseConfig(
            url="https://project.supabase.co",
            publishable_key="publishable-key",
            secret_key="secret-key",
        )
        self.supabase_context = SupabaseRuntimeContext(
            config=config,
            rest_url="https://project.supabase.co/rest/v1",
            auth_url="https://project.supabase.co/auth/v1",
            healthcheck_path="/auth/v1/settings",
            configured=True,
        )
        self.kairos_repository = SupabaseKairosScenarioRepository(context=self.supabase_context, gateway=self.gateway)
        self.gateway.insert(TARGET_TRADES_TABLE, {"id": 69, "trade_number": 69, "status": "open"})
        self.gateway.insert(TARGET_CLOSE_EVENTS_TABLE, {"id": 72, "trade_id": 69, "event_type": "reduce"})
        self.kairos_repository.save_bundle(
            "live-spx-tape-2026-04-10",
            self._build_kairos_payload("live-spx-tape-2026-04-10", "2026-04-10"),
        )

    def test_dry_run_reports_only_missing_trades_close_events_and_live_tapes(self):
        summary = run_incremental_delphi4_sync(
            gateway=self.gateway,
            kairos_repository=self.kairos_repository,
            status_store=self.status_store,
            source_paths=Delphi4SourcePaths(
                source_root=self.workspace_root,
                trade_database_path=self.trade_database_path,
                kairos_replay_dir=self.kairos_dir,
            ),
            dry_run=True,
        )

        self.assertTrue(summary.dry_run)
        self.assertEqual(summary.new_trade_numbers, [70])
        self.assertEqual(summary.new_close_event_count, 1)
        self.assertEqual(summary.new_tape_session_dates, ["2026-04-11"])
        self.assertEqual(summary.new_tape_scenario_keys, ["live-spx-tape-2026-04-11"])
        self.assertEqual(len(self.gateway.tables[TARGET_TRADES_TABLE]), 1)
        self.assertEqual(len(self.gateway.tables[TARGET_CLOSE_EVENTS_TABLE]), 1)
        self.assertEqual(len(self.gateway.tables["kairos_simulation_tapes"]), 1)
        self.assertIsNone(self.status_store.load_object("latest"))

    def test_sync_inserts_only_missing_rows_and_records_last_success(self):
        summary = run_incremental_delphi4_sync(
            gateway=self.gateway,
            kairos_repository=self.kairos_repository,
            status_store=self.status_store,
            source_paths=Delphi4SourcePaths(
                source_root=self.workspace_root,
                trade_database_path=self.trade_database_path,
                kairos_replay_dir=self.kairos_dir,
            ),
            dry_run=False,
        )

        self.assertFalse(summary.dry_run)
        self.assertEqual([row["trade_number"] for row in self.gateway.tables[TARGET_TRADES_TABLE]], [69, 70])
        self.assertEqual([row["id"] for row in self.gateway.tables[TARGET_CLOSE_EVENTS_TABLE]], [72, 73])
        self.assertEqual(
            [row["scenario_key"] for row in self.gateway.tables["kairos_simulation_tapes"]],
            ["live-spx-tape-2026-04-10", "live-spx-tape-2026-04-11"],
        )
        latest_status = self.status_store.load_object("latest")
        self.assertIsNotNone(latest_status)
        self.assertEqual(latest_status["new_trade_numbers"], [70])
        self.assertEqual(latest_status["new_close_event_count"], 1)
        self.assertEqual(latest_status["new_tape_session_dates"], ["2026-04-11"])

    def _build_trade_database(self, database_path: Path) -> None:
        with sqlite3.connect(database_path) as connection:
            connection.executescript(
                """
                CREATE TABLE trades (
                    id INTEGER PRIMARY KEY,
                    trade_number INTEGER,
                    trade_mode TEXT,
                    system_name TEXT,
                    candidate_profile TEXT,
                    pass_type TEXT,
                    notes_entry TEXT,
                    status TEXT,
                    trade_date TEXT,
                    contracts INTEGER,
                    actual_entry_credit REAL
                );
                CREATE TABLE trade_close_events (
                    id INTEGER PRIMARY KEY,
                    trade_id INTEGER,
                    event_type TEXT,
                    contracts_closed INTEGER,
                    actual_exit_value REAL,
                    event_datetime TEXT
                );
                """
            )
            connection.executemany(
                """
                INSERT INTO trades (
                    id, trade_number, trade_mode, system_name, candidate_profile, pass_type,
                    notes_entry, status, trade_date, contracts, actual_entry_credit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (69, 69, "real", "Kairos", "Subprime", "Strict Pass", "Existing", "open", "2026-04-10", 1, 1.55),
                    (70, 70, "real", "Kairos", "Subprime", "Strict Pass", "New", "closed", "2026-04-11", 1, 1.40),
                ],
            )
            connection.executemany(
                """
                INSERT INTO trade_close_events (
                    id, trade_id, event_type, contracts_closed, actual_exit_value, event_datetime
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (72, 69, "reduce", 1, 0.55, "2026-04-10T10:15:00"),
                    (73, 70, "close", 1, 0.25, "2026-04-11T14:55:00"),
                ],
            )

    def _build_kairos_source(self, kairos_dir: Path) -> None:
        repository = KairosScenarioRepository(kairos_dir)
        repository.save_bundle(
            "live-spx-tape-2026-04-10",
            self._build_kairos_payload("live-spx-tape-2026-04-10", "2026-04-10"),
        )
        repository.save_bundle(
            "live-spx-tape-2026-04-11",
            self._build_kairos_payload("live-spx-tape-2026-04-11", "2026-04-11"),
        )
        repository.save_bundle(
            "manual-template-2026-04-11",
            self._build_kairos_payload("manual-template-2026-04-11", "2026-04-11", source_type="manual_import"),
        )

    def _build_kairos_payload(self, scenario_key: str, session_date: str, *, source_type: str = "live_spx_tape"):
        return {
            "template": {
                "scenario_key": scenario_key,
                "scenario_name": scenario_key,
                "session_date": session_date,
                "source_type": source_type,
                "source_label": "Live SPX Tape" if source_type == "live_spx_tape" else "Manual",
                "source_family_tag": "live",
                "created_at": f"{session_date}T15:59:00",
                "session_status": "closed",
                "bar_count": 2,
            },
            "scenario": {
                "key": scenario_key,
                "name": scenario_key,
                "bars": [{"timestamp": f"{session_date}T09:30:00"}, {"timestamp": f"{session_date}T09:31:00"}],
            },
        }


if __name__ == "__main__":
    unittest.main()