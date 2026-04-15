import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from services.runtime.supabase_integration import SupabaseRequestError
from services.trade_history_migration import (
    TARGET_CLOSE_EVENTS_TABLE,
    TARGET_TRADES_TABLE,
    filter_trade_history_snapshot,
    load_trade_history_snapshot,
    sync_trade_history_snapshot,
    verify_trade_history_target,
)


class InMemoryGateway:
    def __init__(self):
        self.tables = {
            TARGET_TRADES_TABLE: [],
            TARGET_CLOSE_EVENTS_TABLE: [],
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
        selected_ids = {row["id"] for row in selected}
        updated = []
        for index, row in enumerate(self.tables[table]):
            if row.get("id") not in selected_ids:
                continue
            self.tables[table][index] = {**row, **payload}
            updated.append(dict(self.tables[table][index]))
        return updated

    def delete(self, table, *, filters):
        raise AssertionError("delete should not be called by trade history sync tests")


class MissingTableGateway(InMemoryGateway):
    def __init__(self, *missing_tables):
        super().__init__()
        self.missing_tables = set(missing_tables)

    def select(self, table, *, filters=None, order=None, limit=None, columns="*"):
        if table in self.missing_tables:
            raise SupabaseRequestError(
                f'Supabase HTTP error 404: {{"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table \'public.{table}\' in the schema cache"}}'
            )
        return super().select(table, filters=filters, order=order, limit=limit, columns=columns)


class TradeHistoryMigrationTest(unittest.TestCase):
    def _create_source_database_path(self) -> Path:
        file_descriptor, raw_path = tempfile.mkstemp(suffix=".db")
        os.close(file_descriptor)
        database_path = Path(raw_path)

        def _cleanup() -> None:
            try:
                database_path.unlink(missing_ok=True)
            except PermissionError:
                pass

        self.addCleanup(_cleanup)
        return database_path

    def _build_source_database(self, database_path: Path):
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
            connection.execute(
                """
                INSERT INTO trades (
                    id, trade_number, trade_mode, system_name, candidate_profile, pass_type,
                    notes_entry, status, trade_date, contracts, actual_entry_credit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    69,
                    69,
                    "real",
                    "Kairos",
                    "Standard",
                    "Strict Pass",
                    "Prefilled from Kairos Best Available candidate card.",
                    "open",
                    "2026-04-10",
                    1,
                    1.55,
                ),
            )
            connection.execute(
                """
                INSERT INTO trade_close_events (
                    id, trade_id, event_type, contracts_closed, actual_exit_value, event_datetime
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (72, 69, "reduce", 1, 0.55, "2026-04-10T10:15:00"),
            )

    def test_load_trade_history_snapshot_normalizes_kairos_best_available_to_subprime(self):
        database_path = self._create_source_database_path()
        self._build_source_database(database_path)

        snapshot = load_trade_history_snapshot(database_path)

        self.assertEqual(snapshot.source_path, database_path)
        self.assertEqual(len(snapshot.trades), 1)
        self.assertEqual(snapshot.trades[0]["candidate_profile"], "Subprime")
        self.assertEqual(len(snapshot.close_events), 1)

    def test_sync_trade_history_snapshot_inserts_and_updates_by_explicit_ids(self):
        database_path = self._create_source_database_path()
        self._build_source_database(database_path)
        snapshot = load_trade_history_snapshot(database_path)
        gateway = InMemoryGateway()

        first_result = sync_trade_history_snapshot(gateway, snapshot)
        second_result = sync_trade_history_snapshot(gateway, snapshot)

        self.assertEqual(first_result.inserted_trade_count, 1)
        self.assertEqual(first_result.inserted_close_event_count, 1)
        self.assertEqual(second_result.updated_trade_count, 1)
        self.assertEqual(second_result.updated_close_event_count, 1)
        self.assertEqual(gateway.tables[TARGET_TRADES_TABLE][0]["id"], 69)
        self.assertEqual(gateway.tables[TARGET_TRADES_TABLE][0]["candidate_profile"], "Subprime")
        self.assertEqual(gateway.tables[TARGET_CLOSE_EVENTS_TABLE][0]["id"], 72)

    def test_filter_trade_history_snapshot_keeps_only_requested_trade_numbers_and_close_events(self):
        database_path = self._create_source_database_path()
        self._build_source_database(database_path)
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                """
                INSERT INTO trades (
                    id, trade_number, trade_mode, system_name, candidate_profile, pass_type,
                    notes_entry, status, trade_date, contracts, actual_entry_credit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    70,
                    70,
                    "simulated",
                    "Apollo",
                    "Standard",
                    "Standard Pass",
                    "Prefilled from Apollo candidate card.",
                    "open",
                    "2026-04-11",
                    2,
                    1.75,
                ),
            )
            connection.execute(
                """
                INSERT INTO trade_close_events (
                    id, trade_id, event_type, contracts_closed, actual_exit_value, event_datetime
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (73, 70, "close", 2, 0.25, "2026-04-11T15:00:00"),
            )

        snapshot = load_trade_history_snapshot(database_path)
        filtered = filter_trade_history_snapshot(snapshot, trade_numbers={70})

        self.assertEqual([trade["trade_number"] for trade in filtered.trades], [70])
        self.assertEqual([event["trade_id"] for event in filtered.close_events], [70])

    def test_verify_trade_history_target_raises_clear_error_for_missing_tables(self):
        gateway = MissingTableGateway(TARGET_TRADES_TABLE)

        with self.assertRaisesRegex(RuntimeError, "Apply supabase/migrations/20260412_phase3_trade_persistence.sql"):
            verify_trade_history_target(gateway)