import tempfile
import unittest
from pathlib import Path

from services.repositories.trade_repository import SQLiteTradeRepository, SupabaseTradeRepository
from services.runtime.supabase_integration import SupabaseConfig, SupabaseRequestError, SupabaseRuntimeContext
from services.trade_store import TradeStore


class InMemorySupabaseGateway:
    def __init__(self):
        self.tables = {
            "journal_trades": [],
            "journal_trade_close_events": [],
        }
        self.identities = {
            "journal_trades": 1,
            "journal_trade_close_events": 1,
        }
        self.select_calls = []
        self.rpc_calls = []

    def select(self, table, *, filters=None, order=None, limit=None, columns="*"):
        self.select_calls.append({"table": table, "filters": dict(filters or {}), "order": order, "limit": limit, "columns": columns})
        rows = [dict(row) for row in self.tables[table]]
        for key, expression in (filters or {}).items():
            operator, _, raw_value = str(expression).partition(".")
            rows = [row for row in rows if self._matches(row.get(key), operator, raw_value)]
        if order:
            for clause in reversed(order.split(",")):
                parts = clause.split(".")
                field = parts[0]
                direction = parts[1] if len(parts) > 1 else "asc"
                nulls_last = any(part == "nullslast" for part in parts[2:])
                rows.sort(
                    key=lambda row: self._sort_key(row.get(field), nulls_last=nulls_last),
                    reverse=direction == "desc",
                )
        if limit is not None:
            rows = rows[: int(limit)]
        return rows

    def insert(self, table, payload):
        row = dict(payload)
        row.setdefault("id", self.identities[table])
        self.identities[table] = max(self.identities[table], int(row["id"]) + 1)
        self.tables[table].append(row)
        return [dict(row)]

    def update(self, table, payload, *, filters):
        rows = self.select(table, filters=filters)
        updated = []
        selected_ids = {int(row["id"]) for row in rows}
        for index, row in enumerate(self.tables[table]):
            if int(row["id"]) not in selected_ids:
                continue
            self.tables[table][index] = {**row, **payload}
            updated.append(dict(self.tables[table][index]))
        return updated

    def delete(self, table, *, filters):
        rows = self.select(table, filters=filters)
        selected_ids = {int(row["id"]) for row in rows}
        self.tables[table] = [row for row in self.tables[table] if int(row["id"]) not in selected_ids]

    def rpc(self, function_name, payload=None):
        self.rpc_calls.append({"function_name": function_name, "payload": dict(payload or {})})
        if function_name == "sync_journal_trade_identity_sequence":
            max_id = max((int(row.get("id") or 0) for row in self.tables["journal_trades"]), default=0)
            self.identities["journal_trades"] = max(max_id + 1, 1)
            return max_id + 1 if max_id > 0 else 1
        if function_name == "sync_journal_trade_close_event_identity_sequence":
            max_id = max((int(row.get("id") or 0) for row in self.tables["journal_trade_close_events"]), default=0)
            self.identities["journal_trade_close_events"] = max(max_id + 1, 1)
            return max_id + 1 if max_id > 0 else 1
        raise AssertionError(f"Unexpected rpc call: {function_name}")

    @staticmethod
    def _matches(value, operator, raw_value):
        if operator == "eq":
            if value is None:
                return raw_value in {"", "null", "None"}
            return str(value) == raw_value
        if operator == "gte":
            if value is None:
                return False
            return str(value) >= raw_value
        if operator == "in":
            if value is None:
                return False
            options = {item.strip() for item in raw_value.strip("()").split(",") if item.strip()}
            return str(value) in options
        return False

    @staticmethod
    def _sort_key(value, *, nulls_last=False):
        if value is None and nulls_last:
            return (1, "")
        return (0, "" if value is None else str(value))


class MissingTableSupabaseGateway(InMemorySupabaseGateway):
    def __init__(self, *missing_tables):
        super().__init__()
        self.missing_tables = set(missing_tables)

    def select(self, table, *, filters=None, order=None, limit=None, columns="*"):
        if table in self.missing_tables:
            raise SupabaseRequestError(
                f'Supabase HTTP error 404: {{"code":"PGRST205","details":null,"hint":null,"message":"Could not find the table \'public.{table}\' in the schema cache"}}'
            )
        return super().select(table, filters=filters, order=order, limit=limit, columns=columns)


class DuplicateTradePrimaryKeyGateway(InMemorySupabaseGateway):
    def __init__(self):
        super().__init__()
        self._first_trade_insert = True

    def insert(self, table, payload):
        if table == "journal_trades" and self._first_trade_insert:
            self._first_trade_insert = False
            existing_id = self.identities[table]
            self.tables[table].append({"id": existing_id, "trade_mode": "real"})
            raise SupabaseRequestError(
                'Supabase HTTP error 409: {"code":"23505","details":"Key (id)=(1) already exists.","hint":null,"message":"duplicate key value violates unique constraint \"journal_trades_pkey\""}'
            )
        return super().insert(table, payload)


class DuplicateTradePrimaryKeyWithoutRpcGateway(DuplicateTradePrimaryKeyGateway):
    def rpc(self, function_name, payload=None):
        self.rpc_calls.append({"function_name": function_name, "payload": dict(payload or {})})
        raise SupabaseRequestError(
            'Supabase HTTP error 404: {"code":"PGRST202","details":"missing rpc","hint":null,"message":"Could not find the function public.sync_journal_trade_identity_sequence without parameters in the schema cache"}'
        )


class DuplicateCloseEventPrimaryKeyGateway(InMemorySupabaseGateway):
    def __init__(self):
        super().__init__()
        self._first_close_event_insert = True

    def insert(self, table, payload):
        if table == "journal_trade_close_events" and self._first_close_event_insert:
            self._first_close_event_insert = False
            existing_id = self.identities[table]
            self.tables[table].append({"id": existing_id, "trade_id": int(payload.get("trade_id") or 0)})
            raise SupabaseRequestError(
                'Supabase HTTP error 409: {"code":"23505","details":"Key (id)=(1) already exists.","hint":null,"message":"duplicate key value violates unique constraint \"journal_trade_close_events_pkey\""}'
            )
        return super().insert(table, payload)


class DuplicateCloseEventPrimaryKeyWithoutRpcGateway(DuplicateCloseEventPrimaryKeyGateway):
    def rpc(self, function_name, payload=None):
        self.rpc_calls.append({"function_name": function_name, "payload": dict(payload or {})})
        raise SupabaseRequestError(
            'Supabase HTTP error 404: {"code":"PGRST202","details":"missing rpc","hint":null,"message":"Could not find the function public.sync_journal_trade_close_event_identity_sequence without parameters in the schema cache"}'
        )


class SupabaseTradeRepositoryTest(unittest.TestCase):
    def _trade_payload(self):
        return {
            "trade_mode": "real",
            "system_name": "Apollo",
            "candidate_profile": "Standard",
            "system_version": "5.0",
            "status": "open",
            "trade_date": "2026-04-10",
            "entry_datetime": "2026-04-10T09:35",
            "expiration_date": "2026-04-10",
            "underlying_symbol": "SPX",
            "spx_at_entry": 6800.0,
            "vix_at_entry": 18.0,
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": 40.0,
            "expected_move_used": 40.0,
            "option_type": "Put Credit Spread",
            "short_strike": 6750.0,
            "long_strike": 6740.0,
            "spread_width": 10.0,
            "contracts": 2,
            "actual_entry_credit": 1.8,
            "distance_to_short": 50.0,
            "actual_distance_to_short": 50.0,
            "actual_em_multiple": 1.25,
            "fallback_used": "no",
            "fallback_rule_name": "",
            "notes_entry": "Apollo rationale",
        }

    def test_supabase_repository_matches_local_contract_for_create_reduce_expire_and_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "contract.db"
            local_backend = TradeStore(database_path)
            local_repository = SQLiteTradeRepository(local_backend)
            local_repository.initialize()

            context = SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            )
            remote_repository = SupabaseTradeRepository(
                context=context,
                gateway=InMemorySupabaseGateway(),
                database_path=database_path,
            )

            local_trade_id = local_repository.create_trade(self._trade_payload())
            remote_trade_id = remote_repository.create_trade(self._trade_payload())

            local_repository.reduce_trade(
                local_trade_id,
                {
                    "contracts_closed": 1,
                    "actual_exit_value": 0.55,
                    "event_datetime": "2026-04-10T10:15",
                    "close_method": "Reduce",
                    "close_reason": "Trimmed risk",
                },
            )
            remote_repository.reduce_trade(
                remote_trade_id,
                {
                    "contracts_closed": 1,
                    "actual_exit_value": 0.55,
                    "event_datetime": "2026-04-10T10:15",
                    "close_method": "Reduce",
                    "close_reason": "Trimmed risk",
                },
            )

            local_repository.expire_trade(local_trade_id, {"event_datetime": "2026-04-10T15:00"})
            remote_repository.expire_trade(remote_trade_id, {"event_datetime": "2026-04-10T15:00"})

            local_trade = local_repository.get_trade(local_trade_id)
            remote_trade = remote_repository.get_trade(remote_trade_id)
            self.assertEqual(local_trade["trade_number"], remote_trade["trade_number"])
            self.assertEqual(local_trade["derived_status_raw"], remote_trade["derived_status_raw"])
            self.assertEqual(local_trade["closed_contracts"], remote_trade["closed_contracts"])
            self.assertEqual(local_trade["remaining_contracts"], remote_trade["remaining_contracts"])
            self.assertEqual(len(local_trade["close_events"]), len(remote_trade["close_events"]))

            local_summary = local_repository.summarize("real")
            remote_summary = remote_repository.summarize("real")
            self.assertEqual(local_summary["total_trades"], remote_summary["total_trades"])
            self.assertEqual(local_summary["closed_trades"], remote_summary["closed_trades"])
            self.assertAlmostEqual(local_summary["total_pnl"], remote_summary["total_pnl"], places=6)

    def test_supabase_repository_finds_duplicate_trade_consistently(self):
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=InMemorySupabaseGateway(),
            database_path="instance/horme_trades.db",
        )

        repository.create_trade(self._trade_payload())
        duplicate = repository.find_duplicate_trade(self._trade_payload())
        recent_duplicate = repository.find_recent_duplicate(self._trade_payload(), window_seconds=60)

        self.assertIsNotNone(duplicate)
        self.assertIsNotNone(recent_duplicate)

    def test_supabase_repository_repairs_trade_identity_sequence_and_retries_insert(self):
        gateway = DuplicateTradePrimaryKeyGateway()
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=gateway,
            database_path="instance/horme_trades.db",
        )

        trade_id = repository.create_trade(self._trade_payload())

        self.assertEqual(trade_id, 2)
        self.assertEqual(len(gateway.rpc_calls), 1)
        self.assertEqual(gateway.rpc_calls[0]["function_name"], "sync_journal_trade_identity_sequence")

    def test_supabase_repository_falls_back_to_explicit_id_when_repair_rpc_is_unavailable(self):
        gateway = DuplicateTradePrimaryKeyWithoutRpcGateway()
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=gateway,
            database_path="instance/horme_trades.db",
        )

        trade_id = repository.create_trade(self._trade_payload())

        self.assertEqual(trade_id, 2)
        self.assertEqual(len(gateway.rpc_calls), 1)
        self.assertEqual(gateway.tables["journal_trades"][-1]["id"], 2)

    def test_supabase_repository_repairs_close_event_identity_sequence_and_retries_insert(self):
        gateway = DuplicateCloseEventPrimaryKeyGateway()
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=gateway,
            database_path="instance/horme_trades.db",
        )

        trade_id = repository.create_trade(self._trade_payload())
        repository.expire_trade(trade_id, {"event_datetime": "2026-04-10T15:00"})

        self.assertEqual(len(gateway.rpc_calls), 1)
        self.assertEqual(gateway.rpc_calls[0]["function_name"], "sync_journal_trade_close_event_identity_sequence")
        self.assertEqual(gateway.tables["journal_trade_close_events"][-1]["id"], 2)

    def test_supabase_repository_falls_back_to_explicit_close_event_id_when_repair_rpc_is_unavailable(self):
        gateway = DuplicateCloseEventPrimaryKeyWithoutRpcGateway()
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=gateway,
            database_path="instance/horme_trades.db",
        )

        trade_id = repository.create_trade(self._trade_payload())
        repository.expire_trade(trade_id, {"event_datetime": "2026-04-10T15:00"})

        self.assertEqual(len(gateway.rpc_calls), 1)
        self.assertEqual(gateway.tables["journal_trade_close_events"][-1]["id"], 2)

    def test_supabase_repository_returns_empty_reads_when_trade_tables_are_missing(self):
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=MissingTableSupabaseGateway("journal_trades", "journal_trade_close_events"),
            database_path="instance/horme_trades.db",
        )

        self.assertEqual(repository.next_trade_number(), 1)
        self.assertEqual(repository.list_trades("real"), [])
        self.assertIsNone(repository.get_trade(1))
        self.assertEqual(
            repository.summarize("real"),
            {
                "total_trades": 0,
                "open_trades": 0,
                "closed_trades": 0,
                "total_pnl": 0,
                "average_pnl": 0.0,
                "win_count": 0,
                "loss_count": 0,
            },
        )

    def test_supabase_repository_batches_close_event_loading_for_list_trades(self):
        gateway = InMemorySupabaseGateway()
        repository = SupabaseTradeRepository(
            context=SupabaseRuntimeContext(
                config=SupabaseConfig(
                    url="https://project.supabase.co",
                    publishable_key="publishable-key",
                    secret_key="secret-key",
                ),
                rest_url="https://project.supabase.co/rest/v1",
                auth_url="https://project.supabase.co/auth/v1",
                healthcheck_path="/auth/v1/settings",
                configured=True,
            ),
            gateway=gateway,
            database_path="instance/horme_trades.db",
        )

        first_trade_id = repository.create_trade(self._trade_payload())
        second_payload = self._trade_payload() | {
            "trade_date": "2026-04-11",
            "entry_datetime": "2026-04-11T09:35",
            "expiration_date": "2026-04-11",
            "short_strike": 6745.0,
            "long_strike": 6735.0,
        }
        second_trade_id = repository.create_trade(second_payload)
        repository.reduce_trade(
            first_trade_id,
            {
                "contracts_closed": 1,
                "actual_exit_value": 0.55,
                "event_datetime": "2026-04-10T10:15",
                "close_method": "Reduce",
                "close_reason": "Trimmed risk",
            },
        )
        repository.reduce_trade(
            second_trade_id,
            {
                "contracts_closed": 1,
                "actual_exit_value": 0.45,
                "event_datetime": "2026-04-11T11:00",
                "close_method": "Reduce",
                "close_reason": "Scaled out",
            },
        )
        gateway.select_calls.clear()

        trades = repository.list_trades("real")

        close_event_queries = [
            call
            for call in gateway.select_calls
            if call["table"] == "journal_trade_close_events"
        ]
        self.assertEqual(len(trades), 2)
        self.assertEqual(len(close_event_queries), 1)
        self.assertEqual(
            close_event_queries[0]["filters"].get("trade_id"),
            f"in.({first_trade_id},{second_trade_id})",
        )

