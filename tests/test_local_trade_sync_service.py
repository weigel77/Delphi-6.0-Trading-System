import tempfile
import unittest
from pathlib import Path

from services.local_trade_sync_service import reconcile_trade_journal, sync_hosted_trade_journal_to_local, sync_local_trade_journal_to_hosted
from services.repositories.trade_repository import SQLiteTradeRepository, SupabaseTradeRepository
from services.runtime.supabase_integration import SupabaseConfig, SupabaseRuntimeContext
from services.trade_store import TradeStore
from tests.test_supabase_trade_repository import InMemorySupabaseGateway


class LocalTradeSyncServiceTest(unittest.TestCase):
    def _build_remote_repository(self):
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
        return SupabaseTradeRepository(
            context=context,
            gateway=InMemorySupabaseGateway(),
            database_path="supabase://journal_trades",
        )

    def _trade_payload(self, *, trade_number: int, trade_mode: str, system_name: str = "Apollo", journal_name: str = "Apollo Main"):
        strike_offset = trade_number % 10
        short_strike = 5350.0 + strike_offset
        long_strike = short_strike - 10.0
        minute_offset = 30 + (trade_number % 20)
        return {
            "trade_number": trade_number,
            "trade_mode": trade_mode,
            "system_name": system_name,
            "journal_name": journal_name,
            "candidate_profile": "Standard",
            "system_version": "6.4.1",
            "status": "open",
            "trade_date": "2026-04-17",
            "entry_datetime": f"2026-04-17T09:{minute_offset:02d}",
            "expiration_date": "2026-04-17",
            "underlying_symbol": "SPX",
            "spx_at_entry": 5400.0,
            "vix_at_entry": 19.2,
            "structure_grade": "Good",
            "macro_grade": "None",
            "expected_move": 35.0,
            "expected_move_used": 35.0,
            "option_type": "Put Credit Spread",
            "short_strike": short_strike,
            "long_strike": long_strike,
            "spread_width": 10.0,
            "contracts": 2,
            "actual_entry_credit": 1.75,
            "distance_to_short": round(50.0 - strike_offset, 2),
            "actual_distance_to_short": round(50.0 - strike_offset, 2),
            "actual_em_multiple": 1.4,
            "fallback_used": "no",
            "fallback_rule_name": "",
            "notes_entry": f"trade {trade_number}",
        }

    def test_sync_imports_missing_hosted_real_and_simulated_trades_without_duplication(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_backend = TradeStore(Path(temp_dir) / "local.db")
            local_repository = SQLiteTradeRepository(local_backend)
            local_repository.initialize()
            remote_repository = self._build_remote_repository()
            local_repository.create_trade(self._trade_payload(trade_number=72, trade_mode="talos"))

            existing_local_id = local_repository.create_trade(self._trade_payload(trade_number=70, trade_mode="real"))
            existing_remote_id = remote_repository.create_trade(self._trade_payload(trade_number=70, trade_mode="real"))
            remote_repository.update_trade(
                existing_remote_id,
                self._trade_payload(trade_number=70, trade_mode="real") | {
                    "close_events": [
                        {
                            "contracts_closed": 1,
                            "actual_exit_value": 0.65,
                            "event_datetime": "2026-04-17T13:00",
                            "close_method": "Reduce",
                            "notes_exit": "trim",
                        }
                    ]
                },
            )

            missing_real_id = remote_repository.create_trade(self._trade_payload(trade_number=92, trade_mode="real"))
            remote_repository.update_trade(
                missing_real_id,
                self._trade_payload(trade_number=92, trade_mode="real") | {
                    "close_events": [
                        {
                            "contracts_closed": 2,
                            "actual_exit_value": 0.0,
                            "event_datetime": "2026-04-17T15:00",
                            "close_method": "Expire",
                            "notes_exit": "expired",
                        }
                    ]
                },
            )
            remote_repository.create_trade(self._trade_payload(trade_number=93, trade_mode="simulated", system_name="Kairos", journal_name="Horme"))
            remote_repository.create_trade(self._trade_payload(trade_number=94, trade_mode="talos"))

            first_result = sync_hosted_trade_journal_to_local(
                hosted_repository=remote_repository,
                local_repository=local_repository,
            )
            second_result = sync_hosted_trade_journal_to_local(
                hosted_repository=remote_repository,
                local_repository=local_repository,
            )

            real_result = first_result.for_mode("real")
            simulated_result = first_result.for_mode("simulated")
            self.assertIsNotNone(real_result)
            self.assertIsNotNone(simulated_result)
            self.assertEqual(real_result.local_before_count, 1)
            self.assertEqual(real_result.hosted_count, 2)
            self.assertEqual(real_result.inserted_count, 1)
            self.assertEqual(list(real_result.imported_trade_numbers), [92])
            self.assertEqual(list(real_result.renumbered_trade_numbers), [])
            self.assertEqual(real_result.local_after_count, 2)
            self.assertEqual(simulated_result.local_before_count, 0)
            self.assertEqual(simulated_result.hosted_count, 1)
            self.assertEqual(simulated_result.inserted_count, 1)
            self.assertEqual(list(simulated_result.imported_trade_numbers), [93])
            self.assertEqual(list(simulated_result.renumbered_trade_numbers), [])
            self.assertEqual(simulated_result.local_after_count, 1)

            self.assertEqual(second_result.for_mode("real").inserted_count, 0)
            self.assertEqual(second_result.for_mode("simulated").inserted_count, 0)

            synced_real = local_repository.get_trade_by_number(92)
            self.assertIsNotNone(synced_real)
            self.assertEqual(synced_real["trade_mode"], "real")
            self.assertEqual(synced_real["derived_status_raw"], "expired")
            self.assertEqual(len(synced_real["close_events"]), 1)

            synced_simulated = local_repository.get_trade_by_number(93)
            self.assertIsNotNone(synced_simulated)
            self.assertEqual(synced_simulated["trade_mode"], "simulated")
            self.assertEqual(synced_simulated["journal_name"], "Horme")

            local_talos = local_repository.list_trades("talos")
            self.assertEqual(len(local_talos), 2)
            self.assertEqual([trade["trade_number"] for trade in local_talos], [94, 72])

            existing_local_trade = local_repository.get_trade(existing_local_id)
            self.assertEqual(existing_local_trade["trade_number"], 70)

    def test_sync_renumbers_when_remote_trade_number_collides_with_local_other_mode(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_backend = TradeStore(Path(temp_dir) / "collision.db")
            local_repository = SQLiteTradeRepository(local_backend)
            local_repository.initialize()
            remote_repository = self._build_remote_repository()

            local_repository.create_trade(self._trade_payload(trade_number=72, trade_mode="talos"))
            remote_repository.create_trade(self._trade_payload(trade_number=72, trade_mode="simulated", system_name="Kairos", journal_name="Horme"))

            result = sync_hosted_trade_journal_to_local(
                hosted_repository=remote_repository,
                local_repository=local_repository,
            )

            simulated_result = result.for_mode("simulated")
            self.assertEqual(simulated_result.inserted_count, 1)
            self.assertEqual(list(simulated_result.imported_trade_numbers), [72])
            self.assertEqual(list(simulated_result.renumbered_trade_numbers), [(72, 73)])

            synced_trade = local_repository.list_trades("simulated")[0]
            self.assertEqual(synced_trade["trade_number"], 72)
            self.assertEqual(synced_trade["journal_name"], "Horme")
            self.assertEqual(local_repository.list_trades("talos")[0]["trade_number"], 73)

    def test_sync_exports_missing_local_trades_to_hosted_and_renumbers_colliding_local_trade_numbers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_backend = TradeStore(Path(temp_dir) / "export.db")
            local_repository = SQLiteTradeRepository(local_backend)
            local_repository.initialize()
            remote_repository = self._build_remote_repository()

            remote_repository.create_trade(self._trade_payload(trade_number=73, trade_mode="real"))
            local_repository.create_trade(self._trade_payload(trade_number=73, trade_mode="talos", system_name="Kairos", journal_name="Horme"))
            local_repository.create_trade(self._trade_payload(trade_number=96, trade_mode="real"))

            result = sync_local_trade_journal_to_hosted(
                local_repository=local_repository,
                hosted_repository=remote_repository,
            )

            talos_result = result.for_mode("talos")
            real_result = result.for_mode("real")
            self.assertEqual(talos_result.inserted_count, 1)
            self.assertEqual(list(talos_result.exported_trade_numbers), [73])
            self.assertEqual(list(talos_result.renumbered_trade_numbers), [(73, 97)])
            self.assertEqual(real_result.inserted_count, 1)
            self.assertEqual(list(real_result.exported_trade_numbers), [96])

            local_talos = local_repository.list_trades("talos")
            self.assertEqual(local_talos[0]["trade_number"], 97)
            remote_talos = remote_repository.list_trades("talos")
            self.assertEqual(remote_talos[0]["trade_number"], 97)
            remote_real = remote_repository.get_trade_by_number(96)
            self.assertIsNotNone(remote_real)
            self.assertEqual(remote_real["trade_mode"], "real")

    def test_reconcile_trade_journal_handles_bidirectional_real_and_talos_parity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_backend = TradeStore(Path(temp_dir) / "reconcile.db")
            local_repository = SQLiteTradeRepository(local_backend)
            local_repository.initialize()
            remote_repository = self._build_remote_repository()

            remote_repository.create_trade(self._trade_payload(trade_number=74, trade_mode="real"))
            remote_repository.create_trade(self._trade_payload(trade_number=73, trade_mode="real"))
            local_repository.create_trade(self._trade_payload(trade_number=73, trade_mode="talos", journal_name="Horme"))
            local_repository.create_trade(self._trade_payload(trade_number=96, trade_mode="real"))
            local_repository.create_trade(self._trade_payload(trade_number=97, trade_mode="real"))

            result = reconcile_trade_journal(
                hosted_repository=remote_repository,
                local_repository=local_repository,
            )

            self.assertEqual(list(result.hosted_to_local.for_mode("real").imported_trade_numbers), [73, 74, 96, 97])
            self.assertEqual(list(result.hosted_to_local.for_mode("real").renumbered_trade_numbers), [])
            self.assertEqual(list(result.local_to_hosted.for_mode("real").exported_trade_numbers), [96, 97])
            self.assertEqual(list(result.local_to_hosted.for_mode("talos").exported_trade_numbers), [73])
            self.assertEqual(list(result.local_to_hosted.for_mode("talos").renumbered_trade_numbers), [(73, 98)])
            self.assertIsNotNone(remote_repository.get_trade_by_number(96))
            self.assertIsNotNone(remote_repository.get_trade_by_number(97))
            self.assertIsNotNone(remote_repository.get_trade_by_number(98))
            self.assertEqual(
                [trade["trade_number"] for trade in local_repository.list_trades("real")],
                [97, 96, 74, 73],
            )
            self.assertEqual(
                [trade["trade_number"] for trade in local_repository.list_trades("talos")],
                [98],
            )