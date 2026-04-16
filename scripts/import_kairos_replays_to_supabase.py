"""Import Kairos replay bundles into the hosted Supabase simulator tape store."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from services.repositories.scenario_repository import SupabaseKairosScenarioRepository
from services.runtime.supabase_integration import SupabaseConfig, SupabaseProjectIntegration


def read_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env", help="Environment file containing Supabase URL and keys.")
    parser.add_argument(
        "--source-dir",
        default="instance/kairos_replays",
        help="Directory containing Kairos replay bundle JSON files.",
    )
    parser.add_argument(
        "--live-only",
        action="store_true",
        help="Import only live SPX tape bundles into the hosted simulator store.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Verify readable bundle count and hosted table availability without writing rows.")
    return parser


def iter_bundle_files(source_dir: Path):
    for path in sorted(source_dir.glob("*.json")):
        if path.name == "_kairos_catalog.json":
            continue
        yield path


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    env_values = read_env(Path(args.env_file))
    source_dir = Path(args.source_dir)
    integration = SupabaseProjectIntegration(
        SupabaseConfig(
            url=env_values.get("SUPABASE_URL", ""),
            publishable_key=env_values.get("SUPABASE_PUBLISHABLE_KEY", ""),
            secret_key=env_values.get("SUPABASE_SECRET_KEY", ""),
        )
    )
    repository = SupabaseKairosScenarioRepository(
        context=integration.initialize_context(),
        gateway=integration.create_table_gateway(),
    )

    # Verify the target table exists before counting or writing.
    repository.gateway.select(repository.TABLE, limit=1, columns="scenario_key")

    bundle_files = list(iter_bundle_files(source_dir))
    selected_files: list[Path] = []
    for path in bundle_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        template = payload.get("template") if isinstance(payload.get("template"), dict) else {}
        if args.live_only and str(template.get("source_type") or "") != "live_spx_tape":
            continue
        selected_files.append(path)

    if args.dry_run:
        print(f"Dry run verified hosted Kairos tape schema. Source bundles={len(selected_files)}.")
        return 0

    inserted_count = 0
    updated_count = 0
    for path in selected_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        template = payload.get("template") if isinstance(payload.get("template"), dict) else {}
        scenario_key = str(template.get("scenario_key") or path.stem)
        existing = repository.load_bundle_payload(scenario_key)
        repository.save_bundle(scenario_key, payload)
        if existing is None:
            inserted_count += 1
        else:
            updated_count += 1

    print(f"Imported Kairos replay bundles from {source_dir}.")
    print(f"Bundles: source={len(selected_files)}, inserted={inserted_count}, updated={updated_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())