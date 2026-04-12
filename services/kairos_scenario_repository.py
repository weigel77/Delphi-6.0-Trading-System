from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Tuple


def get_persistent_data_dir() -> Path:
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / "Horme"
    return Path.home() / ".horme"


class KairosScenarioRepository:
    """Durable file-backed repository for Kairos replay scenarios and recorded live tapes."""

    CATALOG_FILENAME = "_kairos_catalog.json"

    def __init__(self, storage_dir: str | Path, *, legacy_storage_dirs: List[str | Path] | None = None) -> None:
        self.storage_dir = Path(storage_dir)
        self.legacy_storage_dirs = [Path(item) for item in (legacy_storage_dirs or []) if item]

    def ensure_storage_dir(self) -> Path:
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self._migrate_legacy_bundles()
        return self.storage_dir

    def build_catalog_path(self) -> Path:
        return self.ensure_storage_dir() / self.CATALOG_FILENAME

    def build_bundle_path(self, scenario_key: str) -> Path:
        safe_name = str(scenario_key or "scenario").replace("/", "-").replace("\\", "-").strip().lower()
        return self.ensure_storage_dir() / f"{safe_name}.json"

    def save_bundle(self, scenario_key: str, payload: Dict[str, Any]) -> bool:
        path = self.build_bundle_path(scenario_key)
        existed = path.exists()
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self._upsert_catalog_entry(path, payload)
        return existed

    def load_bundle_payloads(self) -> List[Tuple[Path, Dict[str, Any]]]:
        storage_dir = self.ensure_storage_dir()
        bundles: List[Tuple[Path, Dict[str, Any]]] = []
        for path in sorted(storage_dir.glob("*.json")):
            if path.name == self.CATALOG_FILENAME:
                continue
            try:
                bundles.append((path, json.loads(path.read_text(encoding="utf-8"))))
            except (OSError, json.JSONDecodeError, TypeError, ValueError):
                continue
        self._rebuild_catalog_from_bundles(bundles)
        return bundles

    def load_bundle_payload(self, scenario_key: str) -> Dict[str, Any] | None:
        path = self.build_bundle_path(scenario_key)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return None

    def list_catalog_entries(self) -> List[Dict[str, Any]]:
        self.ensure_storage_dir()
        catalog_path = self.build_catalog_path()
        if not catalog_path.exists():
            self._rebuild_catalog_from_bundles(self.load_bundle_payloads())
        try:
            payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            payload = {"entries": []}
        entries = payload.get("entries") or []
        if not isinstance(entries, list):
            return []
        return [dict(item) for item in entries if isinstance(item, dict)]

    def _migrate_legacy_bundles(self) -> None:
        storage_dir = self.storage_dir
        for legacy_dir in self.legacy_storage_dirs:
            if legacy_dir.resolve() == storage_dir.resolve() or not legacy_dir.exists():
                continue
            for path in sorted(legacy_dir.glob("*.json")):
                if path.name == self.CATALOG_FILENAME:
                    continue
                target_path = storage_dir / path.name
                if target_path.exists():
                    continue
                try:
                    shutil.copy2(path, target_path)
                except OSError:
                    continue

    def _rebuild_catalog_from_bundles(self, bundles: List[Tuple[Path, Dict[str, Any]]]) -> None:
        entries_by_key: Dict[str, Dict[str, Any]] = {}
        for path, payload in bundles:
            entry = self._build_catalog_entry(path, payload)
            if entry is None:
                continue
            entries_by_key[str(entry["scenario_key"])] = entry
        catalog_path = self.build_catalog_path()
        catalog_payload = {
            "entries": sorted(
                entries_by_key.values(),
                key=lambda item: (str(item.get("session_date") or ""), str(item.get("label") or "")),
                reverse=True,
            )
        }
        catalog_path.write_text(json.dumps(catalog_payload, indent=2), encoding="utf-8")

    def _upsert_catalog_entry(self, path: Path, payload: Dict[str, Any]) -> None:
        entry = self._build_catalog_entry(path, payload)
        if entry is None:
            return
        entries_by_key = {item["scenario_key"]: item for item in self.list_catalog_entries() if item.get("scenario_key")}
        entries_by_key[entry["scenario_key"]] = entry
        catalog_path = self.build_catalog_path()
        catalog_path.write_text(
            json.dumps(
                {
                    "entries": sorted(
                        entries_by_key.values(),
                        key=lambda item: (str(item.get("session_date") or ""), str(item.get("label") or "")),
                        reverse=True,
                    )
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    @staticmethod
    def _build_catalog_entry(path: Path, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        template = payload.get("template") or {}
        scenario = payload.get("scenario") or {}
        scenario_key = str(template.get("scenario_key") or scenario.get("key") or "").strip().lower()
        if not scenario_key:
            return None
        return {
            "id": scenario_key,
            "scenario_key": scenario_key,
            "session_date": template.get("session_date"),
            "label": template.get("scenario_name") or scenario.get("name") or scenario_key,
            "source": template.get("source_label") or template.get("source_type") or "Unknown",
            "source_family_tag": template.get("source_family_tag") or "",
            "source_type": template.get("source_type") or "",
            "created_at": template.get("created_at") or "",
            "session_status": template.get("session_status") or "unknown",
            "bar_count": int(template.get("bar_count") or len(scenario.get("bars") or [])),
            "bundle_path": path.name,
        }