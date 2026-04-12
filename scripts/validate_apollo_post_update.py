from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MOCK_MODULES = [
    "tests.test_schwab_option_chain_params",
    "tests.test_apollo_candidate_service",
    "tests.test_apollo_reliability",
    "tests.test_apollo_dev_routes",
    "tests.test_trade_routes",
]
LIVE_MODULE = "tests.test_apollo_live_smoke"


def run_modules(modules: list[str]) -> int:
    command = [sys.executable, "-m", "unittest", *modules]
    return subprocess.run(command, cwd=PROJECT_ROOT).returncode


def main() -> int:
    print("[Apollo] Running mock regression gate...")
    mock_exit = run_modules(MOCK_MODULES)
    if mock_exit != 0:
        print("[Apollo] Mock regression gate failed.")
        return mock_exit

    require_live = str(os.getenv("APOLLO_LIVE_SMOKE", "")).strip().lower() in {"1", "true", "yes", "on"}
    print("[Apollo] Running live smoke gate..." + (" required" if require_live else " optional"))
    live_exit = run_modules([LIVE_MODULE])
    if live_exit != 0 and require_live:
        print("[Apollo] Live smoke gate failed.")
        return live_exit

    if live_exit != 0:
        print("[Apollo] Live smoke gate skipped or unavailable.")
    elif require_live:
        print("[Apollo] Live smoke gate passed.")
    else:
        print("[Apollo] Live smoke gate completed.")

    print("[Apollo] Post-update validation completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
