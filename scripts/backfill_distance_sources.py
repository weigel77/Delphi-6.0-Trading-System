import json
import sys
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import create_app


def summarize_safety(payload: Dict[str, Any]) -> Dict[str, Any]:
    safety = (((payload or {}).get("learning") or {}).get("safety_optimization") or {})
    adaptive = (((payload or {}).get("learning") or {}).get("adaptive_safety_guidance") or {})
    return {
        "closed_trade_count": safety.get("closed_trade_count", 0),
        "eligible_trade_count": safety.get("eligible_trade_count", 0),
        "excluded_trade_count": safety.get("excluded_trade_count", 0),
        "distance_source_counts": safety.get("distance_source_counts", {}),
        "exclusion_summary": safety.get("exclusion_summary", {}),
        "overall_best_by_expectancy": safety.get("overall_best_by_expectancy"),
        "overall_best_by_win_rate": safety.get("overall_best_by_win_rate"),
        "vix_bucket_results": adaptive.get("by_vix_bucket", []),
        "recommendation": adaptive.get("recommendation", {}),
    }


def main() -> None:
    app = create_app()
    with app.app_context():
        performance_service = app.extensions["performance_service"]
        trade_store = app.extensions["trade_store"]

        before_payload = performance_service.build_dashboard()
        backfill_report = trade_store.backfill_distance_sources()
        after_payload = performance_service.build_dashboard()

    print(
        json.dumps(
            {
                "before": summarize_safety(before_payload),
                "backfill": backfill_report,
                "after": summarize_safety(after_payload),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()