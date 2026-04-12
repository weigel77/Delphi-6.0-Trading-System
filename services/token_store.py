"""Simple token storage helpers for Schwab OAuth."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


class JsonTokenStore:
    """Persist OAuth tokens to a small local JSON file."""

    def __init__(self, file_path: str) -> None:
        self.file_path = Path(file_path)

    def load(self) -> Optional[Dict[str, Any]]:
        """Load the stored token payload if it exists."""
        if not self.file_path.exists():
            return None

        try:
            return json.loads(self.file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def save(self, token_payload: Dict[str, Any]) -> None:
        """Persist the token payload to disk."""
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.write_text(json.dumps(token_payload, indent=2), encoding="utf-8")

    def clear(self) -> None:
        """Delete any persisted token file."""
        if self.file_path.exists():
            self.file_path.unlink()