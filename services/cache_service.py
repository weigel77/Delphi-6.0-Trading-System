"""Simple in-memory caching helpers for market data requests."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
from typing import Any, Dict, Optional

import pandas as pd


@dataclass(frozen=True)
class CacheEntry:
    """Represents a cached payload and the time it was stored."""

    key: str
    payload: Any
    cached_at: datetime


class CacheService:
    """Manage an in-memory cache for market-data requests."""

    def __init__(self) -> None:
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = Lock()

    def build_cache_key(self, ticker: str, query_type: str, **params: Any) -> str:
        """Build a stable cache key using ticker, query type, and relevant inputs."""
        normalized_parts = [f"ticker={ticker}", f"query_type={query_type}"]
        for key in sorted(params):
            normalized_parts.append(f"{key}={params[key]}")
        return "|".join(normalized_parts)

    def get_cached_result(self, cache_key: str) -> Optional[CacheEntry]:
        """Return the cached entry if one exists."""
        with self._lock:
            return self._cache.get(cache_key)

    def set_cached_result(self, cache_key: str, payload: Any, cached_at: datetime) -> None:
        """Store a payload in the cache."""
        with self._lock:
            self._cache[cache_key] = CacheEntry(
                key=cache_key,
                payload=self.clone_payload(payload),
                cached_at=cached_at,
            )

    @staticmethod
    def is_cache_valid(entry: CacheEntry, ttl_seconds: int, now: datetime) -> bool:
        """Return whether the cache entry is still within its TTL window."""
        return CacheService.cache_age_seconds(entry, now) <= ttl_seconds

    @staticmethod
    def cache_age_seconds(entry: CacheEntry, now: datetime) -> int:
        """Return the age of a cache entry in whole seconds."""
        return max(0, int((now - entry.cached_at).total_seconds()))

    @staticmethod
    def clone_payload(payload: Any) -> Any:
        """Clone a payload so callers cannot mutate the cache in place."""
        if isinstance(payload, pd.DataFrame):
            clone = payload.copy(deep=True)
            clone.attrs = copy.deepcopy(payload.attrs)
            return clone

        return copy.deepcopy(payload)