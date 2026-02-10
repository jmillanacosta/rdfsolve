"""In-memory cache with TTL support."""

from __future__ import annotations

import hashlib
import time
from typing import Any


class MemoryCache:
    """Simple in-memory cache with per-key TTL."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires, value = entry
        if time.time() > expires:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int = 300) -> None:
        self._store[key] = (time.time() + ttl, value)

    def invalidate(self, pattern: str = "") -> None:
        """Delete keys whose key starts with *pattern*."""
        keys = [k for k in self._store if k.startswith(pattern)]
        for k in keys:
            del self._store[k]


# Module-level singleton
cache = MemoryCache()


def cache_key(*parts: Any) -> str:
    """Build a short cache key from arbitrary parts."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
