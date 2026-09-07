"""Share request spacing and server cooldowns within one process."""

from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Condition

_condition = Condition()
_next_request: dict[str, float] = {}


def retry_after_seconds(value: str | None) -> float | None:
    """Read Retry-After seconds or an HTTP date."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            stamp = parsedate_to_datetime(value)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            seconds = (stamp - datetime.now(timezone.utc)).total_seconds()
        except (ValueError, TypeError, OverflowError):
            return None
    return max(0.0, seconds) if math.isfinite(seconds) else None


def defer_host(host: str, seconds: float) -> None:
    """Apply a server cooldown to all helpers for this host."""
    with _condition:
        _next_request[host] = max(_next_request.get(host, 0.0), time.monotonic() + seconds)
        _condition.notify_all()


def wait_for_host(host: str, interval: float, max_wait: float) -> bool:
    """Reserve one request start, or refuse a wait beyond the caller's budget."""
    deadline = time.monotonic() + max_wait
    with _condition:
        while True:
            now = time.monotonic()
            ready = _next_request.get(host, 0.0)
            if ready > deadline:
                return False
            if ready <= now:
                _next_request[host] = now + interval
                return True
            _condition.wait(ready - now)
