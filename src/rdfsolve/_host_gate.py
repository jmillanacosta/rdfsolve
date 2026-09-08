"""Limit each remote host to one active request across cooperating processes."""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

_guard = Lock()
_locks: dict[str, Lock] = {}


class HostBusyError(TimeoutError):
    """The host request slot was not available within the wait budget."""


@contextmanager
def host_request(host: str, *, timeout: float, interval: float = 1.0) -> Iterator[None]:
    """Hold a host slot until the response is consumed or closed.

    POSIX processes coordinate through a shared lock directory. Set
    RDFSOLVE_HTTP_LOCK_DIR to the same shared path on all cluster nodes.
    Local QLever requests do not use this remote-host gate.
    """
    if host in {"localhost", "127.0.0.1", "::1"}:
        yield
        return
    with _guard:
        lock = _locks.setdefault(host.lower(), Lock())
    deadline = time.monotonic() + timeout
    if not lock.acquire(timeout=max(0, timeout)):
        raise HostBusyError(f"Host request slot is busy: {host}")
    try:
        from rdfsolve._http_policy import wait_for_host

        if not wait_for_host(host, max(1.0, interval), max(0.0, deadline - time.monotonic())):
            raise HostBusyError(f"Host cooldown exceeds wait budget: {host}")
        directory = os.environ.get("RDFSOLVE_HTTP_LOCK_DIR")
        if not directory:
            yield
            return
        if os.name != "posix":
            raise RuntimeError("Shared HTTP file locking requires POSIX")
        import fcntl

        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        path = root / (hashlib.sha256(host.lower().encode()).hexdigest() + ".lock")
        with path.open("a+") as stream:
            while True:
                try:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise HostBusyError(f"Host request slot is busy: {host}") from None
                    time.sleep(0.1)
            try:
                stream.seek(0)
                text = stream.read().strip()
                ready = float(text) if text else 0.0
                wait = max(0.0, ready - time.time())
                if wait > max(0.0, deadline - time.monotonic()):
                    raise HostBusyError(f"Host cooldown exceeds wait budget: {host}")
                if wait:
                    time.sleep(wait)
                yield
            finally:
                stream.seek(0)
                stream.truncate()
                from rdfsolve._http_policy import remaining_host_delay

                stream.write(str(time.time() + max(1.0, interval, remaining_host_delay(host))))
                stream.flush()
                fcntl.flock(stream, fcntl.LOCK_UN)
    finally:
        lock.release()
