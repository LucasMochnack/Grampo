"""
Thread-safe in-process TTL cache with stampede protection.

Used by dashboard endpoints to avoid recomputing the same heavy aggregation
multiple times in a short window — typical scenario: 3 managers all hitting
F5 at the same time should share a single computation.

Trade-off: results may be up to TTL seconds stale. For monitoring dashboards
this is acceptable (we already do meta-refresh every 600s anyway).

Stampede protection: a per-key lock ensures that when the cache is cold,
only ONE compute() runs at a time. Concurrent requests for the same key
wait for the first computation, then use the freshly cached result.
Without this, a burst of 20 requests on a cold cache would each open a DB
connection simultaneously and exhaust the connection pool.

Memory: each entry is held until either TTL expires or `invalidate()` is
called. Entries are evicted lazily on access; a background sweep can be
added later if memory grows.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Callable, Hashable

_lock = threading.Lock()
_store: dict[Hashable, tuple[float, Any]] = {}

# Per-key compute locks — prevent cache stampede / thundering herd.
# Multiple threads racing on the same cold key all acquire this lock;
# only the first one runs compute(), the rest use the freshly cached value.
_key_locks: dict[Hashable, threading.Lock] = {}
_key_locks_lock = threading.Lock()

# Stats for /health/deep observability
_hits = 0
_misses = 0


def _get_key_lock(key: Hashable) -> threading.Lock:
    """Return (creating if necessary) the per-key compute lock."""
    with _key_locks_lock:
        if key not in _key_locks:
            _key_locks[key] = threading.Lock()
        return _key_locks[key]


def cached(key: Hashable, ttl: float, compute: Callable[[], Any]) -> Any:
    """Return cached value for `key` if fresher than `ttl` seconds, else
    call `compute()`, store the result, and return it.

    `compute` is called with no arguments; capture the inputs you need
    via closure.

    Only one compute() runs per key at a time — concurrent misses wait for
    the first computation and then reuse the cached result (stampede-safe).
    """
    global _hits, _misses
    now = time.time()
    with _lock:
        entry = _store.get(key)
        if entry is not None and (now - entry[0]) < ttl:
            _hits += 1
            return entry[1]

    # Cache miss — serialize per-key to prevent stampede.
    key_lock = _get_key_lock(key)
    with key_lock:
        # Re-check after acquiring the key lock: a concurrent thread may
        # have just populated the cache while we were waiting.
        now = time.time()
        with _lock:
            entry = _store.get(key)
            if entry is not None and (now - entry[0]) < ttl:
                _hits += 1
                return entry[1]

        # Still a miss — we are the one thread that runs compute().
        value = compute()
        with _lock:
            _store[key] = (time.time(), value)
            _misses += 1
        return value


def invalidate(key: Hashable | None = None) -> None:
    """Drop one entry (or the entire cache if `key is None`)."""
    with _lock:
        if key is None:
            _store.clear()
        else:
            _store.pop(key, None)


def stats() -> dict[str, int]:
    """Return cache observability metrics."""
    with _lock:
        return {
            "entries": len(_store),
            "hits": _hits,
            "misses": _misses,
        }


def sweep_expired(max_age: float) -> int:
    """Remove entries older than `max_age` seconds. Returns count removed.
    Safe to call from a background task.
    """
    now = time.time()
    removed = 0
    with _lock:
        for key in list(_store.keys()):
            ts, _ = _store[key]
            if (now - ts) > max_age:
                del _store[key]
                removed += 1
    return removed
