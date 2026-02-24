"""Per-session sliding-window rate limiter.

Thread-safe, zero external dependencies. Uses collections.deque per session
to track request timestamps within a configurable window.
"""

from __future__ import annotations

import threading
import time
from collections import deque


class SessionRateLimiter:
    """Sliding-window rate limiter keyed by UI session ID."""

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self._max_requests = max_requests
        self._window_seconds = window_seconds
        self._sessions: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def check(self, session_id: str | None) -> bool:
        """Return True if the request is allowed, False if rate-limited.

        A *None* session_id (e.g. background / heartbeat) is always allowed.
        """
        if session_id is None:
            return True

        now = time.monotonic()
        cutoff = now - self._window_seconds

        with self._lock:
            timestamps = self._sessions.get(session_id)
            if timestamps is None:
                timestamps = deque()
                self._sessions[session_id] = timestamps

            # Evict timestamps outside the window
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()

            if len(timestamps) >= self._max_requests:
                return False

            timestamps.append(now)
            return True

    def cleanup_stale(self, max_idle_seconds: float = 300.0) -> int:
        """Remove sessions with no activity within *max_idle_seconds*.

        Returns the number of sessions removed.
        """
        now = time.monotonic()
        cutoff = now - max_idle_seconds
        removed = 0

        with self._lock:
            stale_keys = [
                sid
                for sid, ts in self._sessions.items()
                if not ts or ts[-1] <= cutoff
            ]
            for sid in stale_keys:
                del self._sessions[sid]
                removed += 1

        return removed
