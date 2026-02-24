"""Global admission control gate.

Limits the number of concurrent database queries using a bounded semaphore.
Non-blocking: rejects immediately when at capacity.
"""

from __future__ import annotations

import threading


class AdmissionGate:
    """Non-blocking bounded semaphore for concurrent query admission."""

    def __init__(self, max_concurrent: int) -> None:
        self._semaphore = threading.BoundedSemaphore(max_concurrent)

    def try_acquire(self) -> bool:
        """Attempt to acquire a slot. Returns True if granted, False if at capacity."""
        return self._semaphore.acquire(blocking=False)

    def release(self) -> None:
        """Release a previously acquired slot."""
        self._semaphore.release()
