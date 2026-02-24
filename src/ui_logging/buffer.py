"""src/ui_logging/buffer.py

In-memory, per-session ring buffer for UI log messages.

Designed to be thread-safe and cheap.
"""

from __future__ import annotations

from collections import deque
from threading import Lock
from typing import Deque, Iterable, List

from .models import UILogMessage


class UILogBuffer:
    def __init__(self, *, max_messages: int) -> None:
        if max_messages <= 0:
            raise ValueError("max_messages must be > 0")
        self._max_messages = max_messages
        self._lock = Lock()
        self._messages: Deque[UILogMessage] = deque(maxlen=max_messages)

    @property
    def max_messages(self) -> int:
        return self._max_messages

    def append(self, msg: UILogMessage) -> None:
        with self._lock:
            self._messages.append(msg)

    def clear(self) -> None:
        with self._lock:
            self._messages.clear()

    def snapshot(self) -> List[UILogMessage]:
        with self._lock:
            return list(self._messages)

    def tail(self, *, since_timestamp: float | None = None) -> List[UILogMessage]:
        with self._lock:
            if since_timestamp is None:
                return list(self._messages)
            return [m for m in self._messages if m.timestamp > since_timestamp]

    def extend(self, messages: Iterable[UILogMessage]) -> None:
        with self._lock:
            for m in messages:
                self._messages.append(m)
