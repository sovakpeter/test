"""src/ui_logging/registry.py

Registry for UI log capture per UI session.

The session_id comes from the Dash browser session (client-side) or a
server-side generated token stored in dcc.Store.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Dict, Optional

from src.infrastructure.config import get_settings

from .buffer import UILogBuffer


@dataclass(slots=True)
class UISessionState:
    enabled: bool
    buffer: UILogBuffer


class UILogRegistry:
    def __init__(self, *, max_messages: int) -> None:
        self._max_messages = max_messages
        self._lock = Lock()
        self._sessions: Dict[str, UISessionState] = {}

    @property
    def max_messages(self) -> int:
        return self._max_messages

    def get_or_create(self, session_id: str, *, enabled_default: bool) -> UISessionState:
        with self._lock:
            existing = self._sessions.get(session_id)
            if existing is not None:
                return existing
            state = UISessionState(
                enabled=enabled_default,
                buffer=UILogBuffer(max_messages=self._max_messages),
            )
            self._sessions[session_id] = state
            return state

    def get(self, session_id: str) -> Optional[UISessionState]:
        with self._lock:
            return self._sessions.get(session_id)

    def set_enabled(self, session_id: str, enabled: bool, *, enabled_default: bool) -> UISessionState:
        state = self.get_or_create(session_id, enabled_default=enabled_default)
        with self._lock:
            state.enabled = enabled
            return state

    def clear(self, session_id: str) -> None:
        with self._lock:
            state = self._sessions.get(session_id)
        if state is not None:
            state.buffer.clear()


_registry: UILogRegistry | None = None


def get_registry() -> UILogRegistry:
    global _registry
    if _registry is None:
        settings = get_settings()
        _registry = UILogRegistry(max_messages=settings.ui_log_buffer_max_messages)
    return _registry
