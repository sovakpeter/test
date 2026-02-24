"""src/ui_logging/handler.py

Logging handler that captures LogRecords into a per-session UI buffer.

Routing is done via ContextVar `ui_session_id` from `src.logging.context`.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

from src.infrastructure.config import get_settings
from src.logging.context import get_ui_session_id

from .models import create_message_from_record
from .registry import get_registry


class UILogCaptureHandler(logging.Handler):
    def __init__(
        self,
        *,
        level: int | None = None,
        include_logger_prefixes: Optional[Iterable[str]] = None,
    ) -> None:
        # Resolve level from settings if not explicitly provided
        if level is None:
            settings = get_settings()
            level = getattr(logging, settings.ui_log_level, logging.INFO)
        super().__init__(level=level)
        self._include_prefixes = tuple(include_logger_prefixes or ("src", "pages", "dash_app"))

    def _should_capture(self, record: logging.LogRecord) -> bool:
        name = getattr(record, "name", "")
        if not name:
            return False
        return any(name == p or name.startswith(p + ".") for p in self._include_prefixes)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if not self._should_capture(record):
                return

            session_id = get_ui_session_id()
            if not session_id:
                return

            settings = get_settings()
            registry = get_registry()
            session = registry.get_or_create(session_id, enabled_default=settings.ui_log_enabled_default)
            if not session.enabled:
                return

            correlation_id = getattr(record, "correlation_id", "")
            user_email = getattr(record, "user_email", "")
            scenario = getattr(record, "scenario", "")
            phase = getattr(record, "phase", "")
            table = getattr(record, "table", "")
            msg = create_message_from_record(
                record,
                correlation_id=str(correlation_id),
                user_email=str(user_email),
                scenario=str(scenario),
                phase=str(phase),
                table=str(table),
            )
            session.buffer.append(msg)
        except Exception:
            # Never let UI capture break normal logging.
            self.handleError(record)


_installed = False


def install_ui_log_capture_handler() -> None:
    """Install the capture handler on the root logger once."""

    global _installed
    if _installed:
        return

    root = logging.getLogger()

    # Avoid double-install if hot-reloaded.
    for h in root.handlers:
        if isinstance(h, UILogCaptureHandler):
            _installed = True
            return

    root.addHandler(UILogCaptureHandler())
    _installed = True
