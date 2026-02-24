"""src/logging/config.py

Application-wide logging configuration.

This config is idempotent and safe to call from Dash startup.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Literal

from src.infrastructure.config import get_settings
from src.logging.context import (
    get_correlation_id,
    get_user_email,
    get_scenario,
    get_phase,
    get_table,
    get_auth_method,
)

_logging_configured = False


class ContextFilter(logging.Filter):
    """Inject request context fields into every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if not hasattr(record, "correlation_id"):
            record.correlation_id = get_correlation_id()
        if not hasattr(record, "user_email"):
            record.user_email = get_user_email()
        if not hasattr(record, "scenario"):
            record.scenario = get_scenario().value
        if not hasattr(record, "phase"):
            ph = get_phase()
            record.phase = ph.value if ph else "-"
        if not hasattr(record, "table"):
            record.table = get_table()
        if not hasattr(record, "auth_method"):
            record.auth_method = get_auth_method()
        return True


def setup_logging(
    *,
    level: str | None = None,
    format_style: Literal["terminal", "json"] | None = None,
    log_file: str | None = None,
) -> None:
    """Configure root logging.

    Env-driven defaults:
    - LOG_LEVEL
    - LOG_FORMAT_STYLE
    - LOG_FILE
    """

    global _logging_configured
    if _logging_configured:
        return

    settings = get_settings()
    level = level or settings.log_level
    format_style = format_style or (settings.log_format_style or "terminal")
    log_file = log_file or settings.log_file

    numeric_level = getattr(logging, str(level).upper(), logging.INFO)

    if format_style == "json":
        log_format = (
            '{"timestamp":"%(asctime)s","level":"%(levelname)s",'
            '"correlation_id":"%(correlation_id)s","user":"%(user_email)s",'
            '"auth_method":"%(auth_method)s",'
            '"scenario":"%(scenario)s","phase":"%(phase)s","table":"%(table)s",'
            '"logger":"%(name)s","message":"%(message)s"}'
        )
    else:
        log_format = (
            "%(asctime)s %(levelname)-5s "
            "[%(correlation_id)s] [%(scenario)s] [%(phase)s] [%(auth_method)-5s] "
            "%(name)-28s %(message)s"
        )

    handlers: list[logging.Handler] = []

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(log_format))
    console.addFilter(ContextFilter())
    handlers.append(console)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setFormatter(logging.Formatter(log_format))
        fh.addFilter(ContextFilter())
        handlers.append(fh)

    logging.basicConfig(level=numeric_level, handlers=handlers, force=True)

    for noisy_logger in ["databricks", "urllib3", "asyncio", "werkzeug"]:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)

    _logging_configured = True


def is_configured() -> bool:
    return _logging_configured
