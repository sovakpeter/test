"""src/logging

Structured logging subsystem.

Public API:
- setup_logging(): configure stdlib logging handlers/format
- get_logger(): get a StructuredLogger
"""

from src.logging.config import setup_logging
from src.logging.structured import StructuredLogger


def get_logger(name: str) -> StructuredLogger:
    return StructuredLogger(name)


__all__ = [
    "setup_logging",
    "get_logger",
    "StructuredLogger",
]
