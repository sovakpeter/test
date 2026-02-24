# src/infrastructure/logging.py
"""src/infrastructure/logging.py

Compatibility shim for logging.

Historically, the codebase imported `get_logger()` from this module.
We now delegate configuration to `src.logging.config.setup_logging()`
so that application logs can be captured by the UI log viewer as well.
"""

import logging

from src.logging.config import is_configured
from src.logging.config import setup_logging as _setup_logging


def setup_logging(level: str | None = None) -> None:
    """
    Configure application-wide logging (idempotent).
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Falls back to settings.log_level if not provided.
    """
    # Delegate to the new config module (idempotent).
    _setup_logging(level=level)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger, ensuring logging is configured.
    
    Args:
        name: Logger name (typically __name__).
        
    Returns:
        Configured logger instance.
    """
    if not is_configured():
        setup_logging()
    return logging.getLogger(name)
