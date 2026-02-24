"""Warehouse warmup gate.

Purpose:
- Mitigate Databricks SQL Warehouse cold-start latency by performing a lightweight
  warmup statement (default: SELECT 1) before the first real DB operation.

Guardrails alignment:
- Stores no user/request-derived data (no tokens, no table names).
- Concurrency-safe and bounded (TTL + lock).
- SP-only: warmup always uses token=None.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from src.infrastructure.config import get_settings
from src.infrastructure.logging import get_logger

logger = get_logger(__name__)


class WarehouseWarmupGate:
    """Concurrency-safe warmup gate with TTL and failure backoff."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_success_monotonic: float | None = None
        self._last_attempt_monotonic: float | None = None

    def maybe_warm(self, *, pool: Any, correlation_id: str | None = None) -> bool:
        """Warm the warehouse if TTL has elapsed.

        Returns:
            True if a warmup attempt was executed, False if skipped.
        """
        settings = get_settings()
        if not getattr(settings, "warehouse_warmup_enabled", True):
            return False

        ttl_seconds = int(getattr(settings, "warehouse_warmup_ttl_seconds", 600))
        failure_backoff_seconds = int(getattr(settings, "warehouse_warmup_failure_backoff_seconds", 30))
        warmup_sql = str(getattr(settings, "warehouse_warmup_sql", "SELECT 1"))

        now = time.monotonic()

        # Fast-path: recently successful.
        if self._last_success_monotonic is not None and (now - self._last_success_monotonic) < ttl_seconds:
            return False

        # Fast-path: recently attempted and failed.
        if self._last_success_monotonic is None and self._last_attempt_monotonic is not None:
            if (now - self._last_attempt_monotonic) < failure_backoff_seconds:
                return False

        with self._lock:
            now = time.monotonic()
            if self._last_success_monotonic is not None and (now - self._last_success_monotonic) < ttl_seconds:
                return False

            if self._last_success_monotonic is None and self._last_attempt_monotonic is not None:
                if (now - self._last_attempt_monotonic) < failure_backoff_seconds:
                    return False

            self._last_attempt_monotonic = now

            try:
                # SP-only by design: token=None. Do not use OBO tokens for warmup.
                # In production Databricks (no DATABRICKS_TOKEN env var), skip warmup
                # gracefully rather than fail - the actual handler will use OBO.
                import os
                if not os.environ.get("DATABRICKS_TOKEN"):
                    logger.debug(
                        "Warmup skipped: no DATABRICKS_TOKEN env var (OBO-only environment, corr=%s)",
                        correlation_id or "unknown",
                    )
                    return False
                
                with pool.get_connection(token=None) as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(warmup_sql)
                        try:
                            cursor.fetchall()
                        except Exception:
                            # Some backends/cursors may not support fetchall for non-SELECT.
                            pass

                self._last_success_monotonic = time.monotonic()
                logger.info(
                    "Warehouse warmup executed (corr=%s ttl=%ss)",
                    correlation_id or "unknown",
                    ttl_seconds,
                )
                return True

            except Exception as exc:
                # Best-effort only: do not fail the request pipeline.
                logger.warning(
                    "Warehouse warmup failed (corr=%s): %s",
                    correlation_id or "unknown",
                    type(exc).__name__,
                    exc_info=True,
                )
                return True
