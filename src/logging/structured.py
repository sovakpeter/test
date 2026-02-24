"""src/logging/structured.py

A thin wrapper over stdlib logging that provides convenient helpers
(request boundaries, phase separators, SQL redaction, intent logging).
"""

from __future__ import annotations

import logging
import re
from typing import Any, TYPE_CHECKING

from src.logging.context import LifecyclePhase, OperationScenario

if TYPE_CHECKING:
    from src.contracts.requests import OperationRequest

# Patterns for detecting sensitive keys in payloads
_SENSITIVE_PATTERNS = re.compile(
    r"(token|password|secret|key|credential|auth|api_key|apikey|access_token|refresh_token)",
    re.IGNORECASE
)


def _redact_sensitive(data: dict[str, Any] | None) -> dict[str, Any] | None:
    """Redact values for keys that look like secrets."""
    if data is None:
        return None
    redacted = {}
    for k, v in data.items():
        if _SENSITIVE_PATTERNS.search(k):
            redacted[k] = "***REDACTED***"
        elif isinstance(v, dict):
            redacted[k] = _redact_sensitive(v)
        else:
            redacted[k] = v
    return redacted


class StructuredLogger:
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
        self._name = name

    def debug(self, message: str, **kwargs: Any) -> None:
        self._logger.debug(self._format(message, **kwargs))

    def info(self, message: str, **kwargs: Any) -> None:
        self._logger.info(self._format(message, **kwargs))

    def warning(self, message: str, **kwargs: Any) -> None:
        self._logger.warning(self._format(message, **kwargs))

    def error(self, message: str, **kwargs: Any) -> None:
        self._logger.error(self._format(message, **kwargs))

    def exception(self, message: str, **kwargs: Any) -> None:
        self._logger.exception(self._format(message, **kwargs))

    @staticmethod
    def _format(message: str, **kwargs: Any) -> str:
        if not kwargs:
            return message
        extras = " | ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"{message} | {extras}"

    # ---------------------------------------------------------------------
    # Visual separators
    # ---------------------------------------------------------------------

    def request_start(
        self,
        *,
        correlation_id: str,
        scenario: OperationScenario,
        table: str = "",
        user_email: str | None = None,
    ) -> None:
        border = "═" * 70
        who = user_email or "anonymous"
        self._logger.info(border)
        self._logger.info(f"REQUEST: {correlation_id} | {who} | {scenario.value} | START")
        if table:
            self._logger.info(f"Table: {table}")
        self._logger.info(border)

    def request_end(
        self,
        *,
        correlation_id: str,
        success: bool,
        duration_ms: float,
        affected_rows: int | None = None,
    ) -> None:
        border = "═" * 70
        status = "COMPLETED" if success else "FAILED"
        rows = affected_rows if affected_rows is not None else 0
        self._logger.info(border)
        self._logger.info(
            f"REQUEST: {correlation_id} | {status} | success={success} | {duration_ms:.1f}ms | rows={rows}"
        )
        self._logger.info(border)

    def phase_header(self, phase: LifecyclePhase) -> None:
        border = "─" * 20
        self._logger.info(f"{border} PHASE: {phase.value} {border}")

    def phase_summary(self, phase: LifecyclePhase, duration_ms: float, status: str = "OK") -> None:
        border = "─" * 18
        self._logger.info(f"{border} PHASE: {phase.value} | {status} | {duration_ms:.1f}ms {border}")

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------

    def log_sql(self, sql: str, params: dict[str, Any] | None = None) -> None:
        sql_display = sql[:500] + "..." if len(sql) > 500 else sql
        self.debug(f"SQL: {sql_display}")
        if params:
            safe_params: dict[str, Any] = {}
            for k, v in params.items():
                safe_params[k] = "***" if "token" in k.lower() else v
            self.debug(f"PARAMS: {safe_params}")

    # ---------------------------------------------------------------------
    # Intent & SQL Execution Logging
    # ---------------------------------------------------------------------

    def log_intent(
        self,
        request: "OperationRequest",
        correlation_id: str,
    ) -> None:
        """
        Log the full UI intention message (OperationRequest).
        
        Redacts sensitive values in payload and where clauses.
        """
        from src.infrastructure.config import get_settings
        
        settings = get_settings()
        if not settings.log_intent_enabled:
            return
        
        border = "┈" * 70
        self._logger.debug(border)
        self._logger.debug(f"INTENT: {correlation_id}")
        self._logger.debug(f"  operation: {request.operation.name}")
        self._logger.debug(f"  mode: {request.mode.name}")
        self._logger.debug(f"  table: {request.table}")
        
        if request.columns:
            self._logger.debug(f"  columns: {request.columns}")
        
        if request.where:
            redacted_where = _redact_sensitive(request.where)
            self._logger.debug(f"  where: {redacted_where}")
        
        if request.payload:
            if isinstance(request.payload, dict):
                redacted_payload = _redact_sensitive(request.payload)
                self._logger.debug(f"  payload: {redacted_payload}")
            elif isinstance(request.payload, list) and request.payload:
                self._logger.debug(f"  payload: [{len(request.payload)} records]")
        
        if request.options:
            self._logger.debug(f"  options: {request.options}")
        
        self._logger.debug(f"  data_format: {request.data_format.name}")
        self._logger.debug(f"  ui_format: {request.ui_format.name}")
        
        if request.scenario:
            self._logger.debug(f"  scenario: {request.scenario}")
        
        if request.operations:
            self._logger.debug(f"  operations: [{len(request.operations)} sub-ops]")
        
        self._logger.debug(border)

    def log_sql_execution(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        query_type: str = "DYNAMIC",
        query_name: str | None = None,
        max_length: int | None = None,
    ) -> None:
        """
        Log SQL execution details.
        
        Args:
            sql: The SQL statement
            params: Query parameters
            query_type: "DYNAMIC", "NAMED", or "MUTATION"
            query_name: For named queries, the manifest key
            max_length: Override for max SQL length (uses settings default if None)
        """
        from src.infrastructure.config import get_settings
        
        settings = get_settings()
        if not settings.log_sql_enabled:
            return
        
        if max_length is None:
            max_length = settings.sql_log_max_length
        
        border = "─" * 20
        header = f"{border} SQL: {query_type} {border}"
        
        self._logger.debug(header)
        
        if query_type == "NAMED" and query_name:
            # For named queries, just log name + params (not full SQL)
            self._logger.debug(f"Query: {query_name}")
        else:
            # For dynamic/mutation queries, log SQL with truncation
            if len(sql) > max_length:
                sql_display = sql[:max_length] + f" ... [TRUNCATED at {max_length} chars]"
            else:
                sql_display = sql
            self._logger.debug(f"SQL: {sql_display}")
        
        if params:
            redacted_params = _redact_sensitive(params)
            self._logger.debug(f"Parameters: {redacted_params}")
        
        self._logger.debug("─" * (len(header)))
