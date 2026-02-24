# src/handlers/base.py
"""
BaseHandler - Abstract base class for all handlers.

All handlers must implement the 'handle' method to process requests
statelessly using the provided ExecutionContext.

Handlers return either:
- QueryResult: For read operations (lifecycle converts format)
- OperationResponse: For mutations (no format conversion needed)
"""

from abc import ABC, abstractmethod
from typing import Any

from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.contracts.errors import error_detail_from_exception
from src.dispatcher.context import ExecutionContext
from src.infrastructure.logging import get_logger
from src.contracts.exceptions import OperationError

logger = get_logger(__name__)


class BaseHandler(ABC):
    """
    Abstract base class for all operation handlers.
    
    Enforces the Command Pattern interface.
    Handlers must be stateless.
    
    Return types:
    - QueryResult: For read operations (centralized format conversion in lifecycle)
    - OperationResponse: For mutations (affected_rows, no format conversion)
    """

    @abstractmethod
    def handle(
        self,
        ctx: ExecutionContext,
        request: OperationRequest
    ) -> QueryResult | OperationResponse:
        """
        Execute the operation.
        
        Args:
            ctx: Execution context (connection, user info, request ID)
            request: The operation request payload
            
        Returns:
            QueryResult: For read operations (data in native format)
            OperationResponse: For mutations (affected_rows)
        """
        pass
        
    def _log_start(self, ctx: ExecutionContext, request: OperationRequest, op_name: str) -> None:
        """Helper to log operation start (sanitized)."""
        logger.info(
            "Handler: Starting %s for table=%s mode=%s corr=%s",
            op_name,
            request.table,
            getattr(request.mode, "name", str(request.mode)),
            ctx.correlation_id or "unknown",
        )

    def _log_success(self, op_name: str, details: str = "") -> None:
        """Helper to log operation success."""
        logger.info(f"Handler: {op_name} succeeded. {details}")

    def _log_error(self, op_name: str, error: Exception) -> None:
        """Helper to log operation failure."""
        logger.error(f"Handler: {op_name} failed: {error}", exc_info=True)

    def _handle_error(self, ctx: ExecutionContext, op_name: str, error: Exception) -> OperationResponse:
        """
        Standardized error handling.
        
        Logs full details but returns safe message to UI.
        Returns OperationResponse for error cases (bypasses format conversion).
        """
        self._log_error(op_name, error)

        detail = error_detail_from_exception(error)

        return OperationResponse(
            success=False,
            message=detail.message,
            errors=[detail],
            metadata={"correlation_id": ctx.correlation_id} if ctx.correlation_id else {},
        )
