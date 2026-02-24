"""
OperationManager - The single entry point for all operations.

UI calls execute() - nothing else. This is THE brain of the framework.
"""

from typing import Any

from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse
from src.contracts.errors import error_detail_from_exception
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import ConnectionPool
from src.contracts.exceptions import OperationError
from src.dispatcher.lifecycle import RequestLifecycle, new_correlation_id
from src.dispatcher.router import OperationRouter
from src.infrastructure.logging import get_logger
from src.infrastructure.auth import extract_obo_token

logger = get_logger(__name__)


class OperationManager:
    """
    THE SINGLE ENTRY POINT.
    
    UI calls execute() - nothing else.
    All routing, handler selection, connection management, and error handling
    is hidden inside this class.
    """
    
    def __init__(self, connection_pool: ConnectionPool | None = None):
        """
        Initialize the operation manager.
        
        Args:
            connection_pool: Optional connection pool. If not provided,
                           a default pool will be created.
        """
        from src.infrastructure.config import Settings
        self._pool = connection_pool or ConnectionPool(settings=Settings())
        self._router = OperationRouter()
        self._lifecycle = RequestLifecycle(router=self._router)
    
    def execute(
        self, 
        request: OperationRequest, 
        obo_token: str | None = None,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> OperationResponse:
        """
        Universal execution method.
        
        UI doesn't care what happens inside - just sends request, gets response.
        
        Args:
            request: The operation request.
            obo_token: Optional user identity token (for OBO).
            correlation_id: Optional request tracing ID.
            headers: Optional HTTP request headers for auth/identity extraction.
            
        Returns:
            OperationResponse with success flag and data or error message.
        """
        try:
            # Extract OBO token from headers if not explicitly provided
            # This enables user authorization (on-behalf-of) for Databricks Apps
            effective_obo_token = obo_token
            if effective_obo_token is None and headers:
                effective_obo_token = extract_obo_token(headers)
            
            ctx = ExecutionContext(
                connection_pool=self._pool,
                request=request,
                obo_token=effective_obo_token,
                correlation_id=correlation_id,
                headers=headers,
            )
            return self._lifecycle.process(request, ctx)

        except Exception as e:
            corr = correlation_id or new_correlation_id()
            logger.exception(
                "Operation failed before lifecycle (corr=%s op=%s mode=%s)",
                corr,
                getattr(getattr(request, "operation", None), "name", str(getattr(request, "operation", None))),
                getattr(getattr(request, "mode", None), "name", str(getattr(request, "mode", None))),
            )

            detail = error_detail_from_exception(e)

            return OperationResponse(
                success=False,
                message=detail.message,
                errors=[detail],
                metadata={"correlation_id": corr},
            )
