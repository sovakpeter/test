"""
ExecutionContext - Request-scoped execution context.

Provides access to connection pool and request metadata for handlers.
"""

import uuid
from contextlib import contextmanager
from typing import Generator, Any

from src.contracts.requests import OperationRequest
from src.infrastructure.connection import ConnectionPool


class ExecutionContext:
    """
    Request-scoped execution context.
    
    Handlers receive this context to access:
    - Connection pool for database access
    - Original request for metadata
    - Transaction state (if applicable)
    - Correlation ID for request tracing
    - OBO token for user identity
    """
    
    def __init__(
        self,
        connection_pool: ConnectionPool,
        request: OperationRequest,
        correlation_id: str | None = None,
        obo_token: str | None = None,
        headers: dict[str, Any] | None = None,
    ):
        """
        Initialize execution context.
        
        Args:
            connection_pool: Pool for obtaining database connections.
            request: The original operation request.
            correlation_id: Unique ID for request tracing. Auto-generated if not provided.
            obo_token: OBO token for user identity (passed explicitly, not from globals).
            headers: HTTP request headers for auth/identity extraction.
        """
        self._pool = connection_pool
        self._request = request
        self._transaction_active = False
        self._correlation_id = correlation_id or str(uuid.uuid4())
        self._obo_token = obo_token
        self._headers = headers or {}
    
    @property
    def request(self) -> OperationRequest:
        """Get the original request."""
        return self._request
    
    @property
    def is_transaction(self) -> bool:
        """Check if a transaction is active."""
        return self._transaction_active
    
    @property
    def correlation_id(self) -> str:
        """Get correlation ID for request tracing (per guardrail §9.3)."""
        return self._correlation_id
    
    @property
    def obo_token(self) -> str | None:
        """Get OBO token for user identity."""
        return self._obo_token
    
    @property
    def pool(self) -> ConnectionPool:
        """Get the connection pool."""
        return self._pool

    @property
    def headers(self) -> dict[str, Any]:
        """Get HTTP request headers for auth/identity extraction."""
        return self._headers

    @contextmanager
    def get_connection(self, use_obo: bool = True) -> Generator[Any, None, None]:
        """
        Get a database connection from the pool.
        
        Args:
            use_obo: If True, use OBO token (for user-specific data).
                    If False, use Service Principal (for shared data).
                    
        Yields:
            Database connection.
        """
        token = self._obo_token if use_obo else None
        with self._pool.get_connection(token=token) as conn:
            yield conn
    
    def begin_transaction(self) -> None:
        """Mark transaction as active."""
        self._transaction_active = True
    
    def end_transaction(self) -> None:
        """Mark transaction as complete."""
        self._transaction_active = False
