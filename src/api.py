"""
PUBLIC API - This is what UI imports.
Everything else is internal implementation detail.

The single entry point is `execute()` which accepts an OperationRequest
and returns an OperationResponse.

Usage:
    from src.api import execute, OperationRequest, OperationType, ProcessingMode
    
    response = execute(OperationRequest(
        operation=OperationType.READ,
        table="catalog.schema.users",
        mode=ProcessingMode.SINGLE,
        where={"id": 123}
    ))
    
    if response.success:
        print(response.data)
    else:
        print(response.message)
"""

from src.contracts.enums import OperationType, ProcessingMode
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse
from src.dispatcher.manager import OperationManager
from typing import Any

# Singleton instance
_manager: OperationManager | None = None


def get_manager() -> OperationManager:
    """Get or create the OperationManager singleton."""
    global _manager
    if _manager is None:
        _manager = OperationManager()
    return _manager


def execute(
    request: OperationRequest, 
    obo_token: str | None = None,
    correlation_id: str | None = None,
    headers: dict[str, Any] | None = None,
) -> OperationResponse:
    """
    THE ONLY FUNCTION UI EVER CALLS.
    
    Args:
        request: The operation request specifying what to do.
        obo_token: Optional OBO token for user identity.
        correlation_id: Optional correlation ID for tracing.
        headers: Optional HTTP request headers for auth/identity extraction.
        
    Returns:
        OperationResponse with success flag, data, and any error messages.
    """
    return get_manager().execute(request, obo_token, correlation_id, headers)


# Re-export for convenience
__all__ = [
    "execute",
    "OperationRequest",
    "OperationResponse",
    "OperationType",
    "ProcessingMode",
]
