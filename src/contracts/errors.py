"""Typed error taxonomy utilities.

This module converts internal exceptions into user-safe, structured errors.
"""

from __future__ import annotations

from src.contracts.enums import ErrorCategory
from src.contracts.exceptions import (
    AdmissionError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    ConnectionError,
    NotFoundError,
    OperationError,
    SecurityError,
    ThrottleError,
    TimeoutError,
    ValidationError,
)
from src.contracts.responses import OperationErrorDetail


def error_detail_from_exception(exc: Exception) -> OperationErrorDetail:
    """Map an exception to a typed, user-safe error detail."""
    category = _category_from_exception(exc)

    if isinstance(exc, OperationError):
        message = exc.user_message
    else:
        message = f"System Error: {type(exc).__name__}"

    return OperationErrorDetail(
        category=category,
        code=type(exc).__name__,
        message=message,
        field=getattr(exc, "field", None),
    )


def unknown_error_detail(message: str | None = None) -> OperationErrorDetail:
    return OperationErrorDetail(
        category=ErrorCategory.UNKNOWN,
        code="UNKNOWN",
        message=message or "Operation failed",
    )


def _category_from_exception(exc: Exception) -> ErrorCategory:
    if isinstance(exc, ValidationError):
        return ErrorCategory.VALIDATION
    if isinstance(exc, SecurityError):
        return ErrorCategory.SECURITY
    if isinstance(exc, AuthorizationError):
        return ErrorCategory.AUTHORIZATION
    if isinstance(exc, AuthenticationError):
        return ErrorCategory.AUTHENTICATION
    if isinstance(exc, NotFoundError):
        return ErrorCategory.NOT_FOUND
    if isinstance(exc, ConflictError):
        return ErrorCategory.CONFLICT
    if isinstance(exc, ConnectionError):
        return ErrorCategory.CONNECTION
    if isinstance(exc, TimeoutError):
        return ErrorCategory.TIMEOUT
    if isinstance(exc, ThrottleError):
        return ErrorCategory.THROTTLE
    if isinstance(exc, AdmissionError):
        return ErrorCategory.ADMISSION
    return ErrorCategory.UNKNOWN
