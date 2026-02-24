"""
Custom exception hierarchy for operations.

All exceptions have:
- user_message: Safe to show to users
- internal_message: For logging/debugging
"""


class OperationError(Exception):
    """Base for all operation errors."""
    
    def __init__(self, user_message: str, internal_message: str | None = None):
        self.user_message = user_message
        self.internal_message = internal_message or user_message
        super().__init__(user_message)


class ValidationError(OperationError):
    """Request validation failed."""
    pass


class SecurityError(OperationError):
    """Security check failed (SQL injection, etc.)."""
    pass


class ConnectionError(OperationError):
    """Database connection failed."""
    pass


class TimeoutError(OperationError):
    """Operation timed out."""
    pass


class ThrottleError(OperationError):
    """Request rate limit exceeded for session."""
    pass


class AdmissionError(OperationError):
    """Global concurrent query limit reached."""
    pass


class NotFoundError(OperationError):
    """Requested record not found."""
    pass


class ConflictError(OperationError):
    """Resource conflict (duplicate key, version conflict, etc.)."""
    pass


class AuthenticationError(OperationError):
    """User authentication failed."""
    pass


class AuthorizationError(OperationError):
    """User lacks required permissions."""
    pass


class TokenExpiredError(AuthenticationError):
    """OBO token expired."""
    pass


class QueryExecutionError(OperationError):
    """Database query execution failed."""
    
    def __init__(
        self,
        user_message: str,
        internal_message: str | None = None,
        query: str | None = None,
    ):
        super().__init__(user_message, internal_message)
        # Truncate long queries for logging (never expose to user)
        self.query = query[:200] + "..." if query and len(query) > 200 else query


class MetadataAccessError(OperationError):
    """Failed to access table metadata."""
    
    def __init__(
        self,
        user_message: str,
        internal_message: str | None = None,
        table_ref: str | None = None,
    ):
        super().__init__(user_message, internal_message)
        self.table_ref = table_ref

