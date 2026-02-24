"""
Contracts package - Request/Response definitions.

This package contains:
- OperationType, ProcessingMode, OutputFormat, UIFormat enums
- OperationRequest dataclass
- OperationResponse dataclass
- FilterCondition, SortOrder models
- Custom exception hierarchy
- Immutability utilities
"""

from src.contracts.enums import (
    OperationType,
    ProcessingMode,
    OutputFormat,
    UIFormat,
)
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse
from src.contracts.models import FilterCondition, SortOrder
from src.contracts.immutable import freeze_mapping
from src.contracts.exceptions import (
    OperationError,
    ValidationError,
    SecurityError,
    ConnectionError,
    NotFoundError,
    AuthenticationError,
    AuthorizationError,
    TokenExpiredError,
    QueryExecutionError,
    MetadataAccessError,
)

__all__ = [
    "OperationType",
    "ProcessingMode",
    "OutputFormat",
    "UIFormat",
    "OperationRequest",
    "OperationResponse",
    "FilterCondition",
    "SortOrder",
    "freeze_mapping",
    "OperationError",
    "ValidationError",
    "SecurityError",
    "ConnectionError",
    "NotFoundError",
    "AuthenticationError",
    "AuthorizationError",
    "TokenExpiredError",
    "QueryExecutionError",
    "MetadataAccessError",
]



