# src/services/schema_resolver.py
"""
Schema Resolution Service.

Provides schema resolution for requests with columns="*".
Uses SchemaProvider (cached) for efficient lookups.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.contracts.enums import OperationType
from src.contracts.requests import OperationRequest
from src.infrastructure.logging import get_logger

if TYPE_CHECKING:
    from src.dispatcher.context import ExecutionContext
    from src.metadata.schema import SchemaProvider

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ResolvedSchema:
    """Result of schema resolution.
    
    Attributes:
        column_names: Ordered tuple of column names
        column_metadata: Tuple of column metadata dictionaries
        table_ref: Fully qualified table reference
        from_cache: Whether the result came from cache
    """
    column_names: tuple[str, ...]
    column_metadata: tuple[dict, ...]
    table_ref: str
    from_cache: bool


class SchemaResolver:
    """
    Resolves schema for requests needing column information.
    
    Used by lifecycle to auto-populate columns when columns="*".
    Leverages SchemaProvider for caching.
    """
    
    def __init__(self, provider: "SchemaProvider"):
        self._provider = provider
    
    def needs_resolution(self, request: OperationRequest) -> bool:
        """Check if request needs schema resolution.
        
        Returns True if:
        - Operation is READ
        - columns is None, ["*"], or contains only "*"
        """
        # Only READ operations with missing/wildcard columns need resolution
        if request.operation != OperationType.READ:
            return False
        
        columns = request.columns
        return (
            columns is None or
            columns == ["*"] or
            (isinstance(columns, list) and len(columns) == 1 and columns[0] == "*")
        )
    
    def resolve(self, request: OperationRequest, ctx: "ExecutionContext") -> ResolvedSchema:
        """
        Resolve schema for a table.
        
        Returns column names and metadata from information_schema.
        Results are cached by SchemaProvider.
        
        Args:
            request: The operation request with table reference
            ctx: Execution context (used for logging)
            
        Returns:
            ResolvedSchema with column names and metadata
            
        Raises:
            ValueError: If request has no table reference
        """
        if not request.table:
            raise ValueError("Cannot resolve schema without table reference")
        
        logger.debug(
            "Resolving schema for table=%s corr=%s",
            request.table,
            ctx.correlation_id,
        )
        
        # Use existing SchemaProvider (already has caching)
        table_schema = self._provider.get_table_schema(request.table)
        
        column_names = tuple(col.column_name for col in table_schema.columns)
        column_metadata = tuple(
            {
                "name": col.column_name,
                "type": col.data_type,
                "nullable": col.is_nullable,
                "is_primary_key": col.is_primary_key,
                "ordinal_position": col.ordinal_position,
            }
            for col in table_schema.columns
        )
        
        logger.debug(
            "Schema resolved: table=%s columns=%d corr=%s",
            request.table,
            len(column_names),
            ctx.correlation_id,
        )
        
        return ResolvedSchema(
            column_names=column_names,
            column_metadata=column_metadata,
            table_ref=table_schema.table_ref,
            from_cache=True,  # SchemaProvider handles cache transparency
        )
