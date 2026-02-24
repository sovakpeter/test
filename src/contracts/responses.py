"""
OperationResponse - Universal response object.
QueryResult - Internal result structure for handlers.

UI only receives OperationResponse from operations.
Handlers produce QueryResult which lifecycle shapes into OperationResponse.
"""

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from src.contracts.enums import ErrorCategory, DataFormat

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


@dataclass(frozen=True, slots=True)
class OperationErrorDetail:
    """Typed error details returned to the UI."""

    category: ErrorCategory
    code: str
    message: str
    field: str | None = None


@dataclass(frozen=True, slots=True)
class ColumnMetadata:
    """Rich column metadata for UI rendering.
    
    Provides type information for smarter UI column rendering,
    independent of the data format used.
    """
    name: str
    data_type: str = "STRING"
    nullable: bool = True
    is_primary_key: bool = False
    ordinal_position: int = 0

@dataclass(frozen=True, slots=True)
class QueryResult:
    """
    Internal result structure produced by handlers.
    
    Handlers return this; lifecycle converts to OperationResponse.
    Only ONE of rows/dataframe/arrow_table should be populated based on data_format.
    
    Attributes:
        rows: Data as list of dicts (when data_format=JSON_ROWS)
        dataframe: Data as Pandas DataFrame (when data_format=PANDAS)
        arrow_table: Data as PyArrow Table (when data_format=ARROW)
        columns: Column names (optional, for metadata)
        row_count: Number of rows returned
        data_format: Which format the data is in
        affected_rows: For mutations - number of rows affected
        message: Optional message for the response
        metadata: Additional handler-specific metadata
    """
    
    # Data containers (mutually exclusive based on data_format)
    rows: tuple[dict[str, Any], ...] | None = None
    dataframe: "pd.DataFrame | None" = None
    arrow_table: "pa.Table | None" = None
    
    # Metadata
    columns: tuple[str, ...] = field(default_factory=tuple)
    row_count: int = 0
    data_format: DataFormat = DataFormat.JSON_ROWS
    
    # For mutations
    affected_rows: int = 0
    
    # Optional response info
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    
    # Rich schema metadata (optional, populated by lifecycle during RESOLVE phase)
    column_metadata: tuple[dict, ...] = field(default_factory=tuple)
    
    @property
    def data(self) -> Any:
        """Get the data in whatever format it's stored."""
        if self.data_format == DataFormat.JSON_ROWS:
            return self.rows
        elif self.data_format == DataFormat.PANDAS:
            return self.dataframe
        elif self.data_format == DataFormat.ARROW:
            return self.arrow_table
        return self.rows


@dataclass(frozen=True, slots=True)
class OperationResponse:
    """
    Universal response object - UI only receives this.
    
    Attributes:
        success: Whether the operation succeeded
        data: The result data (records, affected count, etc.)
        affected_rows: Number of rows affected
        message: Human-readable message (especially for errors)
        errors: List of error details
        metadata: Additional response metadata
    """
    
    success: bool
    data: Any = None
    affected_rows: int = 0
    message: str = ""
    errors: list[OperationErrorDetail] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    
    # Schema information (always present for READ operations with columns="*")
    columns: tuple[str, ...] = field(default_factory=tuple)
    schema: tuple[ColumnMetadata, ...] = field(default_factory=tuple)
