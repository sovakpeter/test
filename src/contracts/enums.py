"""
Enums for operation types, processing modes, and data formats.

These enums define:
- OperationType: What kind of operation (READ, INSERT/UPDATE/MERGE, DELETE, HEARTBEAT, TRANSACTION)
- ProcessingMode: How to process (SINGLE, BATCH, NAMED) - operation cardinality
- DataFormat: How data is represented (JSON_ROWS, ARROW, PANDAS) - data representation
- UIFormat: How data is delivered to UI (AUTO, JSON_DICT, PANDAS_DF, ARROW_TABLE)
"""

from enum import Enum, auto


class OperationType(Enum):
    """Type of database operation."""
    
    READ = auto()       # SELECT operations
    INSERT = auto()     # INSERT operations
    UPDATE = auto()     # UPDATE operations
    MERGE = auto()      # MERGE/UPSERT operations
    DELETE = auto()     # DELETE operations
    HEARTBEAT = auto()  # Health check
    TRANSACTION = auto()  # Wrapper operation: executes request.operations atomically
    SCHEMA = auto()  # Metadata operations (information_schema)


class SchemaScenario(Enum):
    """Predefined schema query scenarios (metadata operations)."""

    LIST_CATALOGS = "list_catalogs"
    LIST_SCHEMAS = "list_schemas"
    LIST_TABLES = "list_tables"
    TABLE_COLUMNS = "table_columns"
    TABLE_INFO = "table_info"
    INVALIDATE_TABLE_SCHEMA = "invalidate_table_schema"


class ProcessingMode(Enum):
    """
    How the operation should be processed (operation cardinality).
    
    This is orthogonal to DataFormat - ProcessingMode defines HOW MANY
    records are processed, DataFormat defines HOW data is represented.
    """
    
    SINGLE = auto()      # One record
    BATCH = auto()       # Multiple records, no transaction
    NAMED = auto()       # Named query execution


class DataFormat(Enum):
    """
    Data representation format from database connector.
    
    Controls HOW data is fetched and represented internally.
    This is orthogonal to ProcessingMode (cardinality).
    """
    
    JSON_ROWS = auto()   # list[dict] - Default, most compatible
    ARROW = auto()       # PyArrow Table - High performance, columnar
    PANDAS = auto()      # Pandas DataFrame - Analysis-friendly


# Backwards compatibility alias
OutputFormat = DataFormat


class UIFormat(Enum):
    """
    Format expected by UI layer for data delivery.
    
    Determines the final format of data delivered to the UI.
    AUTO resolves to the native format of the DataFormat.
    """
    
    AUTO = auto()        # Use native format (no conversion)
    JSON_DICT = auto()   # list[dict] - For DataTables, REST APIs
    PANDAS_DF = auto()   # Pandas DataFrame - For charts, aggregation
    ARROW_TABLE = auto() # PyArrow Table - For high-performance scenarios


class ErrorCategory(Enum):
    """Typed error categories for programmatic UI handling."""

    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    SECURITY = "security"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    CONNECTION = "connection"
    TIMEOUT = "timeout"
    THROTTLE = "throttle"
    ADMISSION = "admission"
    UNKNOWN = "unknown"
