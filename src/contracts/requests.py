"""
OperationRequest - Universal request object.

UI only constructs this object to request operations.
"""

from dataclasses import dataclass, field
from typing import Any

from src.contracts.enums import OperationType, ProcessingMode, DataFormat, UIFormat, SchemaScenario

# Backwards compatibility alias
OutputFormat = DataFormat


@dataclass(frozen=True, slots=True)
class OperationRequest:
    """
    Universal request object - UI only constructs this.
    
    Attributes:
        operation: Type of operation (READ, INSERT, UPDATE, MERGE, DELETE, HEARTBEAT, TRANSACTION, SCHEMA)
        table: Fully qualified table name (catalog.schema.table)
        payload: Data for the operation (dict for single, list for batch)
        mode: Processing mode (SINGLE, BATCH, NAMED) - operation cardinality
        columns: Columns to select (for READ) or include
        where: Filter conditions (dict for shared, list[dict] for per-record in batch UPDATE/MERGE)
        options: Additional options (limit, offset, etc.)
        operations: For transactions - list of sub-operations
        
    Format specifications (two orthogonal dimensions):
        data_format: How data is represented internally (JSON_ROWS, ARROW, PANDAS)
        ui_format: How data is delivered to UI (AUTO, JSON_DICT, PANDAS_DF, ARROW_TABLE)
        
        Note: ProcessingMode (SINGLE/BATCH/NAMED) is about operation cardinality,
              DataFormat is about data representation - they are independent.

    Schema/meta convention:
        When operation=OperationType.SCHEMA:
        - scenario MUST be set (SchemaScenario or string)
        - catalog/schema_name MAY be set (for list scenarios)
        - table MAY be set (for table-level scenarios; use catalog.schema.table)

    Transaction wrapper convention:
        - operation MUST be OperationType.TRANSACTION (drives routing)
        - table SHOULD be "_transaction" (informational only)
        - mode SHOULD be ProcessingMode.SINGLE (ignored by router for transactions)
        - operations MUST be a list[OperationRequest] describing the actual work

    Transaction sub-operation constraints (v1):
        - mode MUST be ProcessingMode.SINGLE
        - payload MUST be a dict (no list/batch payloads)
    """
    
    operation: OperationType
    table: str
    payload: dict[str, Any] | list[dict[str, Any]] = field(default_factory=dict)
    
    # Optional metadata that influences routing
    mode: ProcessingMode = ProcessingMode.SINGLE
    columns: list[str] | None = None
    where: dict[str, Any] | list[dict[str, Any]] | None = None
    options: dict[str, Any] = field(default_factory=dict)
    
    # Format specifications (two orthogonal dimensions)
    # DataFormat: How data is represented (JSON_ROWS, ARROW, PANDAS)
    # UIFormat: How data is delivered to UI (AUTO resolves based on data_format)
    data_format: DataFormat = DataFormat.JSON_ROWS
    ui_format: UIFormat = UIFormat.AUTO
    
    # Backwards compatibility alias for output_format
    @property
    def output_format(self) -> DataFormat:
        """Backwards compatibility alias for data_format."""
        return self.data_format
    
    # For transactions: list of sub-operations
    operations: list["OperationRequest"] | None = None

    # Schema/meta operations
    scenario: SchemaScenario | str | None = None
    catalog: str | None = None
    schema_name: str | None = None
