# src/engine/models.py
"""
Pydantic models for SQL intent contracts.

Each model defines a typed, validated structure for a specific SQL operation.
The engine validates and transforms these into SQLGlot AST, never raw SQL.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


# ═══════════════════════════════════════════════════════════════════════════
# Shared building blocks
# ═══════════════════════════════════════════════════════════════════════════

class FilterClause(BaseModel):
    """A single WHERE filter."""
    column: str
    op: str  # must be in ALLOWED_OPERATORS
    value: Any = None  # None valid for IS NULL / IS NOT NULL


class OrderByClause(BaseModel):
    """A single ORDER BY directive."""
    column: str
    direction: Literal["ASC", "DESC"] = "ASC"


class AggregateColumn(BaseModel):
    """
    An aggregate function applied to a column.

    Generates SQL like: COUNT(`col`) AS col_count
    Use column="*" for COUNT(*).
    """
    function: Literal["COUNT", "SUM", "AVG", "MIN", "MAX"]
    column: str  # column name or "*" for COUNT(*)
    alias: str | None = None  # optional AS alias


# ═══════════════════════════════════════════════════════════════════════════
# SELECT intent
# ═══════════════════════════════════════════════════════════════════════════

class SelectIntent(BaseModel):
    """
    Simple SELECT with full pagination support.

    UI picks table, columns, filters, group_by, order_by, limit, offset.
    Supports aggregate functions (COUNT, SUM, AVG, MIN, MAX) and HAVING.
    """
    operation: Literal["SELECT"] = "SELECT"
    version: int = 1
    table: str
    columns: list[str]  # column names, or ["*"] for all
    filters: list[FilterClause] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    aggregations: list[AggregateColumn] = Field(default_factory=list)
    having: list[FilterClause] = Field(default_factory=list)
    order_by: list[OrderByClause] = Field(default_factory=list)
    limit: int | None = None
    offset: int | None = None

    @property
    def is_wildcard(self) -> bool:
        """Check if this is a SELECT * query."""
        return self.columns == ["*"] or (len(self.columns) == 1 and self.columns[0] == "*")

    @model_validator(mode="after")
    def _validate_pagination(self) -> "SelectIntent":
        """Validate OFFSET requires LIMIT and values are non-negative."""
        if self.offset is not None and self.limit is None:
            raise ValueError("OFFSET requires LIMIT to be specified")
        if self.offset is not None and self.offset < 0:
            raise ValueError("OFFSET must be >= 0")
        if self.limit is not None and self.limit <= 0:
            raise ValueError("LIMIT must be > 0")
        return self


# ═══════════════════════════════════════════════════════════════════════════
# Parameterised SELECT intent (stored SQL template)
# ═══════════════════════════════════════════════════════════════════════════

class ParameterisedSelectIntent(BaseModel):
    """
    The SQL template is stored server-side (identified by template_id).
    The UI only sends runtime parameter values.
    """
    operation: Literal["PARAMETERISED_SELECT"] = "PARAMETERISED_SELECT"
    version: int = 1
    template_id: str
    parameters: dict[str, Any] = Field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════════════════
# INSERT intent
# ═══════════════════════════════════════════════════════════════════════════

class InsertIntent(BaseModel):
    """Insert a single new row."""
    operation: Literal["INSERT"] = "INSERT"
    version: int = 1
    table: str
    values: dict[str, Any]  # column -> value


# ═══════════════════════════════════════════════════════════════════════════
# UPDATE intent
# ═══════════════════════════════════════════════════════════════════════════

class UpdateIntent(BaseModel):
    """
    Update one or more columns for a row identified by its PK(s).
    Supports both plain UPDATE and MERGE INTO strategies.

    OCC (Optimistic Concurrency Control):
        old_values holds the original column values fetched at read time.
        When non-empty and strategy="UPDATE", these are added to the WHERE
        clause so the UPDATE affects 0 rows if the record was modified
        concurrently.  Ignored for MERGE strategy.
    """
    operation: Literal["UPDATE"] = "UPDATE"
    version: int = 1
    table: str
    pk_values: dict[str, Any]  # PK column(s) -> value(s)
    updates: dict[str, Any]  # columns to set -> new values
    old_values: dict[str, Any] = Field(default_factory=dict)  # OCC: original values
    strategy: Literal["UPDATE", "MERGE"] = "UPDATE"


# ═══════════════════════════════════════════════════════════════════════════
# DELETE intent
# ═══════════════════════════════════════════════════════════════════════════

class DeleteIntent(BaseModel):
    """
    Delete row(s) by PK.
    Supports a single PK dict or a list of PK dicts (batch delete).
    """
    operation: Literal["DELETE"] = "DELETE"
    version: int = 1
    table: str
    pk_values: dict[str, Any] | list[dict[str, Any]]

    @model_validator(mode="after")
    def _normalise_pk_values(self) -> "DeleteIntent":
        """Ensure pk_values is always a list internally."""
        if isinstance(self.pk_values, dict):
            object.__setattr__(self, "pk_values", [self.pk_values])
        return self


# ═══════════════════════════════════════════════════════════════════════════
# Transaction intent (stub for future use)
# ═══════════════════════════════════════════════════════════════════════════

class TransactionIntent(BaseModel):
    """
    Future: Transaction wrapper for atomic multi-operation execution.
    Currently unused - TransactionHandler manages orchestration directly.
    Defined for future extensibility.
    """
    operation: Literal["TRANSACTION"] = "TRANSACTION"
    version: int = 1
    operations: list[InsertIntent | UpdateIntent | DeleteIntent] = Field(default_factory=list)
