# src/metadata/models.py
"""
Metadata models for Unity Catalog table schemas.

Defines immutable dataclasses for representing table metadata including
column definitions, data types, nullability, and primary key information.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence


@dataclass(frozen=True, slots=True)
class ColumnSchema:
    """
    Schema information for a single column.
    """
    column_name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool
    ordinal_position: int = 0

    @property
    def normalized_type(self) -> str:
        """Return normalized type name for mapping."""
        type_upper = self.data_type.upper()
        if type_upper.startswith("DECIMAL"): return "DECIMAL"
        if type_upper.startswith("VARCHAR") or type_upper.startswith("CHAR"): return "STRING"
        if type_upper.startswith("ARRAY"): return "ARRAY"
        if type_upper.startswith("MAP"): return "MAP"
        if type_upper.startswith("STRUCT"): return "STRUCT"
        return type_upper

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "ColumnSchema":
        """Create ColumnSchema from database row."""
        data = row.asDict() if hasattr(row, "asDict") else dict(row)
        return cls(
            column_name=data.get("column_name", ""),
            data_type=data.get("data_type", "STRING"),
            is_nullable=bool(data.get("is_nullable", True)),
            is_primary_key=bool(data.get("is_pk", False)),
            ordinal_position=int(data.get("ordinal_position", 0)),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "column_name": self.column_name,
            "data_type": self.data_type,
            "is_nullable": self.is_nullable,
            "is_primary_key": self.is_primary_key,
            "ordinal_position": self.ordinal_position,
        }


@dataclass(frozen=True, slots=True)
class TableSchema:
    """
    Complete schema for a table.
    """
    catalog: str
    schema: str
    table: str
    columns: tuple[ColumnSchema, ...]
    fetched_at: datetime

    @property
    def table_ref(self) -> str:
        """Return 3-level table reference."""
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def primary_key_columns(self) -> tuple[str, ...]:
        """Return primary key column names."""
        return tuple(c.column_name for c in self.columns if c.is_primary_key)

    @property
    def column_names(self) -> tuple[str, ...]:
        """Return all column names in order."""
        return tuple(c.column_name for c in self.columns)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "table": self.table,
            "fetched_at": self.fetched_at.isoformat(),
            "columns": [c.to_dict() for c in self.columns],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableSchema":
        """Create TableSchema from dictionary."""
        columns = tuple(
            ColumnSchema(
                column_name=c["column_name"],
                data_type=c["data_type"],
                is_nullable=c.get("is_nullable", True),
                is_primary_key=c.get("is_primary_key", False),
                ordinal_position=c.get("ordinal_position", 0),
            )
            for c in data.get("columns", [])
        )
        fetched_at = datetime.fromisoformat(data.get("fetched_at", datetime.now().isoformat()))
        return cls(
            catalog=data["catalog"],
            schema=data["schema"],
            table=data["table"],
            columns=columns,
            fetched_at=fetched_at,
        )


@dataclass(frozen=True, slots=True)
class SchemaCacheEntry:
    """Cache entry with TTL tracking."""
    schema: Any # TableSchema or Lists
    cached_at: datetime
    ttl_seconds: int = 3600

    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        age_seconds = (datetime.now() - self.cached_at).total_seconds()
        return age_seconds > self.ttl_seconds
