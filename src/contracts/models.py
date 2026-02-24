# src/contracts/models.py
"""
Data models for query operations.

Frozen dataclasses for filter conditions, sort orders, and other
structured data used in OperationRequest.
"""

from dataclasses import dataclass
from typing import Any


# Valid SQL operators for FilterCondition
VALID_OPERATORS = frozenset({
    "=", "!=", "<>", ">", "<", ">=", "<=",
    "IN", "NOT IN",
    "LIKE", "NOT LIKE",
    "IS NULL", "IS NOT NULL",
    "BETWEEN",
})


@dataclass(frozen=True, slots=True)
class FilterCondition:
    """
    Single filter condition for WHERE clause.
    
    Attributes:
        column: Column name to filter on.
        operator: SQL operator (=, !=, >, <, IN, LIKE, etc.).
        value: Filter value. None for IS NULL/IS NOT NULL, tuple for IN.
    """
    
    column: str
    operator: str
    value: Any = None
    
    def __post_init__(self) -> None:
        """Validate operator is supported."""
        if self.operator.upper() not in VALID_OPERATORS:
            raise ValueError(f"Unsupported operator: {self.operator}")


@dataclass(frozen=True, slots=True)
class SortOrder:
    """
    Single sort directive for ORDER BY clause.
    
    Attributes:
        column: Column name to sort by.
        direction: Sort direction (ASC or DESC).
    """
    
    column: str
    direction: str = "ASC"
    
    def __post_init__(self) -> None:
        """Validate direction."""
        if self.direction.upper() not in ("ASC", "DESC"):
            raise ValueError(f"Invalid sort direction: {self.direction}")
