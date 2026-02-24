# src/engine/validators.py
"""
Pre-SQL validation layer.

Validates intent structure before SQL generation:
- Operator safety
- Required fields
- Limit enforcement

No table/column registry checks - Unity Catalog handles permissions.
"""

from __future__ import annotations

from src.engine.config import ALLOWED_OPERATORS, get_global_max_limit
from src.engine.models import (
    SelectIntent,
    InsertIntent,
    UpdateIntent,
    DeleteIntent,
)


ALLOWED_AGGREGATE_FUNCTIONS: frozenset[str] = frozenset({
    "COUNT", "SUM", "AVG", "MIN", "MAX",
})


def validate_select(intent: SelectIntent) -> None:
    """
    Validate SELECT intent structure.

    Checks:
    - Filter operators are in allowed set
    - Limit is enforced within global max
    - Aggregate functions are in allowed set
    - HAVING operators are in allowed set
    - HAVING requires GROUP BY
    """
    # Validate filter operators
    for f in intent.filters:
        op_upper = f.op.upper()
        if op_upper not in ALLOWED_OPERATORS:
            raise ValueError(
                f"Operator '{f.op}' not allowed. "
                f"Allowed: {sorted(ALLOWED_OPERATORS)}"
            )

    # Validate aggregate functions
    for agg in intent.aggregations:
        func_upper = agg.function.upper()
        if func_upper not in ALLOWED_AGGREGATE_FUNCTIONS:
            raise ValueError(
                f"Aggregate function '{agg.function}' not allowed. "
                f"Allowed: {sorted(ALLOWED_AGGREGATE_FUNCTIONS)}"
            )

    # Validate HAVING operators
    for h in intent.having:
        op_upper = h.op.upper()
        if op_upper not in ALLOWED_OPERATORS:
            raise ValueError(
                f"HAVING operator '{h.op}' not allowed. "
                f"Allowed: {sorted(ALLOWED_OPERATORS)}"
            )

    # HAVING requires GROUP BY
    if intent.having and not intent.group_by:
        raise ValueError("HAVING clause requires GROUP BY")

    # Enforce global max limit
    max_limit = get_global_max_limit()
    if intent.limit is not None:
        if intent.limit > max_limit:
            # Silently cap to max_limit
            object.__setattr__(intent, "limit", max_limit)


def validate_insert(intent: InsertIntent) -> None:
    """
    Validate INSERT intent structure.
    
    Checks:
    - At least one column value is provided
    """
    if not intent.values:
        raise ValueError("INSERT requires at least one column value")


def validate_update(intent: UpdateIntent) -> None:
    """
    Validate UPDATE intent structure.
    
    Checks:
    - PK values are provided
    - At least one column to update
    - PK columns are not being updated
    """
    if not intent.pk_values:
        raise ValueError("UPDATE requires primary key values")
    if not intent.updates:
        raise ValueError("UPDATE requires at least one column to update")
    
    # Don't allow updating PK columns
    pk_in_updates = set(intent.updates.keys()) & set(intent.pk_values.keys())
    if pk_in_updates:
        raise ValueError(f"Cannot update primary-key column(s): {pk_in_updates}")

    # OCC: old_values keys must not overlap with pk_values (PK already in WHERE)
    if intent.old_values:
        overlap = set(intent.old_values.keys()) & set(intent.pk_values.keys())
        if overlap:
            raise ValueError(
                f"old_values must not include primary-key column(s) "
                f"(already in WHERE): {overlap}"
            )


def validate_delete(intent: DeleteIntent) -> None:
    """
    Validate DELETE intent structure.
    
    Checks:
    - PK values are provided
    """
    # pk_values is normalised to list[dict] by model validator
    if not intent.pk_values:
        raise ValueError("DELETE requires primary key values")
