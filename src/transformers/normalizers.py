# src/transformers/normalizers.py
"""
Operator and filter normalization for SQL synthesis.

Provides functions to normalize filter conditions, handling:
- Single-item lists optimization (IN -> =)
- Operator translation
- Value normalization
"""

from typing import Any, Sequence

from src.infrastructure.logging import get_logger
from src.contracts.models import FilterCondition


logger = get_logger(__name__)


def normalize_operator(
    operator: str,
    value: Any,
) -> tuple[str, Any]:
    """
    Normalize filter operator based on value type.
    
    Adjusts operators based on value cardinality:
    - Single value with "=" -> "="
    - List value with "=" -> "IN"
    - Single-item list with "IN" -> "=" (optimization)
    
    Args:
        operator: Original operator.
        value: Filter value.
        
    Returns:
        Tuple of (normalized_operator, normalized_value).
    """
    op_upper = operator.upper()

    # Handle list values
    if isinstance(value, (list, tuple)):
        if len(value) == 0:
            # Empty list edge case - effectively matches nothing
            return ("IS NULL", None)

        if len(value) == 1:
            # Single-item list optimization
            if op_upper in ("=", "IN"):
                return ("=", value[0])
            if op_upper in ("!=", "<>", "NOT IN"):
                return ("!=", value[0])
            # Other operators, extract single value
            return (operator, value[0])

        # Multi-item list
        if op_upper in ("=", "IN"):
            return ("IN", tuple(value))
        if op_upper in ("!=", "<>", "NOT IN"):
            return ("NOT IN", tuple(value))

        return (operator, tuple(value))

    # Non-list values
    return (operator, value)


def normalize_filter_conditions(
    conditions: Sequence[FilterCondition],
) -> tuple[FilterCondition, ...]:
    """
    Normalize a sequence of filter conditions.

    Args:
        conditions: Sequence of FilterCondition objects.

    Returns:
        Tuple of normalized FilterCondition objects.
    """
    if not conditions:
        return ()
        
    result = []
    for cond in conditions:
        normalized_op, normalized_val = normalize_operator(cond.operator, cond.value)
        result.append(
            FilterCondition(
                column=cond.column,
                operator=normalized_op,
                value=normalized_val,
            )
        )
    return tuple(result)
