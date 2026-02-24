# src/transformers/sql_builders.py
"""
SQL clause builders for query synthesis.

Delegates entirely to src/engine/ for SQLGlot AST-based SQL generation.
Maintains backward-compatible function signatures for handler compatibility.
"""

from typing import Any

from src.infrastructure.logging import get_logger
from src.contracts.requests import OperationRequest
from src.contracts.enums import OperationType
from src.engine.models import (
    SelectIntent,
    InsertIntent,
    UpdateIntent,
    DeleteIntent,
    FilterClause,
    OrderByClause,
    AggregateColumn,
)
from src.engine.validators import (
    validate_select,
    validate_insert,
    validate_update,
    validate_delete,
)
from src.engine.builders import (
    build_select,
    build_insert,
    build_update,
    build_delete,
)


logger = get_logger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Request → Intent Translation
# ═══════════════════════════════════════════════════════════════════════════

def _request_to_select_intent(request: OperationRequest) -> SelectIntent:
    """Map OperationRequest to SelectIntent."""
    from src.contracts.exceptions import ValidationError
    
    filters = []
    if request.where:
        for k, v in request.where.items():
            filters.append(FilterClause(column=k, op="=", value=v))
    
    order_by = []
    if "order_by" in request.options:
        for order in request.options["order_by"]:
            if hasattr(order, "column"):
                order_by.append(OrderByClause(
                    column=order.column,
                    direction=getattr(order, "direction", "ASC")
                ))
            elif isinstance(order, dict):
                order_by.append(OrderByClause(
                    column=order["column"],
                    direction=order.get("direction", "ASC")
                ))
            else:
                # Tuple format: (column, direction)
                col, direction = order
                order_by.append(OrderByClause(column=col, direction=direction))
    
    # Validate and parse limit
    limit = None
    if "limit" in request.options:
        limit_raw = request.options["limit"]
        try:
            limit = int(limit_raw) if limit_raw is not None else None
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                user_message="Invalid limit",
                internal_message=f"limit must be an int (got {limit_raw!r})",
            ) from exc
        if limit is not None and limit <= 0:
            raise ValidationError(
                user_message="Invalid limit",
                internal_message=f"limit must be > 0 (got {limit})",
            )
    
    # Validate and parse offset
    offset = None
    if "offset" in request.options:
        offset_raw = request.options["offset"]
        try:
            offset = int(offset_raw) if offset_raw is not None else None
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                user_message="Invalid offset",
                internal_message=f"offset must be an int (got {offset_raw!r})",
            ) from exc
        if offset is not None and offset < 0:
            raise ValidationError(
                user_message="Invalid offset",
                internal_message=f"offset must be >= 0 (got {offset})",
            )
    
    # Parse aggregations from options
    aggregations = []
    if "aggregations" in request.options:
        for agg in request.options["aggregations"]:
            if isinstance(agg, dict):
                aggregations.append(AggregateColumn(
                    function=agg["function"],
                    column=agg["column"],
                    alias=agg.get("alias"),
                ))
            elif isinstance(agg, AggregateColumn):
                aggregations.append(agg)

    # Parse HAVING from options
    having = []
    if "having" in request.options:
        for h in request.options["having"]:
            if isinstance(h, dict):
                having.append(FilterClause(
                    column=h["column"],
                    op=h.get("operator", h.get("op", ">")),
                    value=h.get("value"),
                ))
            elif isinstance(h, FilterClause):
                having.append(h)

    # Parse group_by from options
    group_by = []
    if "group_by" in request.options:
        group_by = request.options["group_by"]

    return SelectIntent(
        table=request.table,
        columns=request.columns if request.columns else ["*"],
        filters=filters,
        group_by=group_by,
        aggregations=aggregations,
        having=having,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )


def _request_to_insert_intent(request: OperationRequest) -> InsertIntent:
    """Map OperationRequest to InsertIntent."""
    payload = request.payload
    if not isinstance(payload, dict):
        raise ValueError("INSERT requires dict payload, not list")
    return InsertIntent(
        table=request.table,
        values=payload,
    )


def _request_to_update_intent(request: OperationRequest) -> UpdateIntent:
    """Map OperationRequest to UpdateIntent."""
    payload = request.payload
    if not isinstance(payload, dict):
        raise ValueError("UPDATE requires dict payload, not list")

    strategy = "MERGE" if request.operation == OperationType.MERGE else "UPDATE"
    old_values = request.options.get("old_values", {}) if request.options else {}
    return UpdateIntent(
        table=request.table,
        pk_values=request.where or {},
        updates=payload,
        old_values=old_values,
        strategy=strategy,
    )


def _request_to_delete_intent(request: OperationRequest) -> DeleteIntent:
    """Map OperationRequest to DeleteIntent."""
    if not request.where:
        raise ValueError("DELETE requires WHERE clause (PK)")
    return DeleteIntent(
        table=request.table,
        pk_values=request.where,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Public Builder Functions (Handler Contract - Signatures Unchanged)
# ═══════════════════════════════════════════════════════════════════════════

def build_select_query(request: OperationRequest) -> tuple[str, dict[str, Any]]:
    """
    Build SELECT query from request.
    
    Args:
        request: OperationRequest with table, columns, where, options
        
    Returns:
        Tuple of (sql_string, params_dict)
    """
    intent = _request_to_select_intent(request)
    validate_select(intent)
    sql, params = build_select(intent)
    logger.debug("Built SELECT: %s", sql[:100] if len(sql) > 100 else sql)
    return sql, params


def build_write_query(request: OperationRequest) -> tuple[str, dict[str, Any]]:
    """
    Build INSERT/UPDATE/MERGE query.
    
    Args:
        request: OperationRequest with operation type, table, payload, where
        
    Returns:
        Tuple of (sql_string, params_dict)
    """
    if request.operation == OperationType.INSERT:
        intent = _request_to_insert_intent(request)
        validate_insert(intent)
        sql, params = build_insert(intent)
        logger.debug("Built INSERT: %s", sql[:100] if len(sql) > 100 else sql)
        return sql, params
    else:
        # UPDATE or MERGE
        intent = _request_to_update_intent(request)
        validate_update(intent)
        sql, params = build_update(intent)
        logger.debug("Built UPDATE/MERGE: %s", sql[:100] if len(sql) > 100 else sql)
        return sql, params


def build_delete_query(request: OperationRequest) -> tuple[str, dict[str, Any]]:
    """
    Build DELETE query.
    
    Args:
        request: OperationRequest with table, where (PK condition)
        
    Returns:
        Tuple of (sql_string, params_dict)
    """
    intent = _request_to_delete_intent(request)
    validate_delete(intent)
    sql, params = build_delete(intent)
    logger.debug("Built DELETE: %s", sql[:100] if len(sql) > 100 else sql)
    return sql, params
