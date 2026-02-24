# src/engine/__init__.py
"""
SQLGlot-based SQL generation engine.

Provides AST-based SQL builders that produce parameterized queries
with %(param)s placeholders for the Databricks SQL connector.
"""

from src.engine.models import (
    FilterClause,
    OrderByClause,
    AggregateColumn,
    SelectIntent,
    InsertIntent,
    UpdateIntent,
    DeleteIntent,
    ParameterisedSelectIntent,
    TransactionIntent,
)
from src.engine.config import (
    ALLOWED_OPERATORS,
    DEFAULT_DIALECT,
    get_global_max_limit,
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

__all__ = [
    # Models
    "FilterClause",
    "OrderByClause",
    "AggregateColumn",
    "SelectIntent",
    "InsertIntent",
    "UpdateIntent",
    "DeleteIntent",
    "ParameterisedSelectIntent",
    "TransactionIntent",
    # Config
    "ALLOWED_OPERATORS",
    "DEFAULT_DIALECT",
    "get_global_max_limit",
    # Validators
    "validate_select",
    "validate_insert",
    "validate_update",
    "validate_delete",
    # Builders
    "build_select",
    "build_insert",
    "build_update",
    "build_delete",
]
