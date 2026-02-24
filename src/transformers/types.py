# src/transformers/types.py
"""
SQL literal formatting utilities.

This module provides utilities for formatting values as SQL literals
for debugging and logging. NOT for query building (use parameters).
"""

from typing import Any


def format_as_sql_literal(value: Any, databricks_type: str | None = None) -> str:
    """
    Format a Python value as a SQL literal string for debugging/logging.

    WARNING: This function is for DEBUGGING and LOGGING only.
    NEVER use the output to build actual SQL queries.
    Always use parameterized queries for database operations.

    Args:
        value: The Python value to format.
        databricks_type: Optional Databricks type hint.

    Returns:
        SQL literal representation of the value.
    """
    # Handle NULL
    if value is None:
        return "NULL"

    # Normalize type hint if provided
    type_upper = databricks_type.upper() if databricks_type else ""
    if type_upper.startswith("DECIMAL"):
        type_upper = "DECIMAL"

    # String types need quoting with escaped single quotes
    if type_upper in ("STRING", "VARCHAR", "CHAR", "DATE", "TIMESTAMP", "TIME"):
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    # If no type hint, infer from Python type
    if not databricks_type:
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        # Numbers pass through
        return str(value)

    # Boolean
    if type_upper == "BOOLEAN":
        return "TRUE" if value else "FALSE"

    # Numeric types (no quoting)
    return str(value)
