# src/security/validators.py
"""
SQL validators - Prevent SQL injection and validate queries.

Provides validation utilities for:
- SQL injection prevention (regex patterns)
- Identifier quoting and validation
- Read-only contract enforcement
- Table reference parsing
"""


import re
from typing import Final, Any

from src.contracts.exceptions import SecurityError, ValidationError

# =============================================================================
# Regex Patterns (Immutable, compiled)
# =============================================================================

# Safe identifier pattern: starts with letter/underscore, contains only alphanumeric/underscore
_IDENTIFIER_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# 3-level table reference pattern: catalog.schema.table
_TABLE_REF_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$"
)

# Read-only prefixes for defense-in-depth checks
_READONLY_PREFIXES: Final[tuple[str, ...]] = ("SELECT", "WITH", "DESCRIBE", "SHOW")

# Dangerous SQL patterns for injection detection
_DANGEROUS_PATTERNS: Final[list[str]] = [
    r";\s*(DROP|DELETE|TRUNCATE|INSERT|UPDATE|MERGE|ALTER|CREATE)\b",
    r"xp_",
    r"EXEC\s*\(",
]


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments for safer validation.

    Supports:
    - Line comments: -- ... \n
    - Block comments: /* ... */

    Note: This is a best-effort sanitizer intended for static, externalized SQL.
    """
    if not sql:
        return sql

    # Remove block comments first (non-greedy across newlines)
    without_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    # Remove line comments
    without_line = re.sub(r"--[^\n\r]*", "", without_block)
    return without_line


# =============================================================================
# SQL Injection Validation
# =============================================================================


def validate_sql(sql: str) -> bool:
    """
    Validate SQL statement for injection attempts (Regex-based).
    
    Args:
        sql: SQL statement to validate.
        
    Returns:
        True if SQL is safe.
        
    Raises:
        SecurityError: If SQL contains dangerous patterns.
    """
    if not sql:
        return True

    sql_for_scan = _strip_sql_comments(sql)
    # Normalize whitespace a bit to help regex patterns
    sql_for_scan = re.sub(r"\s+", " ", sql_for_scan)
        
    for pattern in _DANGEROUS_PATTERNS:
        if re.search(pattern, sql_for_scan, re.IGNORECASE):
            raise SecurityError(
                user_message="Invalid query",
                internal_message=f"SQL injection pattern detected: {pattern}"
            )
    return True


def validate_readonly_prefix(sql: str) -> bool:
    """
    Validate SQL starts with a read-only prefix (Defense-in-depth).
    
    Ensures that queries executed via read-only paths (READ operation)
    do not contain mutation commands.
    
    Args:
        sql: SQL string to validate.
        
    Returns:
        True if valid.
        
    Raises:
        SecurityError: If SQL does not start with a read-only prefix.
    """
    sql_upper = _strip_sql_comments(sql).strip().upper()
    if not any(sql_upper.startswith(p) for p in _READONLY_PREFIXES):
        # Truncate for log safety
        preview = sql[:50] + "..." if len(sql) > 50 else sql
        raise SecurityError(
            user_message="Invalid query type",
            internal_message=f"Non-read-only SQL in read-only contract: {preview}"
        )
    return True


# =============================================================================
# Identifier Validation & Quoting
# =============================================================================


def quote_identifier(name: str) -> str:
    """
    Wrap identifier in backticks for safe SQL use.
    
    Args:
        name: Identifier to quote.
        
    Returns:
        Quoted identifier (e.g., "`column_name`").
        
    Raises:
        ValidationError: If identifier format is invalid.
    """
    if not name or not _IDENTIFIER_PATTERN.match(name):
        raise ValidationError(
            user_message="Invalid identifier",
            internal_message=f"Invalid identifier pattern: {name}"
        )
    return f"`{name}`"


def quote_table_ref(table_ref: str) -> str:
    """
    Quote a 3-level table reference.
    
    Args:
        table_ref: Table reference (catalog.schema.table).
        
    Returns:
        Quoted reference (e.g., "`cat`.`sch`.`tbl`").
        
    Raises:
        ValidationError: If format is invalid.
    """
    if not _TABLE_REF_PATTERN.match(table_ref):
        raise ValidationError(
            user_message="Invalid table reference",
            internal_message=f"Must be catalog.schema.table, got: {table_ref}"
        )
    
    parts = table_ref.split(".")
    return ".".join(quote_identifier(p) for p in parts)


def validate_table_name(table: str) -> bool:
    """
    Validate Unity Catalog table name (3-level naming).
    
    Args:
        table: Fully qualified table name.
        
    Returns:
        True if valid.
    """
    # quote_table_ref performs the validation
    quote_table_ref(table)
    return True


def validate_columns(columns: list[str] | None) -> bool:
    """
    Validate column names.
    
    Args:
        columns: List of column names.
        
    Returns:
        True if all columns are valid.
    """
    if columns:
        for col in columns:
            # Allow "*" as wildcard for all columns
            if col == "*":
                continue
            quote_identifier(col)
    return True


def parse_table_ref(table_ref: str) -> tuple[str, str, str]:
    """
    Parse a 3-level table reference into components.
    
    Args:
        table_ref: Table reference.
        
    Returns:
        Tuple of (catalog, schema, table).
    """
    if not _TABLE_REF_PATTERN.match(table_ref):
        raise ValidationError(
            user_message="Invalid table reference",
            internal_message=f"Must be catalog.schema.table, got: {table_ref}"
        )
    
    parts = table_ref.split(".")
    return tuple(parts) # type: ignore

# =============================================================================
# Mutation Safety Validation
# =============================================================================


def validate_mutation_safety(
    operation: Any,
    payload: Any | None,
    where: dict[str, Any] | None = None,
) -> bool:
    """
    Validate safety of mutation operations (INSERT, UPDATE, DELETE).
    
    Enforces architectural invariants (based on OperationRequest contract):
    - INSERT must have a non-empty payload
    - UPDATE must have a non-empty payload AND a non-empty where clause
    - MERGE must have a non-empty payload AND a non-empty where clause (match keys)
    - DELETE must have a non-empty where clause
    
    Args:
        operation: OperationType or string (e.g. "INSERT", "UPDATE")
        payload: Request payload (dict for SINGLE)
        where: Request where clause (dict)
        
    Returns:
        True if valid
        
    Raises:
        ValidationError: If safety checks fail
    """
    op_str = str(operation).upper().replace("OPERATIONTYPE.", "")
    
    # Only validate mutations
    if op_str not in ("INSERT", "UPDATE", "DELETE", "MERGE"):
        return True
        
    # Normalize shapes (SINGLE only for now)
    payload_dict: dict[str, Any] | None = payload if isinstance(payload, dict) else None
    where_dict: dict[str, Any] | None = where if isinstance(where, dict) else None

    if op_str == "INSERT":
        if not payload_dict:
            raise ValidationError(
                user_message="Missing values",
                internal_message="INSERT operation requires a non-empty payload"
            )

    elif op_str == "UPDATE":
        if not payload_dict:
            raise ValidationError(
                user_message="Missing values",
                internal_message="UPDATE operation requires a non-empty payload"
            )
        if not where_dict:
            raise ValidationError(
                user_message="Missing identifier",
                internal_message="UPDATE operation requires a non-empty where clause"
            )

    elif op_str == "MERGE":
        if not payload_dict:
            raise ValidationError(
                user_message="Missing values",
                internal_message="MERGE operation requires a non-empty payload"
            )
        if not where_dict:
            raise ValidationError(
                user_message="Missing identifier",
                internal_message="MERGE operation requires a non-empty where clause (match keys)"
            )

    elif op_str == "DELETE":
        if not where_dict:
            raise ValidationError(
                user_message="Missing identifier",
                internal_message="DELETE operation requires a non-empty where clause"
            )

    return True
