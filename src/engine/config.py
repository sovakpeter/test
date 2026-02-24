# src/engine/config.py
"""
Engine configuration: allowed operators, dialect settings, global limits.

No table/column allow-list registry - access control is enforced by
Unity Catalog permissions on tables/views.
"""

from src.infrastructure.config import get_settings


# ═══════════════════════════════════════════════════════════════════════════
# Allowed filter operators (safety check)
# ═══════════════════════════════════════════════════════════════════════════

ALLOWED_OPERATORS: frozenset[str] = frozenset({
    "=", "!=", "<>", "<", "<=", ">", ">=",
    "IN", "NOT IN",
    "LIKE", "NOT LIKE",
    "IS NULL", "IS NOT NULL",
    "BETWEEN",
})


# ═══════════════════════════════════════════════════════════════════════════
# Dialect configuration
# ═══════════════════════════════════════════════════════════════════════════

DEFAULT_DIALECT = "spark"


# ═══════════════════════════════════════════════════════════════════════════
# Global limits
# ═══════════════════════════════════════════════════════════════════════════

def get_global_max_limit() -> int:
    """Get max limit from app settings."""
    return get_settings().max_read_limit


def get_default_limit() -> int:
    """Get default limit from app settings."""
    return get_settings().default_read_limit
