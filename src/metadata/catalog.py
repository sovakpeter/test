"""
Unity Catalog table references - 3-level naming.

Centralized table name constants for the application.
"""

from src.infrastructure.config import get_settings


def _get_catalog() -> str:
    """Get catalog from settings."""
    return get_settings().catalog


def _get_schema() -> str:
    """Get schema from settings."""
    return get_settings().schema


class Tables:
    """
    Unity Catalog table references - 3-level naming.
    
    All table names follow catalog.schema.table format.
    Update this class when adding new tables.
    """
    
    @classmethod
    def _fqn(cls, table_name: str) -> str:
        """Build fully qualified table name."""
        return f"{_get_catalog()}.{_get_schema()}.{table_name}"
    
    # Example tables - update for your actual schema
    # USERS = f"{CATALOG}.{SCHEMA}.users"
    # ORDERS = f"{CATALOG}.{SCHEMA}.orders"
    # PRODUCTS = f"{CATALOG}.{SCHEMA}.products"
    
    # Reference tables (typically use SP connection + cache)
    # REF_REGIONS = f"{CATALOG}.{SCHEMA}.ref_regions"
    # REF_STATUS = f"{CATALOG}.{SCHEMA}.ref_status_codes"
