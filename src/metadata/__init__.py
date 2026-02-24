"""
Metadata package - Schema awareness and caching.

Provides cached schema information and query registry.
"""

from src.metadata.models import TableSchema, ColumnSchema
from src.metadata.cache import SchemaCache
from src.metadata.schema import SchemaProvider
from src.metadata.registry import QueryRegistry

__all__ = [
    "TableSchema",
    "ColumnSchema",
    "SchemaCache",
    "SchemaProvider",
    "QueryRegistry",
]

