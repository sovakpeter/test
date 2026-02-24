# src/metadata/schema.py
"""
Metadata retrieval service.

Provides table schema information from Unity Catalog's information_schema
with hybrid caching (memory + JSON files).
"""

import threading
from datetime import datetime
from pathlib import Path
from typing import Sequence

from src.infrastructure.connection import ConnectionPool
from src.infrastructure.connection import execute_query
from src.infrastructure.logging import get_logger
from src.contracts.exceptions import MetadataAccessError
from src.metadata.models import ColumnSchema, TableSchema
from src.metadata.cache import SchemaCache, DEFAULT_TTL_SECONDS
from src.security.validators import validate_table_name, parse_table_ref
from src.queries import get_query_loader

logger = get_logger(__name__)

class SchemaProvider:
    """
    Service for retrieving and caching table schemas.
    
    Attributes:
        cache: Hybrid SchemaCache instance
        pool: ConnectionPool for database access
    """

    def __init__(self, cache_dir: Path, pool: ConnectionPool, ttl_seconds: int | None = None):
        self._cache = SchemaCache(cache_dir=cache_dir, ttl_seconds=ttl_seconds or DEFAULT_TTL_SECONDS)
        self._pool = pool
        self._inflight_lock = threading.Lock()
        self._inflight: dict[str, threading.Lock] = {}
        logger.info(f"SchemaProvider initialized with cache_dir={cache_dir}")

    def invalidate_table_schema(self, table_ref: str) -> None:
        """Invalidate cached schema for a table (memory + file)."""
        validate_table_name(table_ref)
        self._cache.invalidate(table_ref)
        with self._inflight_lock:
            self._inflight.pop(table_ref, None)

    def get_table_schema(self, table_ref: str) -> TableSchema:
        """Get schema for a table (cached or fetched)."""
        validate_table_name(table_ref)

        # Check cache
        cached = self._cache.get(table_ref)
        if cached:
            return cached

        # Single-flight fetch
        lock = self._get_inflight_lock(table_ref)
        with lock:
            # Re-check cache
            cached = self._cache.get(table_ref)
            if cached:
                return cached
            return self._fetch_schema(table_ref)

    def validate_columns(self, table_ref: str, columns: Sequence[str]) -> tuple[str, ...]:
        """Filter to only valid column names (SchemaGuard)."""
        schema = self.get_table_schema(table_ref)
        available = {c.column_name.lower(): c.column_name for c in schema.columns}
        
        valid = []
        for col in columns:
            col_lower = col.lower()
            if col_lower in available:
                valid.append(available[col_lower])
            else:
                logger.debug("Dropping unknown column %r for %s", col, table_ref)
        return tuple(valid)

    def _get_inflight_lock(self, table_ref: str) -> threading.Lock:
        with self._inflight_lock:
            if table_ref not in self._inflight:
                self._inflight[table_ref] = threading.Lock()
            return self._inflight[table_ref]

    def _fetch_schema(self, table_ref: str) -> TableSchema:
        """Fetch schema from database."""
        catalog, schema, table = parse_table_ref(table_ref)
        params = {"catalog": catalog, "schema_name": schema, "table_name": table}
        
        try:
            # Use SP connection (token=None) for metadata
            loader = get_query_loader()
            with self._pool.get_connection(token=None) as conn:
                columns_q = loader.get("schema.table_columns")
                pk_q = loader.get("schema.primary_keys")
                col_rows = execute_query(conn, columns_q.sql, params)
                pk_rows = execute_query(conn, pk_q.sql, params)

            pk_names = {
                (r.asDict().get("column_name") if hasattr(r, "asDict") else dict(r).get("column_name"))
                for r in pk_rows
            }
            pk_names.discard(None)

            columns: list[ColumnSchema] = []
            for row in col_rows:
                data = row.asDict() if hasattr(row, "asDict") else dict(row)
                col_name = str(data.get("column_name", ""))
                columns.append(
                    ColumnSchema(
                        column_name=col_name,
                        data_type=str(data.get("data_type", "STRING")),
                        is_nullable=bool(data.get("is_nullable", True)),
                        is_primary_key=col_name in pk_names,
                        ordinal_position=int(data.get("ordinal_position", 0)),
                    )
                )

            table_schema = TableSchema(
                catalog=catalog,
                schema=schema,
                table=table,
                columns=tuple(columns),
                fetched_at=datetime.now(),
            )

            self._cache.set(table_ref, table_schema)
            return table_schema

        except Exception as e:
            logger.error("Failed to fetch schema for %s: %s", table_ref, e)
            raise MetadataAccessError(
                user_message="Failed to access metadata",
                internal_message=str(e),
                table_ref=table_ref,
            )
