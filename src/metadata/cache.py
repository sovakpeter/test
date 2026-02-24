# src/metadata/cache.py
"""
Hybrid schema cache with file and memory storage.

Provides caching for table schemas with:
- JSON file storage for persistence across restarts
- Memory cache with TTL for fast access
- Thread-safe operations
"""

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Final, Any

from src.infrastructure.logging import get_logger
from src.metadata.models import TableSchema, SchemaCacheEntry

logger = get_logger(__name__)

DEFAULT_TTL_SECONDS: Final[int] = 3600


class SchemaCache:
    """
    Hybrid cache for table schemas.
    """

    def __init__(self, cache_dir: Path, ttl_seconds: int = DEFAULT_TTL_SECONDS):
        self._cache_dir = cache_dir
        self._ttl = ttl_seconds
        self._memory: dict[str, SchemaCacheEntry] = {}
        self._lock = threading.Lock()
        
        # Ensure cache directory exists
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, table_ref: str) -> TableSchema | None:
        """Get schema from cache (memory -> file)."""
        # Fast path: check memory
        with self._lock:
            entry = self._memory.get(table_ref)
            if entry and not entry.is_expired():
                logger.debug("Schema cache hit (memory): %s", table_ref)
                return entry.schema # type: ignore

        # Slow path: check file
        schema = self._load_from_file(table_ref)
        if schema:
            age = (datetime.now() - schema.fetched_at).total_seconds()
            if age <= self._ttl:
                with self._lock:
                    self._memory[table_ref] = SchemaCacheEntry(
                        schema=schema,
                        cached_at=schema.fetched_at,
                        ttl_seconds=self._ttl,
                    )
                logger.debug("Schema cache hit (file): %s", table_ref)
                return schema
            logger.debug("Schema cache expired: %s", table_ref)

        logger.debug("Schema cache miss: %s", table_ref)
        return None

    def set(self, table_ref: str, schema: TableSchema) -> None:
        """Store schema in cache (memory + file)."""
        with self._lock:
            self._memory[table_ref] = SchemaCacheEntry(
                schema=schema,
                cached_at=datetime.now(),
                ttl_seconds=self._ttl,
            )
        self._save_to_file(table_ref, schema)
        logger.debug("Schema cached: %s", table_ref)

    def invalidate(self, table_ref: str) -> None:
        """Remove schema from cache."""
        with self._lock:
            self._memory.pop(table_ref, None)
        
        file_path = self._get_file_path(table_ref)
        if file_path.exists():
            file_path.unlink(missing_ok=True)

    def invalidate_all(self) -> None:
        """Clear entire cache."""
        with self._lock:
            self._memory.clear()
        for file_path in self._cache_dir.rglob("*.json"):
            file_path.unlink(missing_ok=True)

    def _get_file_path(self, table_ref: str) -> Path:
        """Get file path: <cache_dir>/<catalog>/<schema>.<table.json"""
        if validate_table_ref_simple(table_ref):
             parts = table_ref.split(".")
             return self._cache_dir / parts[0] / f"{parts[1]}.{parts[2]}.json"
        # Fallback for unexpected format (though validation should catch it)
        return self._cache_dir / f"{table_ref}.json"

    def _load_from_file(self, table_ref: str) -> TableSchema | None:
        """Load schema from JSON file."""
        file_path = self._get_file_path(table_ref)
        if not file_path.exists():
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return TableSchema.from_dict(data)
        except Exception as e:
            logger.warning("Failed to load schema from file %s: %s", file_path, e)
            return None

    def _save_to_file(self, table_ref: str, schema: TableSchema) -> None:
        """Save schema to JSON file."""
        file_path = self._get_file_path(table_ref)
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(schema.to_dict(), f, indent=2)
            os.replace(tmp_path, file_path)
        except Exception as e:
            logger.warning("Failed to save schema to file %s: %s", file_path, e)

# Helper to avoid circular dependency on validators
def validate_table_ref_simple(ref: str) -> bool:
    return ref.count(".") == 2
