"""QueryLoader - loads externalized SQL + metadata from manifest.

Hybrid approach:
- SQL text lives in .sql files under src/queries/
- manifest.json maps query keys -> file + metadata

This is intentionally framework-agnostic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.contracts.exceptions import ValidationError
from src.infrastructure.logging import get_logger
from src.queries.models import QueryDefinition, QueryParameter
from src.security.validators import quote_identifier, validate_readonly_prefix, validate_sql

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    file: str
    description: str = ""
    parameters: tuple[QueryParameter, ...] = ()
    cache_ttl: int = 0
    tags: tuple[str, ...] = ()


class QueryLoader:
    def __init__(self, queries_dir: Path | None = None):
        self._dir = queries_dir or (Path(__file__).parent)
        self._manifest: dict[str, ManifestEntry] = {}
        self._cache: dict[str, QueryDefinition] = {}
        self._load_manifest()

    def _load_manifest(self) -> None:
        manifest_path = self._dir / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Query manifest not found: {manifest_path}")

        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        queries = data.get("queries")
        if not isinstance(queries, dict):
            raise ValidationError(
                user_message="Invalid query manifest",
                internal_message="manifest.json must contain a top-level 'queries' object",
            )

        manifest: dict[str, ManifestEntry] = {}
        for key, entry in queries.items():
            if not isinstance(key, str) or not isinstance(entry, dict):
                raise ValidationError(
                    user_message="Invalid query manifest",
                    internal_message="Each manifest query must map string key -> object",
                )

            # Validate key segments as safe identifiers: schema.list_tables, analytics.foo, etc.
            for segment in key.split("."):
                quote_identifier(segment)

            file = entry.get("file")
            if not isinstance(file, str) or not file.strip():
                raise ValidationError(
                    user_message="Invalid query manifest",
                    internal_message=f"Query '{key}' missing 'file'",
                )

            params: list[QueryParameter] = []
            raw_params = entry.get("parameters", [])
            if raw_params is None:
                raw_params = []
            if not isinstance(raw_params, list):
                raise ValidationError(
                    user_message="Invalid query manifest",
                    internal_message=f"Query '{key}' parameters must be a list",
                )
            for p in raw_params:
                if not isinstance(p, dict):
                    raise ValidationError(
                        user_message="Invalid query manifest",
                        internal_message=f"Query '{key}' parameters must be objects",
                    )
                name = str(p.get("name", "")).strip()
                if not name:
                    raise ValidationError(
                        user_message="Invalid query manifest",
                        internal_message=f"Query '{key}' has parameter with missing name",
                    )
                quote_identifier(name)
                params.append(
                    QueryParameter(
                        name=name,
                        param_type=str(p.get("param_type", "string")),
                        required=bool(p.get("required", True)),
                        default=p.get("default"),
                        description=str(p.get("description", "")),
                    )
                )

            tags = entry.get("tags", [])
            if tags is None:
                tags = []
            if not isinstance(tags, list) or not all(isinstance(t, str) for t in tags):
                raise ValidationError(
                    user_message="Invalid query manifest",
                    internal_message=f"Query '{key}' tags must be a list of strings",
                )

            cache_ttl = entry.get("cache_ttl", 0)
            if cache_ttl is None:
                cache_ttl = 0
            if not isinstance(cache_ttl, int) or cache_ttl < 0:
                raise ValidationError(
                    user_message="Invalid query manifest",
                    internal_message=f"Query '{key}' cache_ttl must be a non-negative int",
                )

            manifest[key] = ManifestEntry(
                file=file,
                description=str(entry.get("description", "")),
                parameters=tuple(params),
                cache_ttl=cache_ttl,
                tags=tuple(tags),
            )

        self._manifest = manifest
        self._cache.clear()
        logger.info("Loaded %d manifest entries", len(self._manifest))

    def get(self, key: str) -> QueryDefinition:
        if key not in self._cache:
            self._cache[key] = self._load_query(key)
        return self._cache[key]

    def _load_query(self, key: str) -> QueryDefinition:
        if key not in self._manifest:
            raise ValidationError(
                user_message="Unknown query",
                internal_message=f"Query not found in manifest: {key}",
            )

        entry = self._manifest[key]
        sql_path = (self._dir / entry.file).resolve()
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        sql = sql_path.read_text(encoding="utf-8").strip()
        validate_sql(sql)
        validate_readonly_prefix(sql)

        return QueryDefinition(
            key=key,
            sql=sql,
            description=entry.description,
            parameters=entry.parameters,
            cache_ttl=entry.cache_ttl,
            tags=entry.tags,
        )

    def list_by_tag(self, tag: str) -> list[str]:
        return [k for k, e in self._manifest.items() if tag in e.tags]

    def reload(self) -> None:
        self._load_manifest()


# ──────────────────────────────────────────────────────────────────────────────
# Singleton instance - shared across all modules
# ──────────────────────────────────────────────────────────────────────────────
_SINGLETON: QueryLoader | None = None
_SINGLETON_LOCK = __import__("threading").Lock()


def get_query_loader() -> QueryLoader:
    """Return the singleton QueryLoader instance.
    
    Thread-safe lazy initialization. All modules should use this
    instead of creating their own QueryLoader instances.
    """
    global _SINGLETON
    if _SINGLETON is None:
        with _SINGLETON_LOCK:
            if _SINGLETON is None:
                _SINGLETON = QueryLoader(Path("src/queries"))
    return _SINGLETON
