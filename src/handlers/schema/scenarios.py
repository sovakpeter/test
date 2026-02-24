"""SchemaScenarioHandler - production metadata retrieval via information_schema.

Implements SCHEMA meta-operations (list catalogs/schemas/tables, table columns/info)
using Unity Catalog system.information_schema.

All schema operations use Service Principal (SP) connection (token=None) by default.

Multi-Format Support:
- Tabular results (LIST_CATALOGS, LIST_SCHEMAS, LIST_TABLES, TABLE_COLUMNS) return QueryResult
- Object results (TABLE_INFO) and actions (INVALIDATE) return OperationResponse
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.contracts.enums import SchemaScenario, DataFormat
from src.contracts.exceptions import ValidationError
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.dispatcher.context import ExecutionContext
from src.handlers.base import BaseHandler
from src.infrastructure.connection import execute_query, execute_query_arrow, execute_query_pandas
from src.infrastructure.logging import get_logger
from src.infrastructure.config import get_settings
from src.metadata.schema import SchemaProvider
from src.queries import get_query_loader

logger = get_logger(__name__)

_CACHE_DIR = Path("cache/schema")
_PROVIDER: SchemaProvider | None = None


def _get_provider(ctx: ExecutionContext) -> SchemaProvider:
    global _PROVIDER
    if _PROVIDER is None:
        settings = get_settings()
        _PROVIDER = SchemaProvider(
            cache_dir=_CACHE_DIR,
            pool=ctx.pool,
            ttl_seconds=settings.schema_cache_ttl_seconds,
        )
    return _PROVIDER


class SchemaScenarioHandler(BaseHandler):
    """Handler for a single SchemaScenario."""

    def __init__(self, scenario: SchemaScenario):
        self._scenario = scenario

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, f"Schema:{self._scenario.value}")

        try:
            if self._scenario == SchemaScenario.LIST_CATALOGS:
                return self._list_catalogs(ctx, request)

            if self._scenario == SchemaScenario.LIST_SCHEMAS:
                if not request.catalog:
                    raise ValidationError(
                        user_message="Missing catalog",
                        internal_message="SCHEMA list_schemas requires request.catalog",
                    )
                return self._list_schemas(ctx, request, request.catalog)

            if self._scenario == SchemaScenario.LIST_TABLES:
                if not request.catalog or not request.schema_name:
                    raise ValidationError(
                        user_message="Missing catalog/schema",
                        internal_message="SCHEMA list_tables requires request.catalog and request.schema_name",
                    )
                return self._list_tables(ctx, request, request.catalog, request.schema_name)

            if self._scenario == SchemaScenario.TABLE_COLUMNS:
                if not request.table:
                    raise ValidationError(
                        user_message="Missing table",
                        internal_message="SCHEMA table_columns requires request.table",
                    )
                provider = _get_provider(ctx)
                schema = provider.get_table_schema(request.table)
                rows = [c.to_dict() for c in schema.columns]
                
                # TABLE_COLUMNS returns QueryResult (tabular data)
                return QueryResult(
                    rows=rows,
                    data_format=DataFormat.JSON_ROWS,
                    row_count=len(rows),
                    metadata={
                        "scenario": self._scenario.value,
                        "table": schema.table_ref,
                        "fetched_at": schema.fetched_at.isoformat(),
                    },
                )

            if self._scenario == SchemaScenario.TABLE_INFO:
                if not request.table:
                    raise ValidationError(
                        user_message="Missing table",
                        internal_message="SCHEMA table_info requires request.table",
                    )
                provider = _get_provider(ctx)
                schema = provider.get_table_schema(request.table)
                # TABLE_INFO returns object data (not tabular), use OperationResponse
                data = {
                    "catalog": schema.catalog,
                    "schema": schema.schema,
                    "table": schema.table,
                    "table_ref": schema.table_ref,
                    "primary_keys": list(schema.primary_key_columns),
                    "columns": [c.to_dict() for c in schema.columns],
                    "fetched_at": schema.fetched_at.isoformat(),
                }
                return OperationResponse(
                    success=True,
                    data=data,
                    metadata={
                        "scenario": self._scenario.value,
                        "table": schema.table_ref,
                        "row_count": len(schema.columns),
                    },
                )

            if self._scenario == SchemaScenario.INVALIDATE_TABLE_SCHEMA:
                if not request.table:
                    raise ValidationError(
                        user_message="Missing table",
                        internal_message="SCHEMA invalidate_table_schema requires request.table",
                    )
                provider = _get_provider(ctx)
                provider.invalidate_table_schema(request.table)
                # Action operations return OperationResponse
                return OperationResponse(
                    success=True,
                    data={"invalidated": True, "table": request.table},
                    metadata={"scenario": self._scenario.value, "table": request.table},
                )

            raise ValidationError(
                user_message="Unsupported schema scenario",
                internal_message=f"Unsupported schema scenario: {self._scenario}",
            )

        except Exception as e:
            return self._handle_error(ctx, f"Schema:{self._scenario.value}", e)

    def _list_catalogs(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult:
        query = get_query_loader().get("schema.list_catalogs")
        data_format = request.data_format

        with ctx.pool.get_connection(token=None) as conn:
            if data_format == DataFormat.ARROW:
                arrow_table = execute_query_arrow(conn, query.sql, {})
                return QueryResult(
                    arrow_table=arrow_table,
                    data_format=DataFormat.ARROW,
                    row_count=arrow_table.num_rows,
                    metadata={"scenario": self._scenario.value},
                )
            elif data_format == DataFormat.PANDAS:
                df = execute_query_pandas(conn, query.sql, {})
                return QueryResult(
                    dataframe=df,
                    data_format=DataFormat.PANDAS,
                    row_count=len(df),
                    metadata={"scenario": self._scenario.value},
                )
            else:
                rows = execute_query(conn, query.sql, {})
                data = [row.asDict() if hasattr(row, "asDict") else dict(row) for row in rows]
                return QueryResult(
                    rows=data,
                    data_format=DataFormat.JSON_ROWS,
                    row_count=len(data),
                    metadata={"scenario": self._scenario.value},
                )

    def _list_schemas(self, ctx: ExecutionContext, request: OperationRequest, catalog: str) -> QueryResult:
        query = get_query_loader().get("schema.list_schemas")
        params = {"catalog": catalog}
        errors = query.validate_params(params)
        if errors:
            raise ValidationError(
                user_message="Invalid parameters",
                internal_message="; ".join(errors),
            )

        data_format = request.data_format

        with ctx.pool.get_connection(token=None) as conn:
            if data_format == DataFormat.ARROW:
                arrow_table = execute_query_arrow(conn, query.sql, params)
                return QueryResult(
                    arrow_table=arrow_table,
                    data_format=DataFormat.ARROW,
                    row_count=arrow_table.num_rows,
                    metadata={"scenario": self._scenario.value, "catalog": catalog},
                )
            elif data_format == DataFormat.PANDAS:
                df = execute_query_pandas(conn, query.sql, params)
                return QueryResult(
                    dataframe=df,
                    data_format=DataFormat.PANDAS,
                    row_count=len(df),
                    metadata={"scenario": self._scenario.value, "catalog": catalog},
                )
            else:
                rows = execute_query(conn, query.sql, params)
                data = [row.asDict() if hasattr(row, "asDict") else dict(row) for row in rows]
                return QueryResult(
                    rows=data,
                    data_format=DataFormat.JSON_ROWS,
                    row_count=len(data),
                    metadata={"scenario": self._scenario.value, "catalog": catalog},
                )

    def _list_tables(self, ctx: ExecutionContext, request: OperationRequest, catalog: str, schema_name: str) -> QueryResult:
        query = get_query_loader().get("schema.list_tables")
        params = {"catalog": catalog, "schema_name": schema_name}
        errors = query.validate_params(params)
        if errors:
            raise ValidationError(
                user_message="Invalid parameters",
                internal_message="; ".join(errors),
            )

        data_format = request.data_format

        with ctx.pool.get_connection(token=None) as conn:
            if data_format == DataFormat.ARROW:
                arrow_table = execute_query_arrow(conn, query.sql, params)
                return QueryResult(
                    arrow_table=arrow_table,
                    data_format=DataFormat.ARROW,
                    row_count=arrow_table.num_rows,
                    metadata={
                        "scenario": self._scenario.value,
                        "catalog": catalog,
                        "schema": schema_name,
                    },
                )
            elif data_format == DataFormat.PANDAS:
                df = execute_query_pandas(conn, query.sql, params)
                return QueryResult(
                    dataframe=df,
                    data_format=DataFormat.PANDAS,
                    row_count=len(df),
                    metadata={
                        "scenario": self._scenario.value,
                        "catalog": catalog,
                        "schema": schema_name,
                    },
                )
            else:
                rows = execute_query(conn, query.sql, params)
                data = [row.asDict() if hasattr(row, "asDict") else dict(row) for row in rows]
                return QueryResult(
                    rows=data,
                    data_format=DataFormat.JSON_ROWS,
                    row_count=len(data),
                    metadata={
                        "scenario": self._scenario.value,
                        "catalog": catalog,
                        "schema": schema_name,
                    },
                )
