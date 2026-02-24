# src/handlers/read/named.py
"""
ReadNamedHandler - Execute named queries from registry with multi-format support.

Loads pre-defined SQL from registry and executes with parameter substitution.
Returns QueryResult in the requested data format.
"""

from typing import Any

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.dispatcher.context import ExecutionContext
from src.contracts.enums import DataFormat
from src.infrastructure.connection import execute_query, execute_query_arrow, execute_query_pandas
from src.queries import get_query_loader
from src.contracts.exceptions import ValidationError
from src.security.validators import quote_table_ref
from src.logging import get_logger as get_structured_logger

slog = get_structured_logger(__name__)


def _resolve_manifest_key(name: str) -> str:
    # If caller passes "analytics.foo" or "schema.list_tables" etc, use as-is.
    if "." in name:
        return name
    # Default namespace for named queries.
    return f"analytics.{name}"

class ReadNamedHandler(BaseHandler):
    """Handler for named query execution with multi-format support."""

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, "ReadNamed")
        
        try:
            # 1. Look up query
            query_name = request.table  # overloading table field for query name alias
            manifest_key = _resolve_manifest_key(query_name)
            query = get_query_loader().get(manifest_key)
                
            sql = query.sql
            params = request.where or {}

            if not isinstance(params, dict):
                raise ValidationError(
                    user_message="Invalid parameters",
                    internal_message="Named query parameters must be provided in request.where as a dict",
                )

            # Strict allowlist: caller may only pass parameters declared in the manifest.
            declared = {p.name for p in query.parameters}
            provided = set(params.keys())
            if not declared:
                if provided:
                    raise ValidationError(
                        user_message="Invalid parameters",
                        internal_message=f"Unknown parameters for '{manifest_key}': {', '.join(sorted(provided))}",
                    )
            else:
                unknown = provided - declared
                if unknown:
                    raise ValidationError(
                        user_message="Invalid parameters",
                        internal_message=f"Unknown parameters for '{manifest_key}': {', '.join(sorted(unknown))}",
                    )

            # Apply defaults + validate types/unknowns if manifest declares parameters
            if query.parameters:
                params = query.apply_defaults(params)
                errors = query.validate_params(params)
                if errors:
                    raise ValidationError(
                        user_message="Invalid parameters",
                        internal_message="; ".join(errors),
                    )

            # Safe identifier substitution (table refs cannot be bound parameters)
            # For any parameter declared as table_ref, replace its ":<name>" placeholder.
            for key in query.table_ref_params():
                placeholder = f":{key}"
                if placeholder in sql:
                    if key not in params:
                        raise ValidationError(
                            user_message="Missing parameter",
                            internal_message=f"Missing required table ref parameter: {key}",
                        )
                    table_value = params.pop(key)
                    if not isinstance(table_value, str):
                        raise ValidationError(
                            user_message="Invalid parameter",
                            internal_message=f"Table ref parameter '{key}' must be a string",
                        )
                    sql = sql.replace(placeholder, quote_table_ref(table_value))
            
            # Log named query execution (name + params only, not full SQL)
            slog.log_sql_execution(
                sql,
                params,
                query_type="NAMED",
                query_name=manifest_key,
            )
            
            # 2. Execute based on requested data_format
            # Skip connection-level logging since we logged above
            data_format = request.data_format
            
            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                if data_format == DataFormat.ARROW:
                    arrow_table = execute_query_arrow(conn, sql, params, skip_logging=True)
                    row_count = arrow_table.num_rows
                    columns = tuple(arrow_table.column_names)
                    
                    self._log_success("ReadNamed", f"Fetched {row_count} rows (ARROW) using template '{query_name}'")
                    
                    return QueryResult(
                        arrow_table=arrow_table,
                        columns=columns,
                        row_count=row_count,
                        data_format=DataFormat.ARROW,
                        metadata={"query_name": query_name, "query_key": query.key},
                    )
                    
                elif data_format == DataFormat.PANDAS:
                    dataframe = execute_query_pandas(conn, sql, params, skip_logging=True)
                    row_count = len(dataframe)
                    columns = tuple(dataframe.columns.tolist())
                    
                    self._log_success("ReadNamed", f"Fetched {row_count} rows (PANDAS) using template '{query_name}'")
                    
                    return QueryResult(
                        dataframe=dataframe,
                        columns=columns,
                        row_count=row_count,
                        data_format=DataFormat.PANDAS,
                        metadata={"query_name": query_name, "query_key": query.key},
                    )
                    
                else:
                    # Default: JSON_ROWS
                    rows = execute_query(conn, sql, params, skip_logging=True)
                    data_rows = tuple(
                        row.asDict() if hasattr(row, "asDict") else dict(row) 
                        for row in rows
                    )
                    columns = tuple(data_rows[0].keys()) if data_rows else ()
                    
                    self._log_success("ReadNamed", f"Fetched {len(data_rows)} rows (JSON_ROWS) using template '{query_name}'")
                    
                    return QueryResult(
                        rows=data_rows,
                        columns=columns,
                        row_count=len(data_rows),
                        data_format=DataFormat.JSON_ROWS,
                        metadata={"query_name": query_name, "query_key": query.key},
                    )

        except Exception as e:
            return self._handle_error(ctx, "ReadNamed", e)
