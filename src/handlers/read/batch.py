# src/handlers/read/batch.py
"""
ReadBatchHandler - Multi-key lookup for multiple records.

Accepts a list of PK dicts in request.payload and builds a SELECT with
OR-combined WHERE conditions to fetch all matching rows in a single query.
Supports all data formats (JSON_ROWS, ARROW, PANDAS).

Returns QueryResult for lifecycle format conversion.
"""

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.contracts.enums import DataFormat
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import execute_query, execute_query_arrow, execute_query_pandas
from src.engine.models import SelectIntent, FilterClause
from src.engine.validators import validate_select
from src.engine.builders import build_select, _combine_and, _combine_or, _make_column, _make_placeholder
from sqlglot import expressions as exp
from src.engine.config import DEFAULT_DIALECT, get_default_limit, get_global_max_limit
from src.engine.builders import _normalize_placeholders
from typing import Any


class ReadBatchHandler(BaseHandler):
    """Handler for reading multiple records by PK lookup."""

    def handle(
        self,
        ctx: ExecutionContext,
        request: OperationRequest
    ) -> QueryResult | OperationResponse:
        """
        Fetch multiple records by PK lookup.

        request.payload contains a list of PK dicts. Each dict maps
        PK column names to values. Builds a single SELECT with OR-combined
        WHERE conditions.

        Args:
            ctx: Execution context.
            request: Request with table, columns, payload (list of PK dicts).

        Returns:
            QueryResult with matching records.
        """
        self._log_start(ctx, request, "ReadBatch")

        try:
            pk_sets = request.payload
            if not isinstance(pk_sets, list) or len(pk_sets) == 0:
                return OperationResponse(
                    success=False,
                    message="Batch read requires a non-empty list payload of PK dicts",
                    metadata={"correlation_id": ctx.correlation_id},
                )

            sql, params = self._build_batch_select(request, pk_sets)

            data_format = request.data_format

            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                if data_format == DataFormat.ARROW:
                    arrow_table = execute_query_arrow(conn, sql, params)
                    row_count = arrow_table.num_rows
                    columns = tuple(arrow_table.column_names)
                    self._log_success("ReadBatch", f"Fetched {row_count} rows (ARROW)")
                    return QueryResult(
                        arrow_table=arrow_table,
                        columns=columns,
                        row_count=row_count,
                        data_format=DataFormat.ARROW,
                    )

                elif data_format == DataFormat.PANDAS:
                    dataframe = execute_query_pandas(conn, sql, params)
                    row_count = len(dataframe)
                    columns = tuple(dataframe.columns.tolist())
                    self._log_success("ReadBatch", f"Fetched {row_count} rows (PANDAS)")
                    return QueryResult(
                        dataframe=dataframe,
                        columns=columns,
                        row_count=row_count,
                        data_format=DataFormat.PANDAS,
                    )

                else:
                    rows = execute_query(conn, sql, params)
                    data_rows = tuple(
                        row.asDict() if hasattr(row, "asDict") else dict(row)
                        for row in rows
                    )
                    columns = tuple(data_rows[0].keys()) if data_rows else ()
                    self._log_success("ReadBatch", f"Fetched {len(data_rows)} rows (JSON_ROWS)")
                    return QueryResult(
                        rows=data_rows,
                        columns=columns,
                        row_count=len(data_rows),
                        data_format=DataFormat.JSON_ROWS,
                    )

        except Exception as e:
            return self._handle_error(ctx, "ReadBatch", e)

    def _build_batch_select(
        self,
        request: OperationRequest,
        pk_sets: list[dict],
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a SELECT with OR-combined PK conditions.

        Each PK set becomes an AND-combined equality condition.
        Multiple PK sets are OR-combined.
        """
        params: dict[str, Any] = {}

        # Columns
        columns = request.columns
        if not columns or columns == ["*"]:
            query = exp.Select().select("*")
        else:
            select_cols = [exp.Column(this=exp.to_identifier(c, quoted=True)) for c in columns]
            query = exp.Select().select(*select_cols)

        # FROM
        query = query.from_(request.table)

        # WHERE: OR-combine each PK set
        row_conditions = []
        for i, pk_dict in enumerate(pk_sets):
            pk_exprs = []
            for col, val in pk_dict.items():
                param_name = f"bp_{col}_{i}"
                params[param_name] = val
                pk_exprs.append(
                    exp.EQ(
                        this=_make_column(col),
                        expression=_make_placeholder(param_name),
                    )
                )

            if len(pk_exprs) == 1:
                row_conditions.append(pk_exprs[0])
            else:
                row_conditions.append(exp.Paren(this=_combine_and(pk_exprs)))

        if len(row_conditions) == 1:
            where_expr = row_conditions[0]
        else:
            where_expr = _combine_or(row_conditions)

        query = query.where(where_expr)

        # LIMIT
        limit = request.options.get("limit") if request.options else None
        if limit is None:
            limit = get_default_limit()
        limit = min(int(limit), get_global_max_limit())
        query = query.limit(limit)

        # ORDER BY
        if request.options and "order_by" in request.options:
            for ob in request.options["order_by"]:
                if isinstance(ob, dict):
                    col_name = ob["column"]
                    direction = ob.get("direction", "ASC")
                else:
                    col_name = ob.column if hasattr(ob, "column") else ob[0]
                    direction = getattr(ob, "direction", "ASC") if hasattr(ob, "direction") else ob[1]
                col_expr = _make_column(col_name)
                ordered = exp.Ordered(this=col_expr, desc=(direction.upper() == "DESC"))
                query = query.order_by(ordered)

        sql = query.sql(dialect=DEFAULT_DIALECT)
        sql = _normalize_placeholders(sql)

        return sql, params
