# src/handlers/read/single.py
"""
ReadSingleHandler - Fetch records with multi-format support.

Handles SELECT operations and returns QueryResult in the requested data format.
Format conversion to UI format is handled centrally by lifecycle.
"""

from typing import Any

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.dispatcher.context import ExecutionContext
from src.contracts.enums import DataFormat
from src.infrastructure.connection import execute_query, execute_query_arrow, execute_query_pandas
from src.transformers.sql_builders import build_select_query


class ReadSingleHandler(BaseHandler):
    """Handler for read operations with multi-format support."""

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, "ReadSingle")
        
        try:
            # 1. Build SQL
            sql, params = build_select_query(request)
            
            # 2. Execute Query based on requested data_format
            data_format = request.data_format
            
            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                if data_format == DataFormat.ARROW:
                    arrow_table = execute_query_arrow(conn, sql, params)
                    row_count = arrow_table.num_rows
                    columns = tuple(arrow_table.column_names)
                    
                    self._log_success("ReadSingle", f"Fetched {row_count} rows (ARROW)")
                    
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
                    
                    self._log_success("ReadSingle", f"Fetched {row_count} rows (PANDAS)")
                    
                    return QueryResult(
                        dataframe=dataframe,
                        columns=columns,
                        row_count=row_count,
                        data_format=DataFormat.PANDAS,
                    )
                    
                else:
                    # Default: JSON_ROWS
                    rows = execute_query(conn, sql, params)
                    data_rows = tuple(
                        row.asDict() if hasattr(row, "asDict") else dict(row) 
                        for row in rows
                    )
                    columns = tuple(data_rows[0].keys()) if data_rows else ()
                    
                    self._log_success("ReadSingle", f"Fetched {len(data_rows)} rows (JSON_ROWS)")
                    
                    return QueryResult(
                        rows=data_rows,
                        columns=columns,
                        row_count=len(data_rows),
                        data_format=DataFormat.JSON_ROWS,
                    )

        except Exception as e:
            return self._handle_error(ctx, "ReadSingle", e)
