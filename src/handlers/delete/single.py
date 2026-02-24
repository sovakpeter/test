# src/handlers/delete/single.py
"""
DeleteSingleHandler - Delete a single record.

Handles DELETE operations with strict Primary Key enforcement.
Returns OperationResponse (not QueryResult) since mutations return affected_rows, not data.

Rowcount-based success detection (Databricks-specific):
- rowcount == -1 → Success (Databricks default, does not report accurate counts)
- rowcount == 0  → Failure (no rows deleted, record not found)
- rowcount > 0   → Success (standard SQL behavior)
"""

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import execute_non_query, transaction_context
from src.transformers.sql_builders import build_delete_query
from src.security.validators import validate_mutation_safety

class DeleteSingleHandler(BaseHandler):
    """Handler for delete operations."""

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, "DeleteSingle")

        try:
            # 1. Security Check
            # DELETE requires PK columns and values to prevent accidental broad deletions
            validate_mutation_safety(request.operation, request.payload, request.where)

            # 2. Build SQL
            sql, params = build_delete_query(request)
            
            # 3. Execute Non-Query
            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                # Always execute mutation operations in an explicit transaction.
                with transaction_context(conn):
                    affected = execute_non_query(conn, sql, params)
            
            # 4. Rowcount-based success detection
            # Databricks-specific behavior:
            #   rowcount == -1 → Success (Databricks default)
            #   rowcount == 0  → Failure (no rows matched)
            #   rowcount > 0   → Success (standard SQL behavior)
            if affected == 0:
                self._log_success("DeleteSingle", f"Affected 0 rows - treating as failure")
                return OperationResponse(
                    success=False,
                    affected_rows=0,
                    message="DELETE affected 0 rows - no matching record found"
                )
            
            self._log_success("DeleteSingle", f"Affected {affected} rows")
            
            # Mutations return OperationResponse with affected_rows (no format conversion)
            return OperationResponse(
                success=True,
                affected_rows=affected,
                message="Delete operation successful"
            )

        except Exception as e:
            return self._handle_error(ctx, "DeleteSingle", e)
