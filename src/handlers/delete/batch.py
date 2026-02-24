# src/handlers/delete/batch.py
"""
DeleteBatchHandler - Delete multiple records by PK.

Accepts a list of PK dicts in request.payload and builds a single DELETE
statement with OR-combined WHERE conditions. Executes within a transaction.

Returns OperationResponse with aggregate affected_rows.
"""

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.contracts.enums import OperationType
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import execute_non_query, transaction_context
from src.engine.models import DeleteIntent
from src.engine.validators import validate_delete
from src.engine.builders import build_delete


class DeleteBatchHandler(BaseHandler):
    """Handler for deleting multiple records."""

    def handle(
        self,
        ctx: ExecutionContext,
        request: OperationRequest
    ) -> QueryResult | OperationResponse:
        """
        Delete multiple records by PK.

        Accepts request.payload as list[dict] where each dict contains
        PK column(s) and their values for one row to delete.

        Args:
            ctx: Execution context.
            request: Request with table and payload (list of PK dicts).

        Returns:
            OperationResponse with total affected rows.
        """
        self._log_start(ctx, request, "DeleteBatch")

        try:
            records = request.payload
            if not isinstance(records, list) or len(records) == 0:
                return OperationResponse(
                    success=False,
                    message="Batch delete requires a non-empty list payload",
                    metadata={"correlation_id": ctx.correlation_id},
                )

            # Build a single DELETE with OR-combined PKs
            # The DeleteIntent model normalises dict → list and the builder
            # generates OR-combined WHERE clauses for batch deletes.
            intent = DeleteIntent(table=request.table, pk_values=records)
            validate_delete(intent)
            sql, params = build_delete(intent)

            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                with transaction_context(conn):
                    affected = execute_non_query(conn, sql, params)

            if affected == 0:
                self._log_success("DeleteBatch", "Affected 0 rows - no matching records")
                return OperationResponse(
                    success=False,
                    affected_rows=0,
                    message="Batch DELETE affected 0 rows - no matching records found",
                    metadata={"correlation_id": ctx.correlation_id, "batch_size": len(records)},
                )

            self._log_success("DeleteBatch", f"Deleted {affected} rows from {len(records)} PK sets")

            return OperationResponse(
                success=True,
                affected_rows=affected,
                message=f"Batch DELETE completed: {len(records)} PK sets processed",
                metadata={"correlation_id": ctx.correlation_id, "batch_size": len(records)},
            )

        except Exception as e:
            return self._handle_error(ctx, "DeleteBatch", e)
