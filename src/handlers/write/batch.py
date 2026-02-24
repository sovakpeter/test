# src/handlers/write/batch.py
"""
WriteBatchHandler - Insert or update multiple records.

- INSERT batch: Uses cursor.executemany() for efficient bulk inserts.
- UPDATE/MERGE batch: Loops through records, executing each individually
  within a shared transaction (each record has its own PK values).

Returns OperationResponse with aggregate affected_rows.
"""

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.contracts.enums import OperationType
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import (
    execute_non_query,
    execute_non_query_many,
    transaction_context,
)
from src.transformers.sql_builders import build_write_query


class WriteBatchHandler(BaseHandler):
    """Handler for writing multiple records."""

    def handle(
        self,
        ctx: ExecutionContext,
        request: OperationRequest
    ) -> QueryResult | OperationResponse:
        """
        Insert or update multiple records.

        INSERT uses executemany for efficiency.
        UPDATE/MERGE loops through records individually within a transaction.

        Args:
            ctx: Execution context.
            request: Request with table and payload (list of dicts).

        Returns:
            OperationResponse with total affected rows.
        """
        self._log_start(ctx, request, "WriteBatch")

        try:
            records = request.payload
            if not isinstance(records, list) or len(records) == 0:
                return OperationResponse(
                    success=False,
                    message="Batch write requires a non-empty list payload",
                    metadata={"correlation_id": ctx.correlation_id},
                )

            if request.operation == OperationType.INSERT:
                return self._batch_insert(ctx, request, records)
            else:
                return self._batch_update_merge(ctx, request, records)

        except Exception as e:
            return self._handle_error(ctx, "WriteBatch", e)

    def _batch_insert(
        self,
        ctx: ExecutionContext,
        request: OperationRequest,
        records: list[dict],
    ) -> OperationResponse:
        """
        Bulk INSERT using executemany.

        Builds the INSERT template from the first record's columns, then
        sends all records via executemany for a single round-trip.
        """
        # Build SQL template from the first record (all records must have same columns)
        first_record_request = OperationRequest(
            operation=OperationType.INSERT,
            table=request.table,
            payload=records[0],
        )
        sql_template, _ = build_write_query(first_record_request)

        # Build param dicts for each record using same param naming convention
        # The INSERT builder uses v_{col} as param names
        param_list = []
        for record in records:
            params = {f"v_{col}": val for col, val in record.items()}
            param_list.append(params)

        with ctx.pool.get_connection(token=ctx.obo_token) as conn:
            with transaction_context(conn):
                affected = execute_non_query_many(conn, sql_template, param_list)

        self._log_success("WriteBatch", f"Bulk INSERT: {len(records)} records, affected {affected} rows")

        return OperationResponse(
            success=True,
            affected_rows=affected,
            message=f"Batch INSERT completed: {len(records)} records submitted",
            metadata={"correlation_id": ctx.correlation_id, "batch_size": len(records)},
        )

    def _batch_update_merge(
        self,
        ctx: ExecutionContext,
        request: OperationRequest,
        records: list[dict],
    ) -> OperationResponse:
        """
        Batch UPDATE/MERGE by looping through records within a single transaction.

        Each record is treated as a separate UPDATE/MERGE statement because
        each has its own PK values. All execute on the same connection within
        a single transaction for atomicity.

        WHERE can be:
        - dict: shared conditions applied to every record (backwards compatible)
        - list[dict]: per-record conditions, one dict per payload record
        """
        total_affected = 0
        per_record_where = isinstance(request.where, list)

        with ctx.pool.get_connection(token=ctx.obo_token) as conn:
            with transaction_context(conn):
                for i, record in enumerate(records):
                    record_where = request.where[i] if per_record_where else request.where
                    sub_request = OperationRequest(
                        operation=request.operation,
                        table=request.table,
                        payload=record,
                        where=record_where,
                    )
                    sql, params = build_write_query(sub_request)
                    affected = execute_non_query(conn, sql, params)
                    total_affected += affected

        op_name = request.operation.value if request.operation else "WRITE"
        self._log_success("WriteBatch", f"Batch {op_name}: {len(records)} records, affected {total_affected} rows")

        return OperationResponse(
            success=True,
            affected_rows=total_affected,
            message=f"Batch {op_name} completed: {len(records)} records processed",
            metadata={"correlation_id": ctx.correlation_id, "batch_size": len(records)},
        )
