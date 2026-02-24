# src/handlers/transaction/multi_op.py
"""
TransactionHandler - Atomic Multi-Step Operations.

Handles execution of multiple operations within a single database transaction.
Returns OperationResponse with aggregate affected_rows (mutations don't return data).
Per architecture: shapes the final aggregate only, not individual sub-operations.
"""

from typing import Any

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import transaction_context, execute_non_query
from src.transformers.sql_builders import build_write_query, build_delete_query
from src.security.validators import validate_mutation_safety
from src.contracts.enums import OperationType
from src.contracts.enums import ProcessingMode
from src.contracts.exceptions import ValidationError

class TransactionHandler(BaseHandler):
    """Handler for multi-operation atomic transactions."""

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, "Transaction")

        if request.operation != OperationType.TRANSACTION:
            raise ValidationError(
                user_message="Invalid transaction request",
                internal_message=f"TransactionHandler received operation={request.operation}",
            )

        if request.mode != ProcessingMode.SINGLE:
            raise ValidationError(
                user_message="Transaction wrapper mode must be SINGLE",
                internal_message=f"Transaction wrapper mode must be SINGLE (got {request.mode})",
            )

        if not request.operations:
            return OperationResponse(success=True, message="Empty transaction (no ops)")

        for idx, op_request in enumerate(request.operations):
            if op_request.operation == OperationType.TRANSACTION:
                raise ValidationError(
                    user_message="Nested transactions are not supported",
                    internal_message=f"Nested transaction at index {idx}",
                )
            if op_request.mode != ProcessingMode.SINGLE:
                raise ValidationError(
                    user_message="Transaction sub-operations must use SINGLE mode",
                    internal_message=f"Sub-op {idx} has mode={op_request.mode}",
                )
            if isinstance(op_request.payload, list):
                raise ValidationError(
                    user_message="Transaction sub-operations require dict payloads",
                    internal_message=f"Sub-op {idx} has list payload (batch) which is not supported",
                )

        total_affected = 0
        
        try:
            # Execute all operations within a single atomic block
            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                with transaction_context(conn):
                    for op_request in request.operations:
                        # Validate and Build SQL for each op
                        # Note: This effectively dispatches locally for now. 
                        # In a fuller implementation, we might delegate back to Dispatcher,
                        # but sharing the SAME connection requires passing 'conn' explicitly,
                        # which the standard handle(ctx) interface doesn't support directly without modification.
                        # For Phase 9 MVP, we implement the logic inline here for simplicity and safety.
                        
                        validate_mutation_safety(op_request.operation, op_request.payload, op_request.where)

                        if op_request.operation in (OperationType.INSERT, OperationType.UPDATE, OperationType.MERGE):
                            sql, params = build_write_query(op_request)
                            affected = execute_non_query(conn, sql, params)
                            total_affected += affected

                        elif op_request.operation == OperationType.DELETE:
                            sql, params = build_delete_query(op_request)
                            affected = execute_non_query(conn, sql, params)
                            total_affected += affected

                        else:
                            raise ValueError(f"Unsupported operation in transaction: {op_request.operation}")

            self._log_success("Transaction", f"Committed {len(request.operations)} ops, Affected {total_affected} rows")
            
            return OperationResponse(
                success=True,
                affected_rows=total_affected,
                message="Transaction committed successfully"
            )

        except Exception as e:
            return self._handle_error(ctx, "Transaction", e)
