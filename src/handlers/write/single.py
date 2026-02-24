# src/handlers/write/single.py
"""
WriteSingleHandler - Insert, Update, or Merge a single record.

Handles mutation operations with safety checks and SQL generation.
Returns OperationResponse (not QueryResult) since mutations return affected_rows, not data.

Rowcount-based success detection (Databricks-specific):
- rowcount == -1 → Success (Databricks default, does not report accurate counts)
- rowcount == 0  → Failure (no rows inserted/updated, possible conflict)
- rowcount > 0   → Success (standard SQL behavior)
"""

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.contracts.enums import OperationType
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import execute_non_query, transaction_context
from src.transformers.sql_builders import build_write_query
from src.security.validators import validate_mutation_safety

class WriteSingleHandler(BaseHandler):
    """Handler for write operations (INSERT, UPDATE, MERGE)."""

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, "WriteSingle")

        try:
            # 1. Security Check (Mutation Invariants)
            # Enforces PK persistence for UPDATE/MERGE, Values for INSERT
            validate_mutation_safety(request.operation, request.payload, request.where)

            # 2. Build SQL
            # Transformers handle the dialect-specific SQL generation
            sql, params = build_write_query(request)
            
            # 3. Execute Non-Query
            with ctx.pool.get_connection(token=ctx.obo_token) as conn:
                # Always execute mutation operations in an explicit transaction.
                # For single statements this is typically redundant but provides consistent
                # commit/rollback behavior aligned with multi-op transaction semantics.
                with transaction_context(conn):
                    affected = execute_non_query(conn, sql, params)
            
            # 4. Rowcount-based success detection
            # Databricks-specific behavior:
            #   rowcount == -1 → Success (Databricks default)
            #   rowcount == 0  → Failure (no rows matched/inserted)
            #   rowcount > 0   → Success (standard SQL behavior)
            if affected == 0:
                op_name = request.operation.value if request.operation else "WRITE"
                has_occ = bool(request.options and request.options.get("old_values"))
                if has_occ:
                    msg = (
                        f"{op_name} affected 0 rows - record was modified by "
                        f"another user (concurrency conflict)"
                    )
                else:
                    msg = f"{op_name} affected 0 rows - no matching record or conflict detected"
                self._log_success("WriteSingle", f"Affected 0 rows - treating as failure")
                return OperationResponse(
                    success=False,
                    affected_rows=0,
                    message=msg,
                )
            
            self._log_success("WriteSingle", f"Affected {affected} rows")
            
            # Mutations return OperationResponse with affected_rows (no format conversion)
            return OperationResponse(
                success=True,
                affected_rows=affected,
                message="Write operation successful"
            )

        except Exception as e:
            return self._handle_error(ctx, "WriteSingle", e)
