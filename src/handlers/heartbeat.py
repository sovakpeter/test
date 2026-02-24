# src/handlers/heartbeat.py
"""
HeartbeatHandler - System health check.

Simple handler that returns success to verify connectivity/availability.
Returns OperationResponse (not QueryResult) since it's not a data query.
"""

from src.handlers.base import BaseHandler
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.dispatcher.context import ExecutionContext
from src.infrastructure.connection import execute_query

class HeartbeatHandler(BaseHandler):
    """Handler for system heartbeat/health check."""

    def handle(self, ctx: ExecutionContext, request: OperationRequest) -> QueryResult | OperationResponse:
        self._log_start(ctx, request, "Heartbeat")

        ping: dict[str, object] = {"attempted": False, "ok": None}

        # Best-effort DB connectivity ping.
        # - Uses OBO identity (user context) since SP token may not be configured.
        # - Skips in unit tests / mocks where no real cursor exists.
        try:
            with ctx.get_connection(use_obo=True) as conn:
                if conn is not None and hasattr(conn, "cursor"):
                    ping["attempted"] = True
                    execute_query(conn, "SELECT 1")
                    ping["ok"] = True
        except Exception:
            ping["attempted"] = True
            ping["ok"] = False
            raise

        # Heartbeat returns OperationResponse (not QueryResult) since it's metadata, not data
        return OperationResponse(
            success=True,
            message="System is online",
            data={"status": "ok", "service": "CRUD Engine", "db_ping": ping},
        )
