"""src/logging/context.py

Request-scoped logging context using ContextVars.

This is intentionally framework-agnostic (no Dash/Flask imports).
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from enum import Enum


class OperationScenario(str, Enum):
    HEARTBEAT = "HEARTBEAT"
    READ_SINGLE = "READ.SINGLE"
    READ_NAMED = "READ.NAMED"
    READ_BATCH = "READ.BATCH"
    INSERT_SINGLE = "INSERT.SINGLE"
    UPDATE_SINGLE = "UPDATE.SINGLE"
    MERGE_SINGLE = "MERGE.SINGLE"
    DELETE_SINGLE = "DELETE.SINGLE"
    INSERT_BATCH = "INSERT.BATCH"
    UPDATE_BATCH = "UPDATE.BATCH"
    MERGE_BATCH = "MERGE.BATCH"
    DELETE_BATCH = "DELETE.BATCH"
    TRANSACTION = "TRANSACTION"
    SCHEMA = "SCHEMA"
    UNKNOWN = "UNKNOWN"


class LifecyclePhase(str, Enum):
    VALIDATE = "VALIDATE"
    THROTTLE = "THROTTLE"
    AUTHN = "AUTHN"
    AUTHZ = "AUTHZ"
    ROUTE = "ROUTE"
    WARMUP = "WARMUP"
    RESOLVE = "RESOLVE"  # Schema resolution for columns="*"
    EXECUTE = "EXECUTE"
    SHAPE = "SHAPE"
    OBSERVE = "OBSERVE"


@dataclass(frozen=True, slots=True)
class RequestContext:
    correlation_id: str
    scenario: OperationScenario = OperationScenario.UNKNOWN
    table: str = ""
    user_email: str = "anonymous"
    auth_method: str = "UNKNOWN"
    phase: LifecyclePhase | None = None


_request_context: ContextVar[RequestContext | None] = ContextVar("request_context", default=None)
_ui_session_id: ContextVar[str | None] = ContextVar("ui_session_id", default=None)


def get_request_context() -> RequestContext | None:
    return _request_context.get()


def set_request_context(ctx: RequestContext) -> None:
    _request_context.set(ctx)


def clear_request_context() -> None:
    _request_context.set(None)


def update_phase(phase: LifecyclePhase | None) -> None:
    ctx = _request_context.get()
    if ctx is None:
        return
    _request_context.set(
        RequestContext(
            correlation_id=ctx.correlation_id,
            scenario=ctx.scenario,
            table=ctx.table,
            user_email=ctx.user_email,
            auth_method=ctx.auth_method,
            phase=phase,
        )
    )


def get_correlation_id() -> str:
    ctx = _request_context.get()
    return ctx.correlation_id if ctx else "no-request"


def get_user_email() -> str:
    ctx = _request_context.get()
    return ctx.user_email if ctx else "anonymous"


def get_scenario() -> OperationScenario:
    ctx = _request_context.get()
    return ctx.scenario if ctx else OperationScenario.UNKNOWN


def get_phase() -> LifecyclePhase | None:
    ctx = _request_context.get()
    return ctx.phase if ctx else None


def get_table() -> str:
    ctx = _request_context.get()
    return ctx.table if ctx else ""


def get_auth_method() -> str:
    ctx = _request_context.get()
    return ctx.auth_method if ctx else "UNKNOWN"


def get_ui_session_id() -> str | None:
    return _ui_session_id.get()


def set_ui_session_id(session_id: str | None) -> None:
    _ui_session_id.set(session_id)
