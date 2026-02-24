"""src/dispatcher/lifecycle.py

Centralized request lifecycle enforcement.

Stages (in order):
 1) VALIDATE   - Structural + semantic validation (no DB)
 2) THROTTLE   - Per-session rate limiting (connection storm protection)
 3) AUTHN      - Identity presence checks (soft for now)
 4) AUTHZ      - Not implemented here (Unity Catalog + OBO assumed)
 5) ROUTE      - Resolve handler
 6) WARMUP     - Best-effort warehouse warmup gate (SP-only, TTL/lock)
 7) RESOLVE    - Schema resolution for columns="*" (cached)
 8) EXECUTE    - Run handler (returns QueryResult), wrapped by admission gate
 9) SHAPE      - Convert format + build OperationResponse (centralized)
10) OBSERVE    - Log outcome + timings

This module is framework-agnostic and is used only by the dispatcher/manager.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from src.services.schema_resolver import ResolvedSchema, SchemaResolver

from src.contracts.enums import OperationType, ProcessingMode, SchemaScenario
from src.contracts.exceptions import AdmissionError, OperationError, ThrottleError, ValidationError
from src.contracts.requests import OperationRequest
from src.contracts.responses import OperationResponse, QueryResult
from src.contracts.errors import error_detail_from_exception, unknown_error_detail
from src.dispatcher.context import ExecutionContext
from src.dispatcher.router import OperationRouter
from src.infrastructure.warmup import WarehouseWarmupGate
from src.infrastructure.logging import get_logger
from src.infrastructure.config import get_settings
from src.logging import get_logger as get_structured_logger
from src.logging.context import (
    LifecyclePhase,
    OperationScenario,
    RequestContext,
    clear_request_context,
    get_ui_session_id,
    set_request_context,
    update_phase,
)
from src.infrastructure.rate_limit import SessionRateLimiter
from src.infrastructure.admission import AdmissionGate
from src.security.validators import (
    validate_columns,
    validate_mutation_safety,
    validate_table_name,
)
from src.infrastructure.auth import (
    AuthMethod,
    detect_auth_method,
    get_current_user_email,
)
from src.transformers.format_converters import query_result_to_data, resolve_ui_format

logger = get_logger(__name__)
slog = get_structured_logger(__name__)


@dataclass(frozen=True, slots=True)
class LifecycleMetrics:
    validation_ms: float = 0.0
    routing_ms: float = 0.0
    execution_ms: float = 0.0
    total_ms: float = 0.0


class RequestLifecycle:
    """Enforces the centralized lifecycle contract for every request."""

    def __init__(self, router: OperationRouter | None = None):
        self._router = router or OperationRouter()
        self._warmup_gate = WarehouseWarmupGate()
        self._schema_resolver: "SchemaResolver | None" = None

        settings = get_settings()
        self._rate_limiter = SessionRateLimiter(
            max_requests=settings.rate_limit_requests,
            window_seconds=settings.rate_limit_window_seconds,
        )
        self._admission_gate = AdmissionGate(
            max_concurrent=settings.max_concurrent_queries,
        )
        self._last_cleanup = time.monotonic()
        self._cleanup_interval = float(settings.rate_limit_cleanup_interval_seconds)

    def process(self, request: OperationRequest, ctx: ExecutionContext) -> OperationResponse:
        start = time.perf_counter()
        metrics = LifecycleMetrics()

        scenario = self._scenario_for(request)
        
        # Detect auth method and user identity from headers early
        auth_method = detect_auth_method(ctx.headers)
        user_email = get_current_user_email(ctx.headers)
        
        set_request_context(
            RequestContext(
                correlation_id=ctx.correlation_id,
                scenario=scenario,
                table=request.table,
                user_email=user_email,
                auth_method=auth_method.value,
                phase=None,
            )
        )

        response: OperationResponse | None = None

        slog.request_start(
            correlation_id=ctx.correlation_id,
            scenario=scenario,
            table=request.table,
            user_email=user_email,
        )
        
        # Log full UI intent message (OperationRequest)
        slog.log_intent(request, ctx.correlation_id)

        try:
            t0 = time.perf_counter()
            update_phase(LifecyclePhase.VALIDATE)
            slog.phase_header(LifecyclePhase.VALIDATE)
            self._validate(request)

            update_phase(LifecyclePhase.THROTTLE)
            slog.phase_header(LifecyclePhase.THROTTLE)
            self._throttle(request, ctx)

            update_phase(LifecyclePhase.AUTHN)
            slog.phase_header(LifecyclePhase.AUTHN)
            self._authenticate(request, ctx)
            metrics = replace(metrics, validation_ms=(time.perf_counter() - t0) * 1000.0)

            t1 = time.perf_counter()
            update_phase(LifecyclePhase.ROUTE)
            slog.phase_header(LifecyclePhase.ROUTE)
            handler = self._router.resolve(request)
            metrics = replace(metrics, routing_ms=(time.perf_counter() - t1) * 1000.0)

            t2 = time.perf_counter()
            update_phase(LifecyclePhase.WARMUP)
            self._maybe_warm_warehouse(request, ctx)

            # RESOLVE phase: resolve schema for columns="*"
            update_phase(LifecyclePhase.RESOLVE)
            resolved_schema = self._maybe_resolve_schema(request, ctx)

            # Admission gate: limit concurrent DB queries
            needs_admission = request.operation not in (
                OperationType.HEARTBEAT,
                OperationType.SCHEMA,
            )
            if needs_admission and not self._admission_gate.try_acquire():
                raise AdmissionError(
                    user_message="Server is busy — too many concurrent queries. Please retry shortly.",
                    internal_message=f"Admission gate full (corr={ctx.correlation_id})",
                )

            try:
                update_phase(LifecyclePhase.EXECUTE)
                slog.phase_header(LifecyclePhase.EXECUTE)
                handler_result = handler.handle(ctx, request)
            finally:
                if needs_admission:
                    self._admission_gate.release()
            metrics = replace(metrics, execution_ms=(time.perf_counter() - t2) * 1000.0)

            update_phase(LifecyclePhase.SHAPE)
            # Handler may return QueryResult (new) or OperationResponse (legacy/mutations)
            response = self._shape_response(handler_result, request, ctx, resolved_schema)
            metrics = replace(metrics, total_ms=(time.perf_counter() - start) * 1000.0)

            update_phase(LifecyclePhase.OBSERVE)
            self._observe(request, response, ctx, metrics)
            return response

        except Exception as exc:
            metrics = replace(metrics, total_ms=(time.perf_counter() - start) * 1000.0)
            response = self._error_response(exc, ctx)
            update_phase(LifecyclePhase.OBSERVE)
            self._observe(request, response, ctx, metrics)
            return response

        finally:
            try:
                if response is not None:
                    slog.request_end(
                        correlation_id=ctx.correlation_id,
                        success=bool(response.success),
                        duration_ms=metrics.total_ms,
                        affected_rows=None,
                    )
            finally:
                update_phase(None)
                clear_request_context()

    def _scenario_for(self, request: OperationRequest) -> OperationScenario:
        # Keep this mapping local to the lifecycle to avoid coupling contracts <-> logging.
        if request.operation == OperationType.HEARTBEAT:
            return OperationScenario.HEARTBEAT
        if request.operation == OperationType.TRANSACTION:
            return OperationScenario.TRANSACTION
        if request.operation == OperationType.SCHEMA:
            return OperationScenario.SCHEMA
        if request.operation == OperationType.READ:
            if request.mode == ProcessingMode.NAMED:
                return OperationScenario.READ_NAMED
            if request.mode == ProcessingMode.BATCH:
                return OperationScenario.READ_BATCH
            return OperationScenario.READ_SINGLE
        if request.operation == OperationType.INSERT:
            if request.mode == ProcessingMode.BATCH:
                return OperationScenario.INSERT_BATCH
            return OperationScenario.INSERT_SINGLE
        if request.operation == OperationType.UPDATE:
            if request.mode == ProcessingMode.BATCH:
                return OperationScenario.UPDATE_BATCH
            return OperationScenario.UPDATE_SINGLE
        if request.operation == OperationType.MERGE:
            if request.mode == ProcessingMode.BATCH:
                return OperationScenario.MERGE_BATCH
            return OperationScenario.MERGE_SINGLE
        if request.operation == OperationType.DELETE:
            if request.mode == ProcessingMode.BATCH:
                return OperationScenario.DELETE_BATCH
            return OperationScenario.DELETE_SINGLE
        return OperationScenario.UNKNOWN

    def _maybe_warm_warehouse(self, request: OperationRequest, ctx: ExecutionContext) -> None:
        # Heartbeat stays non-DB.
        if request.operation == OperationType.HEARTBEAT:
            return

        # Best-effort warmup gate; swallow any failures.
        self._warmup_gate.maybe_warm(pool=ctx.pool, correlation_id=ctx.correlation_id)

    # ----------------------
    # Stage 1.5: Throttle
    # ----------------------

    def _throttle(self, request: OperationRequest, ctx: ExecutionContext) -> None:
        """Per-session rate limiting. Skips HEARTBEAT (no DB work)."""
        if request.operation == OperationType.HEARTBEAT:
            return

        session_id = get_ui_session_id()
        if not self._rate_limiter.check(session_id):
            settings = get_settings()
            raise ThrottleError(
                user_message=(
                    f"Too many requests — please wait a few seconds before retrying. "
                    f"(limit: {settings.rate_limit_requests} per {settings.rate_limit_window_seconds}s)"
                ),
                internal_message=(
                    f"Rate limit exceeded for session={session_id} corr={ctx.correlation_id}"
                ),
            )

    def _get_schema_resolver(self, ctx: ExecutionContext) -> "SchemaResolver":
        """Lazy-initialize schema resolver."""
        if self._schema_resolver is None:
            from src.metadata.schema import SchemaProvider
            from src.services.schema_resolver import SchemaResolver
            
            settings = get_settings()
            provider = SchemaProvider(
                cache_dir=Path("cache/schema"),
                pool=ctx.pool,
                ttl_seconds=getattr(settings, 'schema_cache_ttl_seconds', 3600),
            )
            self._schema_resolver = SchemaResolver(provider)
        return self._schema_resolver

    def _maybe_resolve_schema(
        self, 
        request: OperationRequest, 
        ctx: ExecutionContext
    ) -> "ResolvedSchema | None":
        """
        Resolve schema if needed (columns="*").
        
        Returns ResolvedSchema or None if resolution not needed.
        Schema is returned separately for merging into final response.
        """
        # Quick check: only READ operations with wildcard columns need resolution
        # This avoids creating the resolver for operations that don't need it
        if request.operation != OperationType.READ:
            return None
        
        # Skip schema resolution for NAMED queries (use predefined SQL files)
        if request.mode == ProcessingMode.NAMED:
            return None
        
        columns = request.columns
        needs_resolution = (
            columns is None or
            columns == ["*"] or
            (isinstance(columns, list) and len(columns) == 1 and columns[0] == "*")
        )
        
        if not needs_resolution:
            return None
        
        try:
            resolver = self._get_schema_resolver(ctx)
            
            logger.debug(
                "RESOLVE: Fetching schema for table=%s corr=%s",
                request.table,
                ctx.correlation_id,
            )
            
            resolved = resolver.resolve(request, ctx)
            
            logger.debug(
                "RESOLVE: Schema resolved with %d columns for %s",
                len(resolved.column_names),
                request.table,
            )
            
            return resolved
            
        except Exception as e:
            # Schema resolution is best-effort; log and continue without schema
            logger.warning(
                "RESOLVE: Schema resolution failed for %s: %s (continuing without schema)",
                request.table,
                e,
            )
            return None

    # -----------------
    # Stage 1: Validate
    # -----------------

    def _validate(self, request: OperationRequest) -> None:
        # Basic structural checks
        if not isinstance(request, OperationRequest):
            raise ValidationError(
                user_message="Invalid request",
                internal_message=f"Expected OperationRequest, got {type(request).__name__}",
            )

        if request.operation is None:
            raise ValidationError(user_message="Missing operation")

        if request.mode is None:
            raise ValidationError(user_message="Missing processing mode")

        if not isinstance(request.options, dict):
            raise ValidationError(
                user_message="Invalid options",
                internal_message="request.options must be a dict",
            )

        # Operation-specific validation
        if request.operation == OperationType.SCHEMA:
            self._validate_schema_request(request)
            return

        if request.operation == OperationType.HEARTBEAT:
            return

        if request.operation == OperationType.TRANSACTION:
            self._validate_transaction_request(request)
            return

        # Named query path: request.table is a manifest key, not a UC table ref
        if request.operation == OperationType.READ and request.mode == ProcessingMode.NAMED:
            if not isinstance(request.table, str) or not request.table.strip():
                raise ValidationError(
                    user_message="Missing query name",
                    internal_message="READ/NAMED uses request.table as manifest key",
                )
            if request.where is not None and not isinstance(request.where, dict):
                raise ValidationError(
                    user_message="Invalid parameters",
                    internal_message="Named query parameters must be provided in request.where as a dict",
                )
            return

        # Batch mode validation
        if request.mode == ProcessingMode.BATCH:
            validate_table_name(request.table)
            validate_columns(request.columns)

            settings = get_settings()

            # Batch READ uses payload as list of PK dicts for multi-key lookup
            if request.operation == OperationType.READ:
                if request.payload and not isinstance(request.payload, list):
                    raise ValidationError(
                        user_message="Invalid payload",
                        internal_message="BATCH READ requires list payload (list of PK dicts)",
                    )
                if isinstance(request.payload, list) and len(request.payload) > settings.max_batch_size:
                    raise ValidationError(
                        user_message="Batch size exceeds limit",
                        internal_message=f"Batch size {len(request.payload)} exceeds max_batch_size={settings.max_batch_size}",
                    )
                return

            # Batch mutations require list[dict] payloads
            if request.payload is None or not isinstance(request.payload, list):
                raise ValidationError(
                    user_message="Invalid payload",
                    internal_message="BATCH mode requires list payload",
                )
            if len(request.payload) == 0:
                raise ValidationError(
                    user_message="Empty batch payload",
                    internal_message="BATCH mode requires at least one record",
                )
            if len(request.payload) > settings.max_batch_size:
                raise ValidationError(
                    user_message="Batch size exceeds limit",
                    internal_message=f"Batch size {len(request.payload)} exceeds max_batch_size={settings.max_batch_size}",
                )
            for idx, record in enumerate(request.payload):
                if not isinstance(record, dict):
                    raise ValidationError(
                        user_message="Invalid batch record",
                        internal_message=f"Batch record {idx} must be a dict",
                    )

            # Batch DELETE requires where
            if request.operation == OperationType.DELETE:
                if not request.where:
                    raise ValidationError(
                        user_message="Batch DELETE requires WHERE conditions",
                        internal_message="DELETE BATCH requires request.where with PK column(s)",
                    )

            # Batch UPDATE/MERGE requires where (dict or list[dict])
            if request.operation in (OperationType.UPDATE, OperationType.MERGE):
                if not request.where:
                    raise ValidationError(
                        user_message="Batch UPDATE/MERGE requires WHERE conditions",
                        internal_message=f"{request.operation.value} BATCH requires request.where",
                    )
                if isinstance(request.where, list):
                    if len(request.where) != len(request.payload):
                        raise ValidationError(
                            user_message="WHERE list length must match payload length",
                            internal_message=(
                                f"where has {len(request.where)} entries but payload has "
                                f"{len(request.payload)} records"
                            ),
                        )
                    for idx, w in enumerate(request.where):
                        if not isinstance(w, dict) or not w:
                            raise ValidationError(
                                user_message="Each WHERE entry must be a non-empty dict",
                                internal_message=f"where[{idx}] is empty or not a dict",
                            )

            return

        # Table-based operations
        validate_table_name(request.table)
        validate_columns(request.columns)

        # Centralize mutation safety invariants
        validate_mutation_safety(request.operation, request.payload, request.where)

        # Payload shape checks
        if request.mode == ProcessingMode.SINGLE:
            if request.payload is not None and not isinstance(request.payload, dict):
                raise ValidationError(
                    user_message="Invalid payload",
                    internal_message="SINGLE mode requires dict payload",
                )
        elif request.mode == ProcessingMode.BATCH:
            if request.payload is not None and not isinstance(request.payload, list):
                raise ValidationError(
                    user_message="Invalid payload",
                    internal_message="BATCH mode requires list payload",
                )

    def _validate_schema_request(self, request: OperationRequest) -> None:
        scenario = request.scenario
        if scenario is None:
            raise ValidationError(
                user_message="Missing schema scenario",
                internal_message="SCHEMA operation requires request.scenario",
            )

        scenario_value = scenario.value if isinstance(scenario, SchemaScenario) else str(scenario)

        if scenario_value == SchemaScenario.LIST_SCHEMAS.value:
            if not request.catalog:
                raise ValidationError(user_message="Missing catalog")

        if scenario_value == SchemaScenario.LIST_TABLES.value:
            if not request.catalog or not request.schema_name:
                raise ValidationError(user_message="Missing catalog/schema")

        if scenario_value in (
            SchemaScenario.TABLE_COLUMNS.value,
            SchemaScenario.TABLE_INFO.value,
            SchemaScenario.INVALIDATE_TABLE_SCHEMA.value,
        ):
            if not request.table:
                raise ValidationError(user_message="Missing table")
            validate_table_name(request.table)

    def _validate_transaction_request(self, request: OperationRequest) -> None:
        settings = get_settings()

        if request.mode != ProcessingMode.SINGLE:
            raise ValidationError(
                user_message="Transaction wrapper mode must be SINGLE",
                internal_message=f"Transaction wrapper mode must be SINGLE (got {request.mode})",
            )

        if request.operations is None:
            raise ValidationError(
                user_message="Missing transaction operations",
                internal_message="TRANSACTION requires request.operations",
            )

        if not isinstance(request.operations, list):
            raise ValidationError(
                user_message="Invalid transaction operations",
                internal_message="TRANSACTION requires operations to be a list",
            )

        if len(request.operations) > settings.max_transaction_statements:
            raise ValidationError(
                user_message="Transaction has too many operations",
                internal_message=(
                    f"operations length {len(request.operations)} exceeds max_transaction_statements="
                    f"{settings.max_transaction_statements}"
                ),
            )

        for idx, sub in enumerate(request.operations):
            if not isinstance(sub, OperationRequest):
                raise ValidationError(
                    user_message="Invalid transaction operation",
                    internal_message=f"Sub-op {idx} must be an OperationRequest",
                )
            if sub.operation == OperationType.TRANSACTION:
                raise ValidationError(
                    user_message="Nested transactions are not supported",
                    internal_message=f"Nested transaction at index {idx}",
                )
            if sub.mode != ProcessingMode.SINGLE:
                raise ValidationError(
                    user_message="Transaction sub-operations must use SINGLE mode",
                    internal_message=f"Sub-op {idx} has mode={sub.mode}",
                )
            if not isinstance(sub.payload, dict):
                raise ValidationError(
                    user_message="Transaction sub-operations require dict payloads",
                    internal_message=f"Sub-op {idx} payload must be a dict",
                )

            # Current TransactionHandler supports only mutations.
            if sub.operation not in (
                OperationType.INSERT,
                OperationType.UPDATE,
                OperationType.MERGE,
                OperationType.DELETE,
            ):
                raise ValidationError(
                    user_message="Unsupported operation in transaction",
                    internal_message=f"Sub-op {idx} has operation={sub.operation}",
                )

            validate_table_name(sub.table)
            validate_mutation_safety(sub.operation, sub.payload, sub.where)

    # ----------------------
    # Stage 2: Authenticate
    # ----------------------

    def _authenticate(self, request: OperationRequest, ctx: ExecutionContext) -> None:
        # Detect and log authentication method and user identity.
        auth_method = detect_auth_method(ctx.headers)
        user_email = get_current_user_email(ctx.headers)
        
        logger.info(
            "Authentication: method=%s user=%s corr=%s",
            auth_method.value,
            user_email,
            ctx.correlation_id,
        )
        
        # Today we rely on UC permissions + OBO when supplied.
        # Keep this stage as a soft check: warn if the request is likely user-data but token missing.
        if request.operation in (
            OperationType.READ,
            OperationType.INSERT,
            OperationType.UPDATE,
            OperationType.MERGE,
            OperationType.DELETE,
            OperationType.TRANSACTION,
        ):
            if ctx.obo_token is None and auth_method == AuthMethod.SERVICE_PRINCIPAL:
                logger.debug(
                    "No OBO token provided (corr=%s op=%s); using Service Principal auth",
                    ctx.correlation_id,
                    getattr(request.operation, "name", str(request.operation)),
                )

    # -----------------
    # Stage 7: Shape
    # -----------------

    def _shape_response(
        self,
        handler_result: QueryResult | OperationResponse,
        request: OperationRequest,
        ctx: ExecutionContext,
        resolved_schema: "ResolvedSchema | None" = None,
    ) -> OperationResponse:
        """
        Shape handler result into final OperationResponse.
        
        Handles two cases:
        1. QueryResult (new): Convert format and build OperationResponse
        2. OperationResponse (legacy/mutations): Ensure consistent envelope
        
        Centralized format conversion happens here based on request.ui_format.
        If resolved_schema is provided, merges column info into response.
        """
        from src.contracts.responses import ColumnMetadata
        
        # Build base metadata
        base_meta: dict[str, Any] = {"correlation_id": ctx.correlation_id}
        
        # Prepare schema fields
        response_columns: tuple[str, ...] = ()
        response_schema: tuple[ColumnMetadata, ...] = ()
        
        if resolved_schema:
            response_columns = resolved_schema.column_names
            response_schema = tuple(
                ColumnMetadata(
                    name=m["name"],
                    data_type=m["type"],
                    nullable=m["nullable"],
                    is_primary_key=m["is_primary_key"],
                    ordinal_position=m["ordinal_position"],
                )
                for m in resolved_schema.column_metadata
            )
            base_meta["schema_resolved"] = True
        
        # Case 1: Handler returned QueryResult (new pattern)
        if isinstance(handler_result, QueryResult):
            # Centralized format conversion
            resolved_ui_format = resolve_ui_format(request.ui_format, handler_result.data_format)
            converted_data = query_result_to_data(handler_result, request.ui_format)
            
            # Merge handler metadata with format info
            base_meta.update(handler_result.metadata)
            base_meta["data_format"] = handler_result.data_format.name
            base_meta["ui_format"] = resolved_ui_format.name
            base_meta["row_count"] = handler_result.row_count
            
            # Use handler columns if schema not resolved
            if not response_columns and handler_result.columns:
                response_columns = handler_result.columns
            
            return OperationResponse(
                success=True,
                data=converted_data,
                affected_rows=handler_result.affected_rows,
                message=handler_result.message,
                errors=[],
                metadata=base_meta,
                columns=response_columns,
                schema=response_schema,
            )
        
        # Case 2: Handler returned OperationResponse (legacy pattern for mutations)
        if not isinstance(handler_result, OperationResponse):
            # Handlers should always return QueryResult or OperationResponse
            return OperationResponse(
                success=False,
                message="System Error: Invalid handler response",
                metadata=base_meta,
            )

        # Merge existing metadata
        if isinstance(handler_result.metadata, dict):
            base_meta.update(handler_result.metadata)

        next_errors = list(handler_result.errors) if isinstance(handler_result.errors, list) else []
        if handler_result.success is False and not next_errors:
            next_errors = [unknown_error_detail(handler_result.message)]

        # Always return a new response instance; do not mutate dict in-place.
        return replace(handler_result, metadata=base_meta, errors=next_errors)

    def _error_response(self, exc: Exception, ctx: ExecutionContext) -> OperationResponse:
        detail = error_detail_from_exception(exc)

        return OperationResponse(
            success=False,
            message=detail.message,
            errors=[detail],
            metadata={"correlation_id": ctx.correlation_id},
        )

    # -----------------
    # Stage 7: Observe
    # -----------------

    def _observe(
        self,
        request: OperationRequest,
        response: OperationResponse,
        ctx: ExecutionContext,
        metrics: LifecycleMetrics,
    ) -> None:
        logger.info(
            "Lifecycle corr=%s op=%s mode=%s success=%s total_ms=%.2f validate_ms=%.2f route_ms=%.2f exec_ms=%.2f",
            ctx.correlation_id,
            getattr(request.operation, "name", str(request.operation)),
            getattr(request.mode, "name", str(request.mode)),
            response.success,
            metrics.total_ms,
            metrics.validation_ms,
            metrics.routing_ms,
            metrics.execution_ms,
        )

        # Periodic cleanup of stale rate-limiter sessions
        now = time.monotonic()
        if now - self._last_cleanup >= self._cleanup_interval:
            self._last_cleanup = now
            removed = self._rate_limiter.cleanup_stale(max_idle_seconds=self._cleanup_interval)
            if removed:
                logger.debug("Rate limiter cleanup: removed %d stale sessions", removed)


def new_correlation_id() -> str:
    """Generate a correlation id for cases where ExecutionContext can't be constructed."""
    return str(uuid.uuid4())
