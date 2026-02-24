# src/dispatcher/chain.py
"""
Chained Operations Framework.

Enables composition of handlers into pipelines where output
from one step can inform subsequent steps.

Example usage:
    chain = OperationChain()
    chain.add_step(ChainStep(
        name="fetch_schema",
        step_type=ChainStepType.SCHEMA_FETCH,
        handler=schema_handler,
    ))
    chain.add_step(ChainStep(
        name="fetch_data",
        step_type=ChainStepType.DATA_FETCH,
        handler=read_handler,
    ))
    result = chain.execute(request, ctx)
"""

from dataclasses import dataclass, field
from typing import Any, Callable, TYPE_CHECKING
from enum import Enum

from src.contracts.requests import OperationRequest
from src.contracts.responses import QueryResult, OperationResponse, ColumnMetadata
from src.infrastructure.logging import get_logger

if TYPE_CHECKING:
    from src.dispatcher.context import ExecutionContext
    from src.handlers.base import BaseHandler

logger = get_logger(__name__)


class ChainStepType(str, Enum):
    """Type of chain step.
    
    Determines how the step's result is processed:
    - SCHEMA_FETCH: Extract column info from result
    - DATA_FETCH: Capture as final data result
    - TRANSFORM: Modify data in-place
    - VALIDATE: Check invariants
    """
    SCHEMA_FETCH = "SCHEMA_FETCH"
    DATA_FETCH = "DATA_FETCH"
    TRANSFORM = "TRANSFORM"
    VALIDATE = "VALIDATE"


@dataclass
class ChainContext:
    """
    Context passed through chain steps.
    
    Accumulates results and metadata from each step.
    Mutable during chain execution.
    """
    request: OperationRequest
    execution_ctx: "ExecutionContext"
    
    # Accumulated results from schema fetch steps
    resolved_columns: tuple[str, ...] = field(default_factory=tuple)
    resolved_schema: tuple[dict, ...] = field(default_factory=tuple)
    
    # Intermediate storage for complex chains
    intermediate_results: dict[str, Any] = field(default_factory=dict)
    
    # Final result from data fetch step
    final_result: QueryResult | OperationResponse | None = None


@dataclass
class ChainStep:
    """A single step in an operation chain.
    
    Attributes:
        name: Unique identifier for this step
        step_type: How to process the step's result
        handler: Handler or callable to execute
        request_modifier: Optional function to modify request before execution
    """
    name: str
    step_type: ChainStepType
    handler: "BaseHandler | Callable[[ChainContext], ChainContext]"
    request_modifier: Callable[[ChainContext], OperationRequest] | None = None


class OperationChain:
    """
    Composable chain of operations.
    
    Executes a sequence of steps, passing context between them.
    Schema information from SCHEMA_FETCH steps is preserved
    and merged into the final response.
    
    Example:
        chain = OperationChain()
        chain.add_step(schema_step)
        chain.add_step(data_step)
        response = chain.execute(request, ctx)
    """
    
    def __init__(self):
        self._steps: list[ChainStep] = []
    
    def add_step(self, step: ChainStep) -> "OperationChain":
        """Add a step to the chain. Returns self for fluent API."""
        self._steps.append(step)
        return self
    
    def execute(
        self, 
        request: OperationRequest, 
        ctx: "ExecutionContext"
    ) -> OperationResponse:
        """Execute all steps in sequence.
        
        Each step's result is processed based on its step_type:
        - SCHEMA_FETCH: Extract columns/schema, store in context
        - DATA_FETCH: Store as final result
        - TRANSFORM/VALIDATE: Modify context as needed
        
        Returns final OperationResponse with merged schema info.
        """
        chain_ctx = ChainContext(request=request, execution_ctx=ctx)
        
        logger.debug("Chain execution starting with %d steps", len(self._steps))
        
        for step in self._steps:
            logger.debug("Chain executing step: %s (%s)", step.name, step.step_type.value)
            chain_ctx = self._execute_step(step, chain_ctx)
        
        logger.debug("Chain execution complete")
        return self._build_response(chain_ctx)
    
    def _execute_step(self, step: ChainStep, chain_ctx: ChainContext) -> ChainContext:
        """Execute a single chain step."""
        from src.handlers.base import BaseHandler
        
        # Apply request modifier if present
        if step.request_modifier:
            modified_request = step.request_modifier(chain_ctx)
        else:
            modified_request = chain_ctx.request
        
        # Execute handler or callable
        if isinstance(step.handler, BaseHandler):
            result = step.handler.handle(chain_ctx.execution_ctx, modified_request)
        elif callable(step.handler):
            # Callable receives and returns ChainContext
            return step.handler(chain_ctx)
        else:
            raise TypeError(f"Invalid handler type: {type(step.handler)}")
        
        # Process result based on step type
        if step.step_type == ChainStepType.SCHEMA_FETCH:
            chain_ctx = self._process_schema_result(chain_ctx, result, step.name)
        elif step.step_type == ChainStepType.DATA_FETCH:
            chain_ctx.final_result = result
            chain_ctx.intermediate_results[step.name] = result
        elif step.step_type in (ChainStepType.TRANSFORM, ChainStepType.VALIDATE):
            chain_ctx.intermediate_results[step.name] = result
        
        return chain_ctx
    
    def _process_schema_result(
        self, 
        chain_ctx: ChainContext, 
        result: QueryResult | OperationResponse,
        step_name: str,
    ) -> ChainContext:
        """Extract schema info from schema fetch result."""
        chain_ctx.intermediate_results[step_name] = result
        
        if isinstance(result, QueryResult) and result.rows:
            # Result contains column rows from information_schema
            columns = tuple(row.get("column_name", "") for row in result.rows if row.get("column_name"))
            schema = tuple(
                {
                    "name": row.get("column_name"),
                    "type": row.get("data_type", "STRING"),
                    "nullable": row.get("is_nullable", True),
                    "is_primary_key": row.get("is_primary_key", False),
                    "ordinal_position": row.get("ordinal_position", 0),
                }
                for row in result.rows
                if row.get("column_name")
            )
            chain_ctx.resolved_columns = columns
            chain_ctx.resolved_schema = schema
            
        elif isinstance(result, OperationResponse) and result.data:
            # OperationResponse with structured data
            if isinstance(result.data, dict) and "columns" in result.data:
                chain_ctx.resolved_columns = tuple(
                    c.get("column_name") for c in result.data["columns"]
                    if c.get("column_name")
                )
                chain_ctx.resolved_schema = tuple(result.data["columns"])
        
        return chain_ctx
    
    def _build_response(self, chain_ctx: ChainContext) -> OperationResponse:
        """Build final response merging all chain results."""
        final = chain_ctx.final_result
        
        # Convert resolved_schema dicts to ColumnMetadata tuples
        schema_tuple: tuple[ColumnMetadata, ...] = tuple(
            ColumnMetadata(
                name=m.get("name", ""),
                data_type=m.get("type", "STRING"),
                nullable=m.get("nullable", True),
                is_primary_key=m.get("is_primary_key", False),
                ordinal_position=m.get("ordinal_position", 0),
            )
            for m in chain_ctx.resolved_schema
        ) if chain_ctx.resolved_schema else ()
        
        if isinstance(final, OperationResponse):
            # Merge schema info into existing response
            merged_metadata = dict(final.metadata) if final.metadata else {}
            merged_metadata["chained"] = True
            merged_metadata["chain_steps"] = [s.name for s in self._steps]
            
            return OperationResponse(
                success=final.success,
                data=final.data,
                affected_rows=final.affected_rows,
                message=final.message,
                errors=list(final.errors) if final.errors else [],
                columns=chain_ctx.resolved_columns or final.columns,
                schema=schema_tuple if schema_tuple else final.schema,
                metadata=merged_metadata,
            )
            
        elif isinstance(final, QueryResult):
            # Convert QueryResult to OperationResponse with schema
            return OperationResponse(
                success=True,
                data=final.data,
                affected_rows=final.affected_rows,
                message=final.message,
                columns=chain_ctx.resolved_columns or final.columns,
                schema=schema_tuple,
                metadata={
                    "row_count": final.row_count,
                    "data_format": final.data_format.value if hasattr(final.data_format, 'value') else str(final.data_format),
                    "chained": True,
                    "chain_steps": [s.name for s in self._steps],
                },
            )
        
        # No final result - return error
        return OperationResponse(
            success=False,
            message="Chain did not produce a final result",
            columns=chain_ctx.resolved_columns,
            schema=schema_tuple,
            metadata={
                "chained": True,
                "chain_steps": [s.name for s in self._steps],
            },
        )
