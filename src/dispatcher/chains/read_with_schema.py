# src/dispatcher/chains/read_with_schema.py
"""
Predefined chain: Read with automatic schema resolution.

Chain: SchemaScenarioHandler → ReadSingleHandler

This chain first fetches schema information for a table,
then fetches the actual data. The schema is merged into
the final response, ensuring column info is always available
even for empty result sets.
"""

from dataclasses import replace
from typing import TYPE_CHECKING

from src.contracts.enums import OperationType, SchemaScenario
from src.contracts.requests import OperationRequest
from src.dispatcher.chain import (
    OperationChain, 
    ChainStep, 
    ChainStepType, 
    ChainContext,
)

if TYPE_CHECKING:
    pass


def create_read_with_schema_chain() -> OperationChain:
    """
    Create a chain that fetches schema first, then data.
    
    Use when columns="*" and you need schema metadata in response.
    
    Steps:
    1. fetch_schema: Query information_schema for column metadata
    2. fetch_data: Execute the actual SELECT query
    
    Returns:
        Configured OperationChain instance
    """
    from src.handlers.schema.scenarios import SchemaScenarioHandler
    from src.handlers.read.single import ReadSingleHandler
    
    chain = OperationChain()
    
    # Step 1: Fetch schema using TABLE_COLUMNS scenario
    chain.add_step(ChainStep(
        name="fetch_schema",
        step_type=ChainStepType.SCHEMA_FETCH,
        handler=SchemaScenarioHandler(SchemaScenario.TABLE_COLUMNS),
        request_modifier=_schema_request,
    ))
    
    # Step 2: Fetch data using ReadSingleHandler
    chain.add_step(ChainStep(
        name="fetch_data",
        step_type=ChainStepType.DATA_FETCH,
        handler=ReadSingleHandler(),
    ))
    
    return chain


def _schema_request(chain_ctx: ChainContext) -> OperationRequest:
    """Modify request for schema fetch step.
    
    Transforms the original READ request into a SCHEMA request
    targeting TABLE_COLUMNS scenario.
    """
    original = chain_ctx.request
    
    return replace(
        original,
        operation=OperationType.SCHEMA,
        scenario=SchemaScenario.TABLE_COLUMNS,
        # Keep the table reference from original
        # Clear columns since schema fetch doesn't need them
        columns=None,
    )


__all__ = ["create_read_with_schema_chain"]
