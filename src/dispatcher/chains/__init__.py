# src/dispatcher/chains/__init__.py
"""
Predefined operation chains.

Available chains:
- READ_WITH_SCHEMA: Fetch schema + data in one operation

Future chains (planned):
- WRITE_WITH_VALIDATION: Validate schema before write
- BATCH_WITH_CHUNKING: Split large batch into chunks
"""

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.dispatcher.chain import OperationChain


class PredefinedChain(str, Enum):
    """Available predefined chains."""
    READ_WITH_SCHEMA = "READ_WITH_SCHEMA"


def get_chain(chain_type: PredefinedChain) -> "OperationChain":
    """Get a predefined chain by type.
    
    Lazy imports to avoid circular dependencies.
    
    Args:
        chain_type: The type of predefined chain to create
        
    Returns:
        OperationChain instance configured for the specified chain type
        
    Raises:
        ValueError: If chain_type is unknown
    """
    from src.dispatcher.chains.read_with_schema import create_read_with_schema_chain
    
    _CHAIN_REGISTRY = {
        PredefinedChain.READ_WITH_SCHEMA: create_read_with_schema_chain,
    }
    
    factory = _CHAIN_REGISTRY.get(chain_type)
    if not factory:
        raise ValueError(f"Unknown chain type: {chain_type}")
    return factory()


__all__ = ["PredefinedChain", "get_chain"]
