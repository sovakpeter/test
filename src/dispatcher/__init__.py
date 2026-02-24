"""
Dispatcher package - Routing & orchestration.

This package contains:
- OperationManager: The brain that orchestrates operations
- OperationRouter: Determines which handler to use
- HandlerRegistry: Maps handler keys to handler instances
- ExecutionContext: Request-scoped execution context
"""

# from src.dispatcher.manager import OperationManager
# from src.dispatcher.router import OperationRouter
# from src.dispatcher.registry import HandlerRegistry
from src.dispatcher.context import ExecutionContext

__all__ = [
    # "OperationManager",
    # "OperationRouter",
    # "HandlerRegistry",
    "ExecutionContext",
]
