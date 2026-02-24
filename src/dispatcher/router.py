"""
OperationRouter - Determines which handler processes the request.

Routing logic based on operation type + processing mode.
"""

from src.contracts.requests import OperationRequest
from src.contracts.enums import OperationType, ProcessingMode
from src.handlers.base import BaseHandler
from src.dispatcher.registry import HandlerRegistry


class OperationRouter:
    """Determines which handler processes the request."""
    
    def __init__(self):
        """Initialize with a handler registry."""
        self._registry = HandlerRegistry()
    
    def resolve(self, request: OperationRequest) -> BaseHandler:
        """
        Routing logic based on operation type + processing mode.
        
        Args:
            request: The operation request.
            
        Returns:
            The appropriate handler for this request.
            
        Raises:
            ValueError: If no handler is registered for this request type.
        """
        # Heartbeat is special - no mode suffix
        if request.operation == OperationType.HEARTBEAT:
            return self._registry.get("heartbeat")
        
        # Transaction is special - wrapper operation with dedicated handler
        if request.operation == OperationType.TRANSACTION:
            return self._registry.get("transaction.multi")

        # Schema/meta operations are routed by scenario
        if request.operation == OperationType.SCHEMA:
            scenario = request.scenario
            if scenario is None:
                raise ValueError("SCHEMA operation requires request.scenario")
            scenario_key = scenario.value if hasattr(scenario, "value") else str(scenario)
            return self._registry.get(f"schema.{scenario_key}")
        
        # Build handler key from operation + mode
        op_key = request.operation.name.lower()  # read, insert, update, merge, delete
        mode_key = request.mode.name.lower()     # single, batch, named
        
        handler_key = f"{op_key}.{mode_key}"
        
        return self._registry.get(handler_key)
