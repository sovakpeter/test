# src/dispatcher/registry.py
"""
HandlerRegistry - Maps handler keys to handler instances.

Handlers are registered by key (e.g., "read.single").
Only active/implemented handlers are registered to avoid import errors.
"""

from src.handlers.base import BaseHandler


class HandlerRegistry:
    """Maps handler keys to handler instances."""
    
    def __init__(self):
        """
        Initialize the registry.
        
        Note: Handlers are imported lazily to avoid circular imports.
        """
        self._handlers: dict[str, BaseHandler] = {}
        self._initialized = False
    
    def _initialize_handlers(self) -> None:
        """Lazy initialization of default handlers."""
        if self._initialized:
            return
            
        # Import handlers here to avoid circular imports
        from src.handlers.heartbeat import HeartbeatHandler
        from src.handlers.read.single import ReadSingleHandler
        from src.handlers.read.batch import ReadBatchHandler
        from src.handlers.write.single import WriteSingleHandler
        from src.handlers.write.batch import WriteBatchHandler
        from src.handlers.delete.single import DeleteSingleHandler
        from src.handlers.delete.batch import DeleteBatchHandler
        from src.handlers.transaction.multi_op import TransactionHandler
        from src.handlers.read.named import ReadNamedHandler
        from src.contracts.enums import SchemaScenario
        from src.handlers.schema.scenarios import SchemaScenarioHandler
        
        self._handlers = {
            "heartbeat": HeartbeatHandler(),
            "read.single": ReadSingleHandler(),
            "read.named": ReadNamedHandler(),
            "read.batch": ReadBatchHandler(),
            "insert.single": WriteSingleHandler(),
            "update.single": WriteSingleHandler(),
            "merge.single": WriteSingleHandler(),
            "insert.batch": WriteBatchHandler(),
            "update.batch": WriteBatchHandler(),
            "merge.batch": WriteBatchHandler(),
            "delete.single": DeleteSingleHandler(),
            "delete.batch": DeleteBatchHandler(),
            "transaction.multi": TransactionHandler(),
            # Schema/meta operations (information_schema)
            f"schema.{SchemaScenario.LIST_CATALOGS.value}": SchemaScenarioHandler(SchemaScenario.LIST_CATALOGS),
            f"schema.{SchemaScenario.LIST_SCHEMAS.value}": SchemaScenarioHandler(SchemaScenario.LIST_SCHEMAS),
            f"schema.{SchemaScenario.LIST_TABLES.value}": SchemaScenarioHandler(SchemaScenario.LIST_TABLES),
            f"schema.{SchemaScenario.TABLE_COLUMNS.value}": SchemaScenarioHandler(SchemaScenario.TABLE_COLUMNS),
            f"schema.{SchemaScenario.TABLE_INFO.value}": SchemaScenarioHandler(SchemaScenario.TABLE_INFO),
            f"schema.{SchemaScenario.INVALIDATE_TABLE_SCHEMA.value}": SchemaScenarioHandler(
                SchemaScenario.INVALIDATE_TABLE_SCHEMA
            ),
        }
        self._initialized = True
    
    def get(self, key: str) -> BaseHandler:
        """
        Get handler by key.
        
        Args:
            key: Handler key (e.g., "read.single").
            
        Returns:
            The registered handler.
            
        Raises:
            ValueError: If no handler is registered for this key.
        """
        self._initialize_handlers()
        
        if key not in self._handlers:
            raise ValueError(f"No handler registered for: {key}")
        return self._handlers[key]
    
    def register(self, key: str, handler: BaseHandler) -> None:
        """
        Register a custom handler.
        
        Args:
            key: Handler key.
            handler: Handler instance.
        """
        self._handlers[key] = handler
