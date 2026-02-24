"""
Databricks SQL Operations Framework.

This is the framework-agnostic core layer. Nothing in this package imports from Dash.
The single entry point is `execute()` which accepts an OperationRequest and returns
an OperationResponse.

Usage:
    from src.api import execute, OperationRequest, OperationType
    
    response = execute(OperationRequest(
        operation=OperationType.READ,
        table="catalog.schema.table",
        ...
    ))
"""
