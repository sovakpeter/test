# src/metadata/registry.py
"""
Named query registry loader.

Loads and manages predefined query templates from a JSON registry.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from src.infrastructure.logging import get_logger
from src.contracts.enums import OperationType
from src.contracts.exceptions import ValidationError
from src.security.validators import (
    quote_identifier,
    validate_readonly_prefix,
    validate_sql,
)

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class QueryParameter:
    """Definition of a query parameter."""

    name: str
    param_type: str  # string, integer, float, date, bool, list, table_ref
    required: bool = True
    default: Any = None
    description: str = ""

    def validate(self, value: Any) -> bool:
        if value is None:
            return not self.required

        param_type = (self.param_type or "").lower()
        if param_type == "string":
            return isinstance(value, str)
        if param_type == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if param_type == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if param_type == "date":
            return isinstance(value, str)
        if param_type == "bool":
            return isinstance(value, bool)
        if param_type == "list":
            return isinstance(value, list)
        if param_type == "table_ref":
            return isinstance(value, str)
        return True


@dataclass(frozen=True, slots=True)
class QueryTemplate:
    """Named query template."""
    name: str
    operation: OperationType
    template: str | None = None
    table_ref: str | None = None
    description: str = ""
    parameters: tuple[QueryParameter, ...] = field(default_factory=tuple)

    def apply_defaults(self, provided: Mapping[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = dict(provided)
        for param in self.parameters:
            if param.name not in result and param.default is not None:
                result[param.name] = param.default
        return result

    def validate_params(self, provided: Mapping[str, Any]) -> list[str]:
        if not self.parameters:
            return []

        errors: list[str] = []
        known = {p.name for p in self.parameters}

        for param in self.parameters:
            if param.required and param.name not in provided:
                errors.append(f"Missing required parameter: {param.name}")
            elif param.name in provided and not param.validate(provided[param.name]):
                errors.append(
                    f"Invalid type for '{param.name}': expected {param.param_type}"
                )

        for key in provided:
            if key not in known:
                errors.append(f"Unknown parameter: {key}")

        return errors


class QueryRegistry:
    """
    Registry for named queries.
    """
    
    def __init__(self, registry_path: Path):
        self._registry_path = registry_path
        self._templates: dict[str, QueryTemplate] = {}
        self._load()

    def get(self, name: str) -> QueryTemplate | None:
        """Get query template by name."""
        return self._templates.get(name)

    def _load(self) -> None:
        """Load registry from JSON file."""
        if not self._registry_path.exists():
            logger.warning("Query registry not found: %s", self._registry_path)
            return

        try:
            with open(self._registry_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            queries = data.get("queries", {})
            for name, config in queries.items():
                # Validate query/param identifiers early (defense-in-depth)
                quote_identifier(name)

                template_sql = config.get("template")
                if template_sql:
                    validate_sql(template_sql)
                    validate_readonly_prefix(template_sql)

                parameters: list[QueryParameter] = []
                raw_params = config.get("parameters")
                if raw_params is not None:
                    if not isinstance(raw_params, list):
                        raise ValidationError(
                            user_message="Invalid query registry",
                            internal_message=f"Query '{name}' parameters must be a list",
                        )
                    for p in raw_params:
                        if not isinstance(p, dict):
                            raise ValidationError(
                                user_message="Invalid query registry",
                                internal_message=f"Query '{name}' parameters must be objects",
                            )
                        param_name = str(p.get("name", "")).strip()
                        if not param_name:
                            raise ValidationError(
                                user_message="Invalid query registry",
                                internal_message=f"Query '{name}' has parameter with missing name",
                            )
                        quote_identifier(param_name)
                        parameters.append(
                            QueryParameter(
                                name=param_name,
                                param_type=str(p.get("param_type", "string")),
                                required=bool(p.get("required", True)),
                                default=p.get("default"),
                                description=str(p.get("description", "")),
                            )
                        )

                self._templates[name] = QueryTemplate(
                    name=name,
                    operation=OperationType.READ, # Default to READ for safety
                    template=config.get("template"),
                    table_ref=config.get("table_ref"),
                    description=config.get("description", ""),
                    parameters=tuple(parameters),
                )
            logger.info("Loaded %d query templates", len(self._templates))
            
        except Exception as e:
            logger.error("Failed to load query registry: %s", e)
            raise ValidationError(
                user_message="Invalid query registry",
                internal_message=str(e),
            )
