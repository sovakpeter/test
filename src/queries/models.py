"""Query models for externalized SQL.

This module supports a hybrid approach:
- SQL text in .sql files
- Metadata/params in a JSON manifest
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class QueryParameter:
    name: str
    param_type: str = "string"  # string, integer, float, date, bool, list, table_ref
    required: bool = True
    default: Any = None
    description: str = ""

    def validate(self, value: Any) -> bool:
        if value is None:
            return not self.required

        t = (self.param_type or "").lower()
        if t == "string":
            return isinstance(value, str)
        if t == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        if t == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        if t == "date":
            return isinstance(value, str)
        if t == "bool":
            return isinstance(value, bool)
        if t == "list":
            return isinstance(value, list)
        if t == "table_ref":
            return isinstance(value, str)
        return True


@dataclass(frozen=True, slots=True)
class QueryDefinition:
    key: str
    sql: str
    description: str = ""
    parameters: tuple[QueryParameter, ...] = field(default_factory=tuple)
    cache_ttl: int = 0  # reserved for later; not enforced today
    tags: tuple[str, ...] = field(default_factory=tuple)

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
                errors.append(f"Invalid type for '{param.name}': expected {param.param_type}")

        for key in provided:
            if key not in known:
                errors.append(f"Unknown parameter: {key}")

        return errors

    def table_ref_params(self) -> tuple[str, ...]:
        return tuple(p.name for p in self.parameters if (p.param_type or "").lower() == "table_ref")
