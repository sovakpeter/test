# src/contracts/immutable.py
"""
Immutability utilities for frozen dataclasses.

Provides helpers for converting mutable types to immutable equivalents
in dataclass __post_init__ hooks.

Example usage:
    @dataclass(frozen=True, slots=True)
    class MyClass:
        data: Mapping[str, Any] | None = None

        def __post_init__(self) -> None:
            if self.data is not None:
                object.__setattr__(self, 'data', freeze_mapping(self.data))
"""

from types import MappingProxyType
from typing import Any, Mapping


def freeze_mapping(
    value: Mapping[str, Any] | dict[str, Any] | None,
) -> MappingProxyType[str, Any] | None:
    """
    Convert a dict or Mapping to an immutable MappingProxyType.

    Args:
        value: A dict, Mapping, or None to freeze.

    Returns:
        An immutable MappingProxyType wrapping the data, or None if input is None.

    Notes:
        - If value is already a MappingProxyType, returns it unchanged.
        - Creates a shallow copy of the mapping before wrapping to ensure
          the original dict cannot be mutated to affect the frozen view.
        - This is a shallow freeze - nested dicts/lists remain mutable.
    """
    if value is None:
        return None
    if isinstance(value, MappingProxyType):
        return value
    return MappingProxyType(dict(value))
