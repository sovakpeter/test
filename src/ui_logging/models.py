"""src/ui_logging/models.py

Models for UI log capture.

No Dash imports here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class UILogMessage:
    timestamp: float
    timestamp_str: str
    level: str
    logger_name: str
    message: str
    correlation_id: str
    user_email: str
    scenario: str
    phase: str
    table: str
    is_phase_header: bool = False
    is_request_boundary: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "timestamp_str": self.timestamp_str,
            "level": self.level,
            "logger": self.logger_name,
            "message": self.message,
            "correlation_id": self.correlation_id,
            "user_email": self.user_email,
            "scenario": self.scenario,
            "phase": self.phase,
            "table": self.table,
            "is_phase_header": self.is_phase_header,
            "is_request_boundary": self.is_request_boundary,
        }


def _is_phase_header_message(message: str) -> bool:
    return "PHASE:" in message and "─" in message


def _is_request_boundary_message(message: str) -> bool:
    return "REQUEST:" in message and "═" in message


def create_message_from_record(
    record,  # logging.LogRecord
    *,
    correlation_id: str,
    user_email: str,
    scenario: str,
    phase: str,
    table: str,
) -> UILogMessage:
    msg = record.getMessage()
    ts = float(getattr(record, "created", 0.0))
    ts_str = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]

    return UILogMessage(
        timestamp=ts,
        timestamp_str=ts_str,
        level=getattr(record, "levelname", "INFO"),
        logger_name=getattr(record, "name", ""),
        message=msg,
        correlation_id=correlation_id,
        user_email=user_email,
        scenario=scenario,
        phase=phase,
        table=table,
        is_phase_header=_is_phase_header_message(msg),
        is_request_boundary=_is_request_boundary_message(msg),
    )
