# src/transformers/format_converters.py
"""
Multi-Format Data Transformation Layer.

Provides format conversion utilities for transforming query results
between Arrow, Pandas, and JSON formats for the UI.

Two orthogonal dimensions:
- DataFormat: How data is represented (JSON_ROWS, ARROW, PANDAS)
- UIFormat: How data is delivered to UI (AUTO, JSON_DICT, PANDAS_DF, ARROW_TABLE)
"""

from typing import Any, TYPE_CHECKING
from src.infrastructure.logging import get_logger
from src.contracts.enums import DataFormat, UIFormat

# Backwards compatibility alias
OutputFormat = DataFormat

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from src.contracts.responses import QueryResult

logger = get_logger(__name__)


def resolve_ui_format(ui_format: UIFormat, data_format: DataFormat) -> UIFormat:
    """
    Resolve AUTO ui_format to actual format based on data_format.
    
    When ui_format is AUTO, returns the native UI format for the given data format.
    """
    if ui_format != UIFormat.AUTO:
        return ui_format

    mapping = {
        DataFormat.JSON_ROWS: UIFormat.JSON_DICT,
        DataFormat.ARROW: UIFormat.ARROW_TABLE,
        DataFormat.PANDAS: UIFormat.PANDAS_DF,
    }
    return mapping.get(data_format, UIFormat.JSON_DICT)


def needs_conversion(data_format: DataFormat, ui_format: UIFormat) -> bool:
    """
    Check if format conversion is needed.
    
    Returns False if data_format and ui_format are naturally compatible.
    """
    if ui_format == UIFormat.AUTO:
        return False
    
    compatible = {
        (DataFormat.JSON_ROWS, UIFormat.JSON_DICT),
        (DataFormat.ARROW, UIFormat.ARROW_TABLE),
        (DataFormat.PANDAS, UIFormat.PANDAS_DF),
    }
    return (data_format, ui_format) not in compatible


def convert_format(
    data: Any,
    source_format: DataFormat,
    target_format: UIFormat,
) -> Any:
    """
    Convert data from source format to target format.
    
    Args:
        data: Input data (list[dict], Arrow Table, or DataFrame)
        source_format: Current DataFormat
        target_format: Desired UIFormat
        
    Returns:
        Converted data in the target format
    """
    if target_format == UIFormat.AUTO:
        target_format = resolve_ui_format(UIFormat.AUTO, source_format)

    # No conversion needed cases
    compatible = {
        (DataFormat.JSON_ROWS, UIFormat.JSON_DICT),
        (DataFormat.ARROW, UIFormat.ARROW_TABLE),
        (DataFormat.PANDAS, UIFormat.PANDAS_DF),
    }
    if (source_format, target_format) in compatible:
        return data

    logger.debug("Converting format: %s -> %s", source_format, target_format)

    # Route conversion
    if source_format == DataFormat.ARROW:
        if target_format == UIFormat.PANDAS_DF:
            return data.to_pandas()
        elif target_format == UIFormat.JSON_DICT:
            return data.to_pylist()
            
    elif source_format == DataFormat.PANDAS:
        if target_format == UIFormat.JSON_DICT:
            return data.to_dict("records")
        elif target_format == UIFormat.ARROW_TABLE:
            import pyarrow as pa
            return pa.Table.from_pandas(data)
            
    elif source_format == DataFormat.JSON_ROWS:
        if target_format == UIFormat.PANDAS_DF:
            import pandas as pd
            return pd.DataFrame.from_records(data) if data else pd.DataFrame()
        elif target_format == UIFormat.ARROW_TABLE:
            import pandas as pd
            import pyarrow as pa
            df = pd.DataFrame.from_records(data) if data else pd.DataFrame()
            return pa.Table.from_pandas(df)
            
    raise ValueError(f"Unsupported conversion: {source_format} -> {target_format}")


def query_result_to_data(
    result: "QueryResult",
    ui_format: UIFormat,
) -> Any:
    """
    Convert QueryResult data to the requested UI format.
    
    This is the main entry point for lifecycle's _shape_response phase.
    
    Args:
        result: QueryResult from handler
        ui_format: Desired UIFormat (may be AUTO)
        
    Returns:
        Data in the requested format
    """
    data_format = result.data_format
    resolved_format = resolve_ui_format(ui_format, data_format)
    
    # Get raw data from QueryResult
    if data_format == DataFormat.JSON_ROWS:
        raw_data = result.rows or ()
    elif data_format == DataFormat.ARROW:
        raw_data = result.arrow_table
    elif data_format == DataFormat.PANDAS:
        raw_data = result.dataframe
    else:
        raw_data = result.rows or ()
    
    # Convert if needed
    if not needs_conversion(data_format, resolved_format):
        # For JSON_ROWS, convert tuple to list for JSON serialization
        if data_format == DataFormat.JSON_ROWS and isinstance(raw_data, tuple):
            return list(raw_data)
        return raw_data
    
    return convert_format(raw_data, data_format, resolved_format)
