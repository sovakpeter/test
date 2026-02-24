# src/infrastructure/connection.py
"""
Database configuration and connection management.

Provides:
- ConnectionPool: Dictionary-based connection management (Dependency Injection)
- Transaction Context Manager
- Query Execution Helpers
- SQL Execution Logging (with skip_logging flag for named queries)
"""

from contextlib import contextmanager
from typing import Any, Generator
import threading

# Databricks connector (ensure installed: databricks-sql-connector)
try:
    from databricks import sql
except ImportError:
    sql = None  # Handle missing driver gracefully during dev

from src.infrastructure.config import Settings
from src.infrastructure.config import get_settings
from src.infrastructure.logging import get_logger
from src.contracts.exceptions import TimeoutError
from src.logging import get_logger as get_structured_logger

logger = get_logger(__name__)
slog = get_structured_logger(__name__)


# Thread-local storage for request-scoped connection reuse.
# IMPORTANT: reuse MUST be identity-aware to avoid leaking identity context.
_local = threading.local()


class ConnectionPool:
    """
    Database connection factory.
    
    Manages connections to Databricks SQL Warehouse using either:
    1. Service Principal (SP) credentials from Settings (Shared)
    2. On-Behalf-Of (OBO) token passed explicitly (User-Specific)
    """

    def __init__(self, settings: Settings):
        self._settings = settings

    @contextmanager
    def get_connection(self, token: str | None = None) -> Generator[Any, None, None]:
        """
        Get a request-scoped database connection.
        
        Args:
            token: Optional OBO access token.
                   If provided, connects as that user (Row-Level Security).
                   If None, connects as Service Principal (Shared Schema Access).
                   
        Yields:
            Databricks SQL Connection
        """
        # Identity-aware thread-local reuse:
        # - SP connections (token=None) may be reused *within a request*.
        # - OBO connections may be reused *within a request* only when token matches.
        # - OBO and SP MUST NEVER be mixed.

        if token is None:
            existing_sp = getattr(_local, "sp_conn", None)
            if existing_sp is not None:
                yield existing_sp
                return

            conn = None
            try:
                conn = self._create_connection(token=None)
                _local.sp_conn = conn
                yield conn
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception as e:
                        logger.warning(f"Error closing connection: {e}")
                    finally:
                        _local.sp_conn = None
            return

        # token is not None (OBO)
        existing_obo = getattr(_local, "obo_conn", None)
        existing_obo_token = getattr(_local, "obo_token", None)

        if existing_obo is not None and existing_obo_token == token:
            yield existing_obo
            return

        # If an OBO connection exists for a different token, do NOT reuse or overwrite it.
        # Create a one-off connection scoped to this context.
        if existing_obo is not None and existing_obo_token != token:
            conn = None
            try:
                conn = self._create_connection(token=token)
                yield conn
            finally:
                if conn is not None:
                    try:
                        conn.close()
                    except Exception as e:
                        logger.warning(f"Error closing connection: {e}")
            return

        # No existing OBO connection in this thread: create and cache for nested reuse.
        conn = None
        try:
            conn = self._create_connection(token=token)
            _local.obo_conn = conn
            _local.obo_token = token
            yield conn
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
                finally:
                    _local.obo_conn = None
                    _local.obo_token = None

    def _create_connection(self, token: str | None) -> Any:
        """Create a raw Databricks SQL connection."""
        if not sql:
            raise RuntimeError("databricks-sql-connector not installed")

        # Determine credentials
        if token:
            # OBO Mode
            auth_token = token
            logger.debug("Creating connection with OBO token")
        else:
            # SP Mode
            # TODO: Add explicit SP credentials if not using token
            # For now, leveraging settings.databricks_token as fallback or PAT
            auth_token = self._settings.databricks_token
            logger.debug("Creating connection with Service Credential")

        # --- DIAGNOSTIC LOGGING FOR PROD DEBUGGING ---
        # Log connection parameters (mask sensitive token)
        token_preview = f"{auth_token[:8]}...{auth_token[-4:]}" if auth_token and len(auth_token) > 12 else ("SET" if auth_token else "EMPTY/NONE")
        logger.info(
            "DB Connection attempt: server_hostname=%r, http_path=%r, token=%s",
            self._settings.server_hostname or "(EMPTY)",
            self._settings.http_path or "(EMPTY)",
            token_preview,
        )
        
        # Validate required parameters before attempting connection
        if not self._settings.server_hostname:
            logger.error("DATABRICKS_SERVER_HOSTNAME/DATABRICKS_HOST is not set!")
            raise RuntimeError("Missing server_hostname: Set DATABRICKS_SERVER_HOSTNAME or DATABRICKS_HOST env var")
        if not self._settings.http_path:
            logger.error("DATABRICKS_HTTP_PATH/WAREHOUSE_ID is not set!")
            raise RuntimeError("Missing http_path: Set DATABRICKS_HTTP_PATH or WAREHOUSE_ID env var")
        if not auth_token:
            logger.error("No authentication token available (neither OBO token nor DATABRICKS_TOKEN)!")
            raise RuntimeError("Missing auth token: OBO header not present and DATABRICKS_TOKEN env var not set")

        # Effective per-request timeout guardrail.
        # Databricks connector timeouts are connection-level, but they provide a practical ceiling
        # for long-running statements and large fetches.
        query_timeout = getattr(self._settings, "query_timeout_seconds", 0)
        effective_socket_timeout = self._settings.db_socket_timeout
        effective_session_timeout = self._settings.db_session_timeout
        if isinstance(query_timeout, int) and query_timeout > 0:
            effective_socket_timeout = min(effective_socket_timeout, query_timeout)
            effective_session_timeout = min(effective_session_timeout, query_timeout)

        return sql.connect(
            server_hostname=self._settings.server_hostname,
            http_path=self._settings.http_path,
            access_token=auth_token,
            # Configured timeouts
            _socket_timeout=effective_socket_timeout,
            _http_session_timeout=effective_session_timeout,
        )


@contextmanager
def transaction_context(conn: Any) -> Generator[Any, None, None]:
    """
    Context manager for atomic transactions.
    
    Disables autocommit, commits on success, rolls back on error.
    """
    if not hasattr(conn, "autocommit"):
         # Mock/Test support
         yield conn
         return

    original_autocommit = getattr(conn, "autocommit", True)
    conn.autocommit = False
    
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = original_autocommit


def execute_query(
    conn: Any,
    sql_stmt: str,
    params: dict[str, Any] | None = None,
    *,
    skip_logging: bool = False,
) -> list[Any]:
    """
    Execute SELECT query and return Row objects (JSON_ROWS format).
    
    This is the default fetch method returning list of Row objects that
    can be converted to dicts via row.asDict().
    
    Args:
        conn: Database connection
        sql_stmt: SQL statement to execute
        params: Query parameters
        skip_logging: If True, skip SQL logging (used by named queries that log separately)
    """
    if not skip_logging:
        slog.log_sql_execution(sql_stmt, params, query_type="SELECT")

    timeout_seconds = get_settings().query_timeout_seconds
    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        with conn.cursor() as cursor:
            cursor.execute(sql_stmt, params)
            return cursor.fetchall()

    with conn.cursor() as cursor:
        result: list[Any] | None = None
        error: Exception | None = None

        def _run() -> None:
            nonlocal result, error
            try:
                cursor.execute(sql_stmt, params)
                result = cursor.fetchall()
            except Exception as exc:  # connector may raise connector-specific errors
                error = exc

        thread = threading.Thread(target=_run, name="dbx-exec-query", daemon=True)
        thread.start()
        thread.join(timeout_seconds)

        if thread.is_alive():
            # Best-effort cancellation. The connector supports cursor.cancel().
            try:
                cursor.cancel()
            except Exception:
                pass

            # Give cancellation a moment to propagate, then fail fast.
            thread.join(min(5.0, max(0.0, float(timeout_seconds))))
            raise TimeoutError(
                user_message="Query timed out",
                internal_message=f"Query exceeded timeout_seconds={timeout_seconds}",
            )

        if error is not None:
            # If the connector surfaced a cancellation error, map it to a TimeoutError.
            if hasattr(sql, "exc") and isinstance(error, getattr(sql.exc, "RequestError", (Exception,))):
                raise TimeoutError(
                    user_message="Query was cancelled",
                    internal_message=f"Query cancelled (timeout_seconds={timeout_seconds})",
                )
            raise error

        return result or []


def execute_query_arrow(
    conn: Any,
    sql_stmt: str,
    params: dict[str, Any] | None = None,
    *,
    skip_logging: bool = False,
) -> Any:
    """
    Execute SELECT query and return PyArrow Table (ARROW format).
    
    Uses cursor.fetchall_arrow() for high-performance columnar data transfer.
    This is the fastest method for large datasets.
    
    Args:
        conn: Database connection
        sql_stmt: SQL statement to execute
        params: Query parameters
        skip_logging: If True, skip SQL logging (used by named queries that log separately)
    
    Returns:
        PyArrow Table
    """
    if not skip_logging:
        slog.log_sql_execution(sql_stmt, params, query_type="SELECT_ARROW")
    
    timeout_seconds = get_settings().query_timeout_seconds
    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        with conn.cursor() as cursor:
            cursor.execute(sql_stmt, params)
            return cursor.fetchall_arrow()

    with conn.cursor() as cursor:
        result: Any = None
        error: Exception | None = None

        def _run() -> None:
            nonlocal result, error
            try:
                cursor.execute(sql_stmt, params)
                result = cursor.fetchall_arrow()
            except Exception as exc:
                error = exc

        thread = threading.Thread(target=_run, name="dbx-exec-arrow", daemon=True)
        thread.start()
        thread.join(timeout_seconds)

        if thread.is_alive():
            try:
                cursor.cancel()
            except Exception:
                pass
            thread.join(min(5.0, max(0.0, float(timeout_seconds))))
            raise TimeoutError(
                user_message="Query timed out",
                internal_message=f"Arrow query exceeded timeout_seconds={timeout_seconds}",
            )

        if error is not None:
            if hasattr(sql, "exc") and isinstance(error, getattr(sql.exc, "RequestError", (Exception,))):
                raise TimeoutError(
                    user_message="Query was cancelled",
                    internal_message=f"Arrow query cancelled (timeout_seconds={timeout_seconds})",
                )
            raise error

        return result


def execute_query_pandas(
    conn: Any,
    sql_stmt: str,
    params: dict[str, Any] | None = None,
    *,
    skip_logging: bool = False,
) -> Any:
    """
    Execute SELECT query and return Pandas DataFrame (PANDAS format).
    
    Fetches as Arrow first then converts to Pandas for efficiency.
    
    Args:
        conn: Database connection
        sql_stmt: SQL statement to execute
        params: Query parameters
        skip_logging: If True, skip SQL logging (used by named queries that log separately)
    
    Returns:
        Pandas DataFrame
    """
    # Arrow fetch handles logging if not skipped
    arrow_table = execute_query_arrow(conn, sql_stmt, params, skip_logging=skip_logging)
    return arrow_table.to_pandas()


def execute_non_query(
    conn: Any,
    sql_stmt: str,
    params: dict[str, Any] | None = None,
    *,
    skip_logging: bool = False,
) -> int:
    """
    Execute INSERT/UPDATE/DELETE and return rows affected.
    
    Args:
        conn: Database connection
        sql_stmt: SQL statement to execute
        params: Query parameters
        skip_logging: If True, skip SQL logging
    """
    if not skip_logging:
        slog.log_sql_execution(sql_stmt, params, query_type="MUTATION")

    timeout_seconds = get_settings().query_timeout_seconds
    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        with conn.cursor() as cursor:
            cursor.execute(sql_stmt, params)
            if hasattr(cursor, "rowcount"):
                return cursor.rowcount
            return -1

    with conn.cursor() as cursor:
        affected_rows: int | None = None
        error: Exception | None = None

        def _run() -> None:
            nonlocal affected_rows, error
            try:
                cursor.execute(sql_stmt, params)
                if hasattr(cursor, "rowcount"):
                    affected_rows = cursor.rowcount
                else:
                    affected_rows = -1
            except Exception as exc:
                error = exc

        thread = threading.Thread(target=_run, name="dbx-exec-non-query", daemon=True)
        thread.start()
        thread.join(timeout_seconds)

        if thread.is_alive():
            try:
                cursor.cancel()
            except Exception:
                pass
            thread.join(min(5.0, max(0.0, float(timeout_seconds))))
            raise TimeoutError(
                user_message="Query timed out",
                internal_message=f"Non-query exceeded timeout_seconds={timeout_seconds}",
            )

        if error is not None:
            if hasattr(sql, "exc") and isinstance(error, getattr(sql.exc, "RequestError", (Exception,))):
                raise TimeoutError(
                    user_message="Query was cancelled",
                    internal_message=f"Non-query cancelled (timeout_seconds={timeout_seconds})",
                )
            raise error

        return int(affected_rows if affected_rows is not None else -1)


def execute_non_query_many(
    conn: Any,
    sql_stmt: str,
    param_list: list[dict[str, Any]],
    *,
    skip_logging: bool = False,
) -> int:
    """
    Execute INSERT with executemany for efficient bulk inserts.

    Uses cursor.executemany() which batches multiple parameter sets into a
    single round-trip where the connector supports it.

    Args:
        conn: Database connection
        sql_stmt: SQL statement (INSERT INTO ... VALUES ...)
        param_list: List of parameter dicts, one per row
        skip_logging: If True, skip SQL logging

    Returns:
        Number of rows affected (rowcount from cursor)
    """
    if not skip_logging:
        slog.log_sql_execution(
            sql_stmt,
            {"batch_size": len(param_list), "sample": param_list[0] if param_list else {}},
            query_type="BATCH_MUTATION",
        )

    timeout_seconds = get_settings().query_timeout_seconds
    if not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        with conn.cursor() as cursor:
            cursor.executemany(sql_stmt, param_list)
            if hasattr(cursor, "rowcount"):
                return cursor.rowcount
            return -1

    with conn.cursor() as cursor:
        affected_rows: int | None = None
        error: Exception | None = None

        def _run() -> None:
            nonlocal affected_rows, error
            try:
                cursor.executemany(sql_stmt, param_list)
                if hasattr(cursor, "rowcount"):
                    affected_rows = cursor.rowcount
                else:
                    affected_rows = -1
            except Exception as exc:
                error = exc

        thread = threading.Thread(target=_run, name="dbx-exec-many", daemon=True)
        thread.start()
        thread.join(timeout_seconds)

        if thread.is_alive():
            try:
                cursor.cancel()
            except Exception:
                pass
            thread.join(min(5.0, max(0.0, float(timeout_seconds))))
            raise TimeoutError(
                user_message="Batch query timed out",
                internal_message=f"executemany exceeded timeout_seconds={timeout_seconds}",
            )

        if error is not None:
            if hasattr(sql, "exc") and isinstance(error, getattr(sql.exc, "RequestError", (Exception,))):
                raise TimeoutError(
                    user_message="Batch query was cancelled",
                    internal_message=f"executemany cancelled (timeout_seconds={timeout_seconds})",
                )
            raise error

        return int(affected_rows if affected_rows is not None else -1)
