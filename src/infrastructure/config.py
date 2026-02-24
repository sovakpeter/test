"""
Application settings - Environment-driven configuration.

Uses environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass, field

from src.infrastructure.env import load_dotenv_once


@dataclass(frozen=True, slots=True)
class Settings:
    """
    Application settings loaded from environment variables.
    
    All settings have sensible defaults for local development.
    Production values should be set via environment variables.
    """
    
    # Databricks connection settings
    server_hostname: str = field(
        default_factory=lambda: (
            (os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST") or "")
            .replace("https://", "")
            .replace("http://", "")
        )
    )
    http_path: str = field(
        default_factory=lambda: (
            os.getenv("DATABRICKS_HTTP_PATH")
            or (
                f"/sql/1.0/warehouses/{os.getenv('WAREHOUSE_ID')}"
                if os.getenv("WAREHOUSE_ID")
                else ""
            )
        )
    )
    databricks_token: str | None = field(
        default_factory=lambda: os.getenv("DATABRICKS_TOKEN")
    )
    
    # Unity Catalog settings
    catalog: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_CATALOG", "main")
    )
    schema: str = field(
        default_factory=lambda: os.getenv("DATABRICKS_SCHEMA", "default")
    )

    
    # Application settings
    debug: bool = field(
        default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true"
    )
    log_level: str = field(
        default_factory=lambda: os.getenv("LOG_LEVEL", "INFO")
    )

    # Logging format/output
    # - LOG_FORMAT_STYLE: "terminal" (human) or "json" (structured-ish)
    # - LOG_FILE: optional path to write logs to (in addition to stdout)
    log_format_style: str = field(
        default_factory=lambda: os.getenv("LOG_FORMAT_STYLE", "terminal")
    )
    log_file: str | None = field(
        default_factory=lambda: (os.getenv("LOG_FILE") or None)
    )

    # UI log viewer (Dash) - opt-in per browser session
    ui_log_enabled_default: bool = field(
        default_factory=lambda: os.getenv("UI_LOG_ENABLED_DEFAULT", "false").lower() == "true"
    )
    ui_log_level: str = field(
        default_factory=lambda: os.getenv("UI_LOG_LEVEL", "INFO").upper()
    )
    ui_log_buffer_max_messages: int = field(
        default_factory=lambda: int(os.getenv("UI_LOG_BUFFER_MAX_MESSAGES", "500"))
    )
    ui_log_poll_interval_ms: int = field(
        default_factory=lambda: int(os.getenv("UI_LOG_POLL_INTERVAL_MS", "500"))
    )
    
    # Intent & SQL Logging
    log_intent_enabled: bool = field(
        default_factory=lambda: os.getenv("LOG_INTENT_ENABLED", "true").lower() == "true"
    )
    log_sql_enabled: bool = field(
        default_factory=lambda: os.getenv("LOG_SQL_ENABLED", "true").lower() == "true"
    )
    sql_log_max_length: int = field(
        default_factory=lambda: int(os.getenv("SQL_LOG_MAX_LENGTH", "1000"))
    )
    
    # Query settings
    # Primary names (match architecture docs)
    default_read_limit: int = field(
        default_factory=lambda: int(os.getenv("DEFAULT_READ_LIMIT", os.getenv("DEFAULT_QUERY_LIMIT", "1000")))
    )
    max_read_limit: int = field(
        default_factory=lambda: int(os.getenv("MAX_READ_LIMIT", os.getenv("MAX_QUERY_LIMIT", "10000")))
    )

    # Bounded operations
    max_batch_size: int = field(
        default_factory=lambda: int(os.getenv("MAX_BATCH_SIZE", "1000"))
    )
    max_transaction_statements: int = field(
        default_factory=lambda: int(os.getenv("MAX_TRANSACTION_STATEMENTS", "50"))
    )
    query_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("QUERY_TIMEOUT_SECONDS", "900"))
    )

    # Warehouse warmup (cold-start mitigation)
    warehouse_warmup_enabled: bool = field(
        default_factory=lambda: os.getenv("WAREHOUSE_WARMUP_ENABLED", "true").lower() == "true"
    )
    warehouse_warmup_ttl_seconds: int = field(
        default_factory=lambda: int(os.getenv("WAREHOUSE_WARMUP_TTL_SECONDS", "600"))
    )
    warehouse_warmup_failure_backoff_seconds: int = field(
        default_factory=lambda: int(os.getenv("WAREHOUSE_WARMUP_FAILURE_BACKOFF_SECONDS", "30"))
    )
    warehouse_warmup_sql: str = field(
        default_factory=lambda: os.getenv("WAREHOUSE_WARMUP_SQL", "SELECT 1")
    )
    
    # Connection pool settings
    connection_pool_size: int = field(
        default_factory=lambda: int(os.getenv("CONNECTION_POOL_SIZE", "5"))
    )
    
    # Database timeout settings (seconds)
    db_socket_timeout: int = field(
        default_factory=lambda: int(os.getenv("DB_SOCKET_TIMEOUT", "600"))
    )
    db_session_timeout: int = field(
        default_factory=lambda: int(os.getenv("DB_SESSION_TIMEOUT", "3600"))
    )
    
    # Cloud Fetch optimization (disable if network blocks direct Azure Storage access)
    use_cloud_fetch: bool = field(
        default_factory=lambda: os.getenv("USE_CLOUD_FETCH", "false").lower() == "true"
    )

    # Connection storm protection
    rate_limit_requests: int = field(
        default_factory=lambda: int(os.getenv("RATE_LIMIT_REQUESTS", "8"))
    )
    rate_limit_window_seconds: int = field(
        default_factory=lambda: int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "10"))
    )
    max_concurrent_queries: int = field(
        default_factory=lambda: int(os.getenv("MAX_CONCURRENT_QUERIES", "20"))
    )
    rate_limit_cleanup_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("RATE_LIMIT_CLEANUP_INTERVAL_SECONDS", "300"))
    )

    # Metadata/schema cache
    schema_cache_ttl_seconds: int = field(
        default_factory=lambda: int(os.getenv("SCHEMA_CACHE_TTL_SECONDS", "3600"))
    )

    # Backward-compatible aliases (older code/docs)
    @property
    def default_limit(self) -> int:  # pragma: no cover
        return self.default_read_limit

    @property
    def max_limit(self) -> int:  # pragma: no cover
        return self.max_read_limit


# Singleton settings instance
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the settings singleton."""
    global _settings
    if _settings is None:
        load_dotenv_once()
        _settings = Settings()
        # --- DIAGNOSTIC LOGGING FOR PROD DEBUGGING ---
        # Log key connection settings at startup (helps diagnose env var issues)
        import logging
        _log = logging.getLogger(__name__)
        _log.info(
            "Settings initialized: server_hostname=%r, http_path=%r, token_set=%s, catalog=%r, schema=%r",
            _settings.server_hostname or "(EMPTY)",
            _settings.http_path or "(EMPTY)",
            "YES" if _settings.databricks_token else "NO",
            _settings.catalog,
            _settings.schema,
        )
        # Log raw env vars for debugging
        _log.debug(
            "Raw env vars: DATABRICKS_HOST=%r, DATABRICKS_SERVER_HOSTNAME=%r, WAREHOUSE_ID=%r, DATABRICKS_HTTP_PATH=%r",
            os.getenv("DATABRICKS_HOST"),
            os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            os.getenv("WAREHOUSE_ID"),
            os.getenv("DATABRICKS_HTTP_PATH"),
        )
    return _settings

