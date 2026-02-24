# src/infrastructure/auth.py
"""
Authentication utilities for OBO flow.

Pure functional module for extracting and validating identity information
from request headers. Implementation agnostic - does not import Flask.
"""

import os
from enum import Enum
from typing import Any

from src.infrastructure.logging import get_logger

logger = get_logger(__name__)

# Documented headers (Databricks Apps)
H_ACCESS_TOKEN = "x-forwarded-access-token"
H_EMAIL = "x-forwarded-email"
H_USER = "x-forwarded-user"
H_PREFERRED = "x-forwarded-preferred-username"


class AuthMethod(str, Enum):
    """Authentication method used for the request."""
    OBO = "OBO"                      # On-Behalf-Of (Databricks Apps production)
    SERVICE_PRINCIPAL = "SP"         # Service Principal / PAT (no user context)
    LOCAL = "LOCAL"                  # Local development (OS user fallback)


def detect_auth_method(headers: dict[str, Any] | None) -> AuthMethod:
    """
    Detect authentication method based on headers.
    
    Args:
        headers: Dictionary of request headers (can be None)
        
    Returns:
        AuthMethod enum value indicating detected auth method
    """
    if headers is None:
        headers = {}
    
    # Check for OBO token first (production Databricks Apps)
    obo_token = extract_obo_token(headers)
    if obo_token:
        logger.debug("Auth method detected: OBO (token present)")
        return AuthMethod.OBO
    
    # Check for forwarded email/user headers (IdP present but no token)
    user_info = get_user_info(headers)
    if user_info.get("email") or user_info.get("user_id"):
        logger.debug("Auth method detected: OBO (user headers present, but NO token)")
        return AuthMethod.OBO
    
    # Check if we have a Databricks token configured (Service Principal)
    if os.getenv("DATABRICKS_TOKEN"):
        logger.debug("Auth method detected: SERVICE_PRINCIPAL (DATABRICKS_TOKEN env var set)")
        return AuthMethod.SERVICE_PRINCIPAL
    
    # Fall back to local dev mode
    logger.debug("Auth method detected: LOCAL (no OBO headers, no DATABRICKS_TOKEN)")
    return AuthMethod.LOCAL


def extract_obo_token(headers: dict[str, Any]) -> str | None:
    """
    Extract OBO access token from headers.
    
    Args:
        headers: Dictionary of request headers (case-insensitive lookup preferred)
        
    Returns:
        Token string or None
    """
    # Try common casing
    token = headers.get(H_ACCESS_TOKEN) or headers.get(H_ACCESS_TOKEN.title())
    result = str(token).strip() if token else None
    
    # --- DIAGNOSTIC LOGGING FOR PROD DEBUGGING ---
    if result:
        logger.debug("OBO token extracted from header %r (length=%d)", H_ACCESS_TOKEN, len(result))
    else:
        # Log available headers (keys only, no values for security)
        header_keys = list(headers.keys()) if headers else []
        logger.debug(
            "No OBO token found. Header %r not present. Available headers: %s",
            H_ACCESS_TOKEN,
            header_keys,
        )
    
    return result


def get_user_info(headers: dict[str, Any]) -> dict[str, str | None]:
    """
    Extract user identity from headers.
    
    Args:
        headers: Dictionary of request headers
        
    Returns:
        Dictionary with email, user_id, preferred_username
    """
    def _get(key: str) -> str | None:
        val = headers.get(key) or headers.get(key.title())
        return str(val).strip() if val else None

    return {
        "email": _get(H_EMAIL),
        "user_id": _get(H_USER),
        "preferred_username": _get(H_PREFERRED),
    }


def get_current_user_email(headers: dict[str, Any] | None) -> str:
    """
    Get user email for audit logging, with fallback.
    
    Args:
        headers: Request headers (can be None)
        
    Returns:
        Email address, OS username, or 'unknown_user'
    """
    if headers is None:
        headers = {}
    
    info = get_user_info(headers)
    email = info.get("email") or info.get("preferred_username")
    
    if email:
        # Sanitize "john.doe (External)" format
        if "(" in email:
            return email.split("(")[0].strip()
        return email
    
    # Fallback to OS user for local development
    try:
        return os.getlogin()
    except OSError:
        # os.getlogin() can fail in some environments (e.g., services, containers)
        return os.getenv("USERNAME") or os.getenv("USER") or "unknown_user"
