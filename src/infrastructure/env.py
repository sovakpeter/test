"""Environment loading helpers.

We primarily use environment variables for configuration.

For local development, we optionally load a `.env` file into the process
environment (env vars > .env > defaults).

Important:
- No secrets should be committed; `.env` is gitignored.
- We skip auto-loading during pytest by default to keep tests deterministic.
"""

from __future__ import annotations

import os
from pathlib import Path


_loaded = False


def load_dotenv_once() -> None:
    global _loaded
    if _loaded:
        return

    # Avoid surprising test behavior if a developer happens to have a local .env.
    if os.getenv("PYTEST_CURRENT_TEST") and os.getenv("LOAD_DOTENV_IN_TESTS", "false").lower() != "true":
        _loaded = True
        return

    env_file = os.getenv("ENV_FILE", ".env")

    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        # Optional dependency; if missing, just rely on OS env vars + defaults.
        _loaded = True
        return

    # Resolve relative paths from the repo root (current working dir is typically root).
    env_path = Path(env_file)
    if not env_path.is_absolute():
        env_path = Path.cwd() / env_path

    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    _loaded = True
