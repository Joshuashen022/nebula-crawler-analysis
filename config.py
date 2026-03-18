"""
Compatibility shim.

Many modules do `import config` while the actual settings live in `src/config.py`.
This file allows running analysis scripts from the repo root without tweaking
`PYTHONPATH` or import statements.
"""

from src.config import AUTH_TOKEN, DB_HOST, INTERVAL_COUNT, get_config

__all__ = ["DB_HOST", "INTERVAL_COUNT", "AUTH_TOKEN", "get_config"]

