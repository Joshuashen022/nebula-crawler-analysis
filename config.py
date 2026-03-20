"""
Compatibility shim for `import config`.

Most modules in `src/` use `import config`, but the actual implementation lives in
`src/config.py`. When running from the project root (so the root directory is on
`sys.path`), this shim makes `import config` resolve correctly.
"""

from src.config import *  # noqa: F403

