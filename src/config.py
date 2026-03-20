import os
from dotenv import load_dotenv
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

load_dotenv()

DB_HOST = os.getenv("NEBULA_DATABASE_NAME", "localhost")
INTERVAL_COUNT = int(os.getenv("INTERVAL_COUNT", 6))
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "empty")

def get_config():
    return {
        "db_host": DB_HOST,
        "interval_count": INTERVAL_COUNT,
        "auth_token": AUTH_TOKEN,
    }

def setup_logging() -> None:
    """
    Configure application logging for the API entrypoint.

    Env vars:
      - LOG_LEVEL: default INFO
      - LOG_DIR: default <repo_root>/logs
      - LOG_FILE: default crawler.log
      - LOG_MAX_BYTES: default 10MB
      - LOG_BACKUP_COUNT: default 5
    """

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    repo_root = Path(__file__).resolve().parent.parent
    log_dir = Path(os.getenv("LOG_DIR", str(repo_root / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = Path(os.getenv("LOG_FILE", str(log_dir / "crawler.log")))
    max_bytes = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))
    backup_count = int(os.getenv("LOG_BACKUP_COUNT", "5"))

    log_format = os.getenv(
        "LOG_FORMAT",
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    formatter = logging.Formatter(log_format)

    # Avoid duplicate handlers if this module is imported/reloaded.
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    root_logger.setLevel(level)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)
