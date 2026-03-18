import os
try:
    # Optional: keeps local/dev runs working even if python-dotenv isn't installed.
    from dotenv import load_dotenv

    load_dotenv()
except ModuleNotFoundError:
    pass

DB_HOST = os.getenv("NEBULA_DATABASE_NAME", "localhost")
INTERVAL_COUNT = int(os.getenv("INTERVAL_COUNT", 6))
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "empty")

def get_config():
    return {
        "db_host": DB_HOST,
        "interval_count": INTERVAL_COUNT,
        "auth_token": AUTH_TOKEN,
    }