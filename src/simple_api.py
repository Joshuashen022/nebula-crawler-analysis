import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import crawl
from analysis import geographical

HOST = "0.0.0.0"
PORT = 8080
ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def read_env_file(path: Path) -> dict:
    data = {}
    if not path.exists():
        return data

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def load_auth_token() -> str:
    env_data = read_env_file(ENV_PATH)
    token = env_data.get("TOKEN") or env_data.get("AUTH_TOKEN") or env_data.get("API_TOKEN")
    if not token:
        raise RuntimeError("No auth token found in .env. Add TOKEN=your_secret_token")
    return token


AUTH_TOKEN = load_auth_token()


class ApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _is_authorized(self) -> bool:
        auth_header = self.headers.get("Authorization", "")
        expected = f"Bearer {AUTH_TOKEN}"
        return auth_header == expected

    def do_GET(self):
        if not self._is_authorized():
            self._send_json(401, {"error": "unauthorized"})
            return

        if self.path == "/status":
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "simple_api",
                    "analysis_count": crawl.analysis_count,
                    "run_count": crawl.run_count,
                    "intervals_since_last_crawl": crawl.intervals_since_last_crawl,
                },
            )
            return

        if self.path == "/geographical":
            data = geographical.fetch_geographical_data()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "geographical",
                    "data": data,
                },
            )
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if not self._is_authorized():
            self._send_json(401, {"error": "unauthorized"})
            return

        if self.path == "/analyze":
            # fire-and-forget background analysis
            threading.Thread(
                target=crawl.run_analysis,
                daemon=True,
                name="analysis-runner",
            ).start()
            self._send_json(200, {"ok": True, "service": "analyze", "started": True})
            return

        if self.path == "/crawl":
            threading.Thread(
                target=crawl.run,
                daemon=True,
                name="crawl-runner",
            ).start()
            self._send_json(200, {"ok": True, "service": "crawl", "started": True})
            return

        self._send_json(404, {"error": "not found"})

    def log_message(self, fmt: str, *args):
        return

