
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
import logging
logger = logging.getLogger("crawler.api")

import crawl
from analysis import global_geographical, global_new_found, global_each_crawl, \
    global_peer_neighbour, protocol_peer, protocol_distribution_country, peer_uptime_protocol, \
    peer_uptime_percentage, peer_uptime_country, peer_uptime_agent, multi_hash_prefix, multi_hash_prefix_country,\
    multi_hash_prefix_asn, multi_hash_count_by_update_duration, agent_peer_count, \
    agent_distribution_country, compromized_peer_metadata
import config

HOST = "0.0.0.0"
PORT = 8080

AUTH_TOKEN = config.AUTH_TOKEN

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
        logger.info(f"GET request: {self.path}")
        if not self._is_authorized():
            self._send_json(401, {"error": "unauthorized"})
            return

        if self.path == "/config":
            self._send_json(200, {"ok": True, "service": "config", "config": config.get_config()})
            return

        if self.path == "/status":
            crawl_thread = getattr(self.server, "crawl_thread", None)
            monitor_thread = getattr(self.server, "monitor_thread", None)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "simple_api",
                    "analysis_count": crawl.analysis_count,
                    "run_count": crawl.run_count,
                    "crawl_thread": crawl_thread.is_alive() if crawl_thread else False,
                    "monitor_thread": monitor_thread.is_alive() if monitor_thread else False,
                    "intervals_since_last_crawl": crawl.intervals_since_last_crawl,
                },
            )
            return

        if self.path == "/global-geographical":
            data = global_geographical.fetch_geographical_data()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "geographical",
                    "data": data,
                },
            )
            return
        if self.path == "/global-new-found":
            data = global_new_found.fetch_all_multi_hashes()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "new-found",
                    "data": data,
                },
            )
            return
        if self.path == "/global-each-crawl":
            results_dir = Path(__file__).resolve().parent / "results"
            data = global_each_crawl.load_crawl_stats(results_dir)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "each-crawl",
                    "data": data,
                },
            )
            return

        if self.path == "/global-peer-neighbour":
            data = global_peer_neighbour.fetch_neighbor_peer()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "peer-neighbour",
                    "data": data,
                },
            )
            return
        if self.path == "/protocol-peer":
            raw = protocol_peer.fetch_protocol_peer_count()
            data = protocol_peer.sort_protocol_peer_count(raw)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "protocol-peer",
                    "data": data,
                },
            )
            return
        if self.path.startswith("/protocol-distribution-country"):
            # `BaseHTTPRequestHandler.path` includes the query string (e.g. `/foo?country=US`).
            # `parse_qs()` expects *only* the query portion, so we must extract it first.
            query = urlparse(self.path).query
            qs = parse_qs(query)
            country = (qs.get("country") or [None])[0]
            logger.info(f"protocol-distribution-country: {country}, {self.path}")
            data = protocol_distribution_country.get_protocol_distribution_for_country(country, top_n=30)
            logger.info(f"protocol-distribution-country data: {len(data)}")
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "protocol-distribution-country",
                    "data": data,
                },
            )
            return
        if self.path.startswith("/country-distribution-protocol"):
            query = urlparse(self.path).query
            qs = parse_qs(query)
            protocol = (qs.get("protocol") or [None])[0]
            logger.info(f"country-distribution-protocol: {protocol}, {self.path}")
            data = protocol_distribution_country.get_country_distribution_for_protocol(protocol, top_n=30)
            logger.info(f"country-distribution-protocol data: {len(data)}")
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "country-distribution-protocol",
                    "data": data,
                },
            )
            return
        if self.path == "/reliable-protocol-counts":
            data = peer_uptime_protocol.get_reliable_protocol_counts(0.9)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "peer-uptime-protocol",
                    "data": data,
                },
            )
            return
        if self.path == "/uptime-percentage-distributions":
            data = peer_uptime_percentage.get_uptime_percentage_distributions()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "uptime-percentage-distributions",
                    "data": data,
                },
            )
            return
        if self.path == "/reliable-peers-by-country":
            data = peer_uptime_country.get_reliable_peers_by_country()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "reliable-peers-by-country",
                    "data": data,
                },
            )
            return
        if self.path == "/reliable-agent-counts":
            data = peer_uptime_agent.get_reliable_agent_counts(0.9)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "reliable-agent-counts",
                    "data": data,
                },
            )
            return
        if self.path == "/multi-hash-prefix":
            data = multi_hash_prefix.fetch_all_multi_hashes()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "multi-hash-prefix",
                    "data": data,
                },
            )
            return
        if self.path == "/multi-hash-prefix-country":
            data = multi_hash_prefix_country.get_peer_id_prefix_by_country()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "multi-hash-prefix-country",
                    "data": data,
                },
            )
            return
        if self.path == "/multi-hash-prefix-asn":
            data = multi_hash_prefix_asn.split_peer_ids_by_asn_count()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "multi-hash-prefix-asn",
                    "data": data,
                },
            )
            return
        if self.path == "/multi-hash-count-by-update-duration":
            data = multi_hash_count_by_update_duration.get_multi_hash_count_by_update_duration()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "multi-hash-count-by-update-duration",
                    "data": data,
                },
            )
            return
        if self.path == "/agent-peer-count":
            data = agent_peer_count.get_agent_peer_count()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent-peer-count",
                    "data": data,
                },
            )
            return

        if self.path == "/agent-country-with-without":
            data = agent_distribution_country.get_country_with_without_agent()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent-country-with-without",
                    "data": data,
                },
            )
            return
        if self.path.startswith("/agent-distribution-country"):
            query = urlparse(self.path).query
            qs = parse_qs(query)
            country = (qs.get("country") or [None])[0]
            logger.info(f"agent-distribution-country: {country}, {self.path}")
            data = agent_distribution_country.get_agent_distribution_for_country(country)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent-distribution-country",
                    "data": data,
                }, 
            )
            return
        if self.path.startswith("/country-distribution-agent"):
            query = urlparse(self.path).query
            qs = parse_qs(query)
            agent = (qs.get("agent") or [None])[0]
            logger.info(f"country-distribution-agent: {agent}, {self.path}")
            data = agent_distribution_country.get_country_distribution_for_agent(agent)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent-distribution-country",
                    "data": data,
                },
            )
            return
        if self.path.startswith("/agent-country-top-share"):
            data = agent_distribution_country.get_country_top_agent_share()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent-country-top-share",
                    "data": data,
                },
            )
            return
        if self.path.startswith("/agent-country-share"):
            query = urlparse(self.path).query
            qs = parse_qs(query)
            agent = (qs.get("agent") or [None])[0]
            logger.info(f"agent-country-share: {agent}, {self.path}")
            data = agent_distribution_country.get_country_share_for_agent(agent)
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "agent-country-share",
                    "data": data,
                },
            )
            return
        if self.path == "/compromized-peer-metadata":
            data = compromized_peer_metadata.build_compromized_peer_metadata()
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "compromized-peer-metadata",
                    "data": data,
                },
            )
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        logger.info(f"POST request: {self.path}")
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
            self.server.crawl_thread = threading.Thread(
                target=crawl.run,
                daemon=True,
                name="crawl-runner",
            )
            self.server.crawl_thread.start()
            self.server.monitor_thread = threading.Thread(
                target=crawl.run_monitor,
                daemon=True,
                name="monitor-runner",
            )
            self.server.monitor_thread.start()
            self._send_json(200, {"ok": True, "service": "crawl", "started": True, "monitor": True})
            return

        self._send_json(404, {"error": "not found"})

    def log_message(self, fmt: str, *args):
        return

