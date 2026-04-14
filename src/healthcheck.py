"""Tiny HTTP health check server. Used by Fly.io rolling deployments."""
from __future__ import annotations

import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import structlog

logger = structlog.get_logger()


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args) -> None:
        # Suppress noisy default access logging
        pass


def start_healthcheck_server(port: int = 8080) -> None:
    """Start the healthcheck server in a background thread."""
    def _serve():
        server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
        logger.info("healthcheck_server_started", port=port)
        server.serve_forever()

    thread = Thread(target=_serve, daemon=True)
    thread.start()
