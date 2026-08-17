"""Lightweight KEDA-compatible metrics server for flexiq.

Exposes only the scaler endpoint, Prometheus metrics, and health check
on a dedicated port — ideal for KEDA external scaler or HTTP trigger.

Usage::

    flexiq scaler --app myapp:queue --port 9091

Or programmatically::

    from flexiq.scaler import serve_scaler
    serve_scaler(queue, host="127.0.0.1", port=9091)
"""

from __future__ import annotations

import json
import logging
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from flexiq.dashboard import build_scaler_response

if TYPE_CHECKING:
    from flexiq.app import Queue

logger = logging.getLogger("flexiq.scaler")


def serve_scaler(
    queue: Queue,
    host: str = "127.0.0.1",
    port: int = 9091,
    target_queue_depth: int = 10,
) -> None:
    """Start a minimal KEDA metrics HTTP server (blocking).

    Args:
        queue: The Queue instance to monitor.
        host: Bind address. Defaults to loopback; only bind to ``0.0.0.0``
            behind a trusted network boundary — the endpoints are unauthenticated.
        port: Bind port.
        target_queue_depth: Default scaling target hint for KEDA.
    """
    handler = _make_scaler_handler(queue, target_queue_depth)
    server = ThreadingHTTPServer((host, port), handler)
    print(f"flexiq scaler -> http://{host}:{port}")
    print("Endpoints: /api/scaler, /metrics, /health")
    print("Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _make_scaler_handler(queue: Queue, target_queue_depth: int) -> type:
    """Create a minimal request handler for KEDA endpoints."""

    class ScalerHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            try:
                self._route()
            except BrokenPipeError:
                pass
            except Exception:
                logger.exception("Error handling GET %s", self.path)
                self._json_response({"error": "Internal server error"}, status=500)

        def _route(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if path == "/api/scaler":
                q_name = qs.get("queue", [None])[0]
                self._json_response(
                    build_scaler_response(
                        queue,
                        queue_name=q_name,
                        target_queue_depth=target_queue_depth,
                    )
                )
            elif path == "/metrics":
                self._serve_prometheus_metrics()
            elif path == "/health":
                self._json_response({"status": "ok"})
            else:
                self._json_response({"error": "Not found"}, status=404)

        def _json_response(self, data: Any, status: int = 200) -> None:
            body = json.dumps(data, default=str).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _serve_prometheus_metrics(self) -> None:
            try:
                from prometheus_client import generate_latest

                body = generate_latest()
                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8",
                )
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except ImportError:
                self._json_response({"error": "prometheus-client not installed"}, status=501)

        def log_message(self, format: str, *args: Any) -> None:
            pass

    return ScalerHandler
