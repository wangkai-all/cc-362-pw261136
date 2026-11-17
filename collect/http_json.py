"""Minimal JSON-over-HTTP plumbing for the config service and the sink."""
from __future__ import annotations

import json
import re
import traceback
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlsplit


class ApiError(Exception):
    def __init__(self, status: int, message: str, **extra):
        super().__init__(message)
        self.status = status
        self.extra = extra


def route(method: str, pattern: str):
    regex = re.compile("^" + pattern + "$")

    def deco(fn):
        fn._route = (method, regex)
        return fn
    return deco


def collect_routes(namespace: dict) -> list:
    """Gather every decorated handler in a module namespace."""
    found = []
    for value in namespace.values():
        info = getattr(value, "_route", None)
        if info is not None:
            method, regex = info
            found.append((method, regex, value))
    return found


class JsonHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    routes: list = []

    def log_message(self, fmt, *args):
        if getattr(self.server, "verbose", False):
            import sys
            sys.stderr.write("http: " + (fmt % args) + "\n")

    def do_GET(self):  # noqa: N802
        self._dispatch("GET")

    def do_POST(self):  # noqa: N802
        self._dispatch("POST")

    def do_DELETE(self):  # noqa: N802
        self._dispatch("DELETE")

    @property
    def query(self) -> dict[str, list[str]]:
        return parse_qs(urlsplit(self.path).query)

    def _dispatch(self, method: str) -> None:
        path = urlsplit(self.path).path
        matched_path = False
        for m, regex, fn in self.routes:
            match = regex.match(path)
            if not match:
                continue
            matched_path = True
            if m != method:
                continue
            try:
                body = self._read_json() if method in ("POST", "PUT") else None
                result = fn(self, **match.groupdict(), body=body)
            except ApiError as exc:
                self.send_json(exc.status, {"error": str(exc), **exc.extra})
            except Exception as exc:  # noqa: BLE001
                traceback.print_exc()
                self.send_json(500, {"error": f"{type(exc).__name__}: {exc}"})
            else:
                if result is not None:
                    status, payload = result if isinstance(result, tuple) else (200, result)
                    self.send_json(status, payload)
            return
        if matched_path:
            self.send_json(405, {"error": f"method {method} not allowed"})
        else:
            self.send_json(404, {"error": f"no route for {method} {path}"})

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            obj = json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise ApiError(400, f"invalid JSON: {exc}") from exc
        if not isinstance(obj, dict):
            raise ApiError(400, "request body must be a JSON object")
        return obj

    def send_raw(self, status: int, raw: bytes, ctype: str,
                 headers: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(raw)

    def send_json(self, status: int, obj) -> None:
        raw = (json.dumps(obj, sort_keys=True) + "\n").encode("utf-8")
        self.send_raw(status, raw, "application/json")
