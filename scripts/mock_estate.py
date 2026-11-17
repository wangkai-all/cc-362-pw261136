#!/usr/bin/env python3
"""Local mock upstream estate.

Serves three source shapes from one inventory:

* ``/catalog``            JSON rows           (api entry)
* ``/product/<id>.html``  product pages       (page entry)
* ``<run>/source/catalog.json``               (file entry)

Each response made for a collector carries two proof headers:

    X-Echo-Nonce:     the ``X-Read-Nonce`` the caller supplied
    X-Source-Receipt: HMAC-SHA256(source_key, nonce|canonical_path|sha256(body))

``source_key`` lives in ``<run>/secrets/source.key``.  Sinks know it, collectors
do not, so a valid receipt can only be obtained by actually fetching that URL
with that nonce.  Cached bytes from an earlier read carry an old nonce and
therefore cannot produce a valid receipt for a new task.

Collectors are addressed through ``/e/<entry_id>/...``.  ``POST /admin/outage``
with ``{"entry": "page", "down": true}`` severs exactly that entry's link; the
other two entries keep working.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import html
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collect.presets import ITEMS, PRESETS, page_ids  # noqa: E402
from collect.util import write_json_atomic  # noqa: E402


def load_or_create_key(path: Path) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return bytes.fromhex(path.read_text(encoding="utf-8").strip())
    key = os.urandom(32)
    path.write_text(key.hex() + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass
    return key


class Handler(BaseHTTPRequestHandler):
    server_version = "mock-estate/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        if getattr(self.server, "verbose", False):
            sys.stderr.write("mock: " + (fmt % args) + "\n")

    # ------------------------------------------------------------- helpers
    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        try:
            obj = json.loads(raw.decode("utf-8"))
        except ValueError:
            return {}
        return obj if isinstance(obj, dict) else {}

    def _send(self, status: int, raw: bytes, ctype: str,
              sign_path: str | None = None,
              extra_headers: dict | None = None) -> None:
        headers = dict(extra_headers or {})
        nonce = self.headers.get("X-Read-Nonce")
        if nonce and sign_path is not None:
            digest = hashlib.sha256(raw).hexdigest()
            material = f"{nonce}|{sign_path}|{digest}".encode("utf-8")
            receipt = hmac.new(self.server.source_key, material,
                               hashlib.sha256).hexdigest()
            headers["X-Echo-Nonce"] = nonce
            headers["X-Source-Receipt"] = receipt
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, obj, status: int = 200, sign_path: str | None = None) -> None:
        self._send(status, json.dumps(obj).encode("utf-8"), "application/json",
                   sign_path=sign_path)

    def _split(self) -> tuple[str | None, str]:
        path = urlsplit(self.path).path
        parts = path.split("/")
        if len(parts) >= 4 and parts[1] == "e":
            return parts[2], "/" + "/".join(parts[3:])
        return None, path

    def _entry_down(self, entry: str | None) -> bool:
        if entry is None:
            return False
        with self.server.lock:
            return bool(self.server.outage.get(entry))

    # -------------------------------------------------------------- routes
    def do_GET(self):  # noqa: N802
        entry, path = self._split()
        if self._entry_down(entry):
            # Severed link: close the socket without any response.
            try:
                self.connection.shutdown(2)
            except OSError:
                pass
            try:
                self.connection.close()
            except OSError:
                pass
            return

        if path in ("/health", "/__health"):
            return self._json({"ok": True, "entry": entry})

        if path == "/catalog":
            return self._json({"items": self.server.items,
                               "revision": self.server.revision}, sign_path=path)

        if path.startswith("/product/") and path.endswith(".html"):
            item_id = path[len("/product/"):-len(".html")]
            item = next((i for i in self.server.items
                         if item_id in (i["sku"], i["code"])), None)
            if item is None:
                return self._send(404, b"<html><body>not found</body></html>",
                                  "text/html; charset=utf-8", sign_path=path)
            return self._send(200, render_page(item), "text/html; charset=utf-8",
                              sign_path=path)

        if path == "/__state":
            with self.server.lock:
                outage = dict(self.server.outage)
            return self._json({"outage": outage, "revision": self.server.revision,
                               "items": len(self.server.items)})

        if path == "/__ids":
            return self._json({"ids": page_ids()})

        return self._json({"error": "not found", "path": path}, 404)

    def do_POST(self):  # noqa: N802
        _entry, path = self._split()
        if path == "/admin/outage":
            body = self._body()
            target, down = body.get("entry"), body.get("down")
            if not isinstance(target, str) or not isinstance(down, bool):
                return self._json({"error": "need entry:str, down:bool"}, 400)
            with self.server.lock:
                self.server.outage[target] = down
                snapshot = dict(self.server.outage)
            return self._json({"ok": True, "outage": snapshot})
        if path == "/admin/reset":
            with self.server.lock:
                self.server.outage.clear()
            return self._json({"ok": True, "outage": {}})
        return self._json({"error": "not found", "path": path}, 404)


def esc(value) -> str:
    return html.escape(str(value), quote=True)


def render_page(item: dict) -> bytes:
    """Deterministic page carrying aliases for both config presets."""
    page = (
        "<!doctype html>\n<html><head><meta charset='utf-8'>"
        f"<title>{esc(item['title'])}</title></head><body>\n"
        f"<div class='product' id='{esc(item['sku'])}' "
        f"data-id='{esc(item['sku'])}' data-sku='{esc(item['sku'])}' "
        f"data-code='{esc(item['code'])}'>\n"
        f"  <h1 class='label'>{esc(item['label'])}</h1>\n"
        f"  <h1 class='title'>{esc(item['title'])}</h1>\n"
        f"  <span class='price_cents'>{item['price_cents']}</span>\n"
        f"  <span class='price'>{item['price']}</span>\n"
        f"  <span class='in_stock'>{item['in_stock']}</span>\n"
        f"  <span class='stock'>{item['stock']}</span>\n"
        "</div>\n</body></html>\n"
    )
    return page.encode("utf-8")


def build_source_file(path: Path) -> str:
    write_json_atomic(path, {
        "feed": "acme-supply-cache",
        "generated_at": "2026-11-17T09:00:00Z",
        "items": ITEMS,
    })
    return str(path)


def make_server(host: str, port: int, run_dir: str | Path,
                verbose: bool = False) -> ThreadingHTTPServer:
    run = Path(run_dir)
    for sub in ("source", "mock", "files", "secrets", "work"):
        (run / sub).mkdir(parents=True, exist_ok=True)
    source_key = load_or_create_key(run / "secrets" / "source.key")
    source_file = build_source_file(run / "source" / "catalog.json")
    srv = ThreadingHTTPServer((host, port), Handler)
    srv.daemon_threads = True
    srv.verbose = verbose
    srv.source_key = source_key
    srv.items = [dict(i) for i in ITEMS]
    srv.revision = 1
    srv.outage = {}
    srv.lock = threading.Lock()
    srv.run_dir = str(run)
    srv.source_file = source_file
    return srv


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)
    srv = make_server(args.host, args.port, args.run_dir, args.verbose)
    url = f"http://{args.host}:{srv.server_address[1]}"
    print(json.dumps({"url": url, "source_file": srv.source_file,
                      "presets": sorted(PRESETS)}), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
