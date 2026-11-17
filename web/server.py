#!/usr/bin/env python3
"""Web console: publish config, observe entries, run tasks, inspect evidence.

Static single page + a small JSON proxy in front of the config service, sink
and mock estate.  No framework, no build step.
"""
from __future__ import annotations

import argparse
import json
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from collect import wire  # noqa: E402
from collect.http_json import (ApiError, JsonHandler, collect_routes,  # noqa: E402
                               route)

WEB_DIR = Path(__file__).resolve().parent / "static"


class Console(JsonHandler):
    routes = []

    def do_GET(self):  # noqa: N802
        path = urlsplit(self.path).path
        if path.startswith("/api/"):
            return self._dispatch("GET")
        if path == "/health":
            return self._dispatch("GET")
        if path in ("/", "/index.html"):
            return self._file("index.html", "text/html; charset=utf-8")
        if path == "/app.js":
            return self._file("app.js", "application/javascript; charset=utf-8")
        if path == "/style.css":
            return self._file("style.css", "text/css; charset=utf-8")
        self.send_json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        self._dispatch("POST")

    def _file(self, name: str, ctype: str) -> None:
        raw = (WEB_DIR / name).read_bytes()
        self.send_raw(200, raw, ctype)

    # -------------------------------------------------------------- access
    def _get(self, url: str) -> dict:
        status, raw, _ = wire.request("GET", url, ua="console")
        payload = wire.decode_json(raw) if raw else {}
        if status >= 400:
            raise ApiError(status, payload.get("error", f"HTTP {status}"),
                           **{k: v for k, v in payload.items() if k != "error"})
        return payload

    def _post(self, url: str, body: dict) -> dict:
        status, raw, _ = wire.request("POST", url, body, ua="console")
        payload = wire.decode_json(raw) if raw else {}
        if status >= 400:
            raise ApiError(status, payload.get("error", f"HTTP {status}"),
                           **{k: v for k, v in payload.items() if k != "error"})
        return payload


SINK_URL = ""
CONFIG_URL = ""
MOCK_URL = ""


@route("GET", r"/api/overview")
def overview(handler, body):
    out = {}
    for key, url in (("published", CONFIG_URL + "/published"),
                     ("versions", CONFIG_URL + "/versions"),
                     ("entries", SINK_URL + "/entries"),
                     ("tasks", SINK_URL + "/tasks"),
                     ("events", SINK_URL + "/events"),
                     ("mock", MOCK_URL + "/__state")):
        try:
            out[key] = handler._get(url)
        except ApiError as exc:
            out[key] = {"error": str(exc)}
        except wire.WireError as exc:
            out[key] = {"error": str(exc)}
    # The config service's /published returns the body directly; normalise it
    # into the same {body: ...} envelope the console uses everywhere else.
    if isinstance(out.get("published"), dict) and "body" not in out["published"]:
        out["published"] = {"body": out["published"]}
    return out


@route("POST", r"/api/config/publish")
def publish(handler, body):
    """Create a pair from a preset (or explicit mapping+scope), then publish."""
    mapping, scope = body.get("mapping"), body.get("scope")
    if mapping is None and scope is None:
        from collect.presets import PRESETS
        preset = PRESETS.get(body.get("preset"))
        if preset is None:
            raise ApiError(400, "unknown preset")
        mapping, scope = preset["mapping"], preset["scope"]
    if mapping is None or scope is None:
        raise ApiError(400, "both mapping and scope are required")
    version = body.get("version")
    created = handler._post(CONFIG_URL + "/versions",
                            {"mapping": mapping, "scope": scope,
                             **({"version": version} if version else {})})
    data = handler._post(CONFIG_URL + "/publish",
                         {"version": created["version"]})
    return {"created": created, "published": data}


@route("POST", r"/api/config/partial")
def partial(handler, body):
    """Demonstrate that a partial update is impossible (expected to fail)."""
    return handler._post(CONFIG_URL + "/versions", body)


@route("POST", r"/api/task")
def task(handler, body):
    return handler._post(SINK_URL + "/tasks", {
        "gate": body.get("gate", "barrier"),
        "publish": body.get("publish", "auto"),
        **({"version": body["version"]} if body.get("version") else {}),
    })


@route("GET", r"/api/tasks")
def tasks(handler, body):
    return handler._get(SINK_URL + "/tasks")


@route("GET", r"/api/task/(?P<task_id>[^/]+)")
def task_detail(handler, task_id, body):
    return handler._get(SINK_URL + f"/tasks/{task_id}")


@route("POST", r"/api/task/(?P<task_id>[^/]+)/commit")
def commit(handler, task_id, body):
    return handler._post(SINK_URL + f"/tasks/{task_id}/commit", {})


@route("POST", r"/api/task/(?P<task_id>[^/]+)/abort")
def abort(handler, task_id, body):
    return handler._post(SINK_URL + f"/tasks/{task_id}/abort",
                         {"reason": body.get("reason", "manual abort")})


@route("GET", r"/api/records")
def records(handler, body):
    task_id = (handler.query.get("task") or [None])[0]
    return handler._get(SINK_URL + "/records" + (f"?task={task_id}" if task_id else ""))


@route("GET", r"/api/events")
def events(handler, body):
    return handler._get(SINK_URL + "/events")


@route("GET", r"/api/access")
def access(handler, body):
    return handler._get(CONFIG_URL + "/access")


@route("POST", r"/api/observe")
def observe(handler, body):
    """Ask each entry to re-read the published config and report to the sink."""
    entries = handler._get(SINK_URL + "/entries")["entries"]
    results = []
    for entry in entries:
        target = entry["url"]
        try:
            status, raw, _ = wire.request("POST", target + "/observe", {},
                                          ua="console", timeout=5.0)
            evidence = wire.decode_json(raw) if raw else {}
            if status >= 400:
                raise wire.WireError(f"HTTP {status}", status=status)
            stored = handler._post(
                SINK_URL + f"/entries/{entry['entry_id']}/observe",
                {"config": evidence.get("config")})
            results.append({"entry_id": entry["entry_id"], "ok": True,
                            "matches_published": stored["matches_published"]})
        except (wire.WireError, ValueError, ApiError) as exc:
            results.append({"entry_id": entry["entry_id"], "ok": False,
                            "error": str(exc)})
            # A failing entry is itself an observation: record that it could
            # not produce a live confirmation.
            try:
                handler._post(SINK_URL + f"/entries/{entry['entry_id']}/observe",
                              {"config": {"source": "unreachable",
                                          "error": str(exc)}})
            except (ApiError, wire.WireError):
                pass
    return {"results": results}


@route("POST", r"/api/outage")
def outage(handler, body):
    """Withdraw or restore one entry's own link.

    ``up=false`` makes the entry process refuse BOTH its config read and its
    source read, so it genuinely cannot contribute a confirmation.  (The mock
    also has a provider-side outage switch, exercised by the tests; the
    console uses the entry-side one because it reflects a whole entry going
    away rather than a single source being unavailable.)
    """
    entries = {e["entry_id"]: e for e in
               handler._get(SINK_URL + "/entries")["entries"]}
    entry = entries.get(body.get("entry"))
    if entry is None:
        raise ApiError(404, "unknown entry")
    status, raw, _ = wire.request(
        "POST", entry["url"] + "/admin/link",
        {"up": not bool(body.get("down")), "reason": "console"}, ua="console")
    payload = wire.decode_json(raw) if raw else {}
    if status >= 400:
        raise ApiError(status, payload.get("error", f"HTTP {status}"))
    return payload


@route("GET", r"/health")
def health(handler, body):
    return {"ok": True}


def make_server(host: str, port: int, sink_url: str, config_url: str,
                mock_url: str) -> ThreadingHTTPServer:
    global SINK_URL, CONFIG_URL, MOCK_URL
    Console.routes = collect_routes(globals())
    SINK_URL, CONFIG_URL, MOCK_URL = (sink_url.rstrip("/"),
                                      config_url.rstrip("/"),
                                      mock_url.rstrip("/"))
    srv = ThreadingHTTPServer((host, port), Console)
    srv.daemon_threads = True
    return srv


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sink-url", required=True)
    ap.add_argument("--config-url", required=True)
    ap.add_argument("--mock-url", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8088)
    ap.add_argument("--run-dir", default=".run")
    args = ap.parse_args(argv)
    srv = make_server(args.host, args.port, args.sink_url, args.config_url,
                      args.mock_url)
    print(json.dumps({"url": f"http://{args.host}:{srv.server_address[1]}"}),
          flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
