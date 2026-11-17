#!/usr/bin/env python3
"""Config service: the only authority on (mapping, scope) pairs.

Invariants
----------
1. A version requires BOTH mapping and range; partial updates are rejected.
2. Mapping, scope and both hashes live in one row written in one transaction,
   so a reader sees either the whole old pair or the whole new pair.
3. A created body is immutable; publishing only moves a pointer.
4. The published pointer is monotonic.  Publishing an older version is refused
   (409), so a restarted/old component cannot rewind the estate.
5. Responses are ``Cache-Control: no-store`` and carry ETag + hash headers.
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collect.db import DB, dumps  # noqa: E402
from collect.http_json import (ApiError, JsonHandler, collect_routes,  # noqa: E402
                               route)
from collect.protocol import (build_body, validate_body, validate_mapping,  # noqa: E402
                              validate_scope)
from collect.util import utcnow  # noqa: E402

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS versions (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    version      TEXT NOT NULL UNIQUE,
    body_json    TEXT NOT NULL,
    body_sha256  TEXT NOT NULL UNIQUE,
    fingerprint  TEXT NOT NULL,
    scope_hash   TEXT NOT NULL,
    status       TEXT NOT NULL
                 CHECK (status IN ('candidate','published','superseded')),
    created_at   TEXT NOT NULL,
    published_at TEXT
);

CREATE TABLE IF NOT EXISTS pointer (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    seq        INTEGER NOT NULL REFERENCES versions(seq),
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS access_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    kind        TEXT NOT NULL,
    version     TEXT,
    body_sha256 TEXT,
    remote      TEXT,
    user_agent  TEXT
);
"""


def now() -> str:
    return utcnow()


class ConfigStore:
    def __init__(self, db_path: str):
        self.db = DB(db_path, SCHEMA)
        self.lock = threading.RLock()

    def create_version(self, mapping: dict, scope: dict,
                       version: str | None = None) -> dict:
        validate_mapping(mapping)
        validate_scope(scope)
        vid = version or f"v-{uuid.uuid4().hex[:12]}"
        body = build_body(vid, mapping, scope, now())
        validate_body(body)
        with self.lock, self.db.tx() as conn:
            if conn.execute("SELECT 1 FROM versions WHERE version=?",
                            (vid,)).fetchone():
                raise ApiError(409, f"version {vid} already exists")
            dup = conn.execute(
                "SELECT version FROM versions WHERE body_sha256=?",
                (body["body_sha256"],)).fetchone()
            if dup:
                raise ApiError(409, "byte-identical config already exists",
                               existing_version=dup["version"])
            conn.execute(
                "INSERT INTO versions (version, body_json, body_sha256,"
                " fingerprint, scope_hash, status, created_at)"
                " VALUES (?,?,?,?,?,'candidate',?)",
                (vid, json.dumps(body, sort_keys=True, indent=2),
                 body["body_sha256"], body["fingerprint"],
                 body["scope_hash"], body["created_at"]))
        return body

    def publish(self, version: str, expected_current: str | None) -> dict:
        with self.lock, self.db.tx() as conn:
            row = conn.execute(
                "SELECT * FROM versions WHERE version=?", (version,)).fetchone()
            if row is None:
                raise ApiError(404, f"unknown version {version}")
            pointer = conn.execute(
                "SELECT p.seq, v.version FROM pointer p"
                " JOIN versions v ON v.seq=p.seq").fetchone()
            current = pointer["version"] if pointer else None
            if expected_current is not None and expected_current != current:
                raise ApiError(409, "published pointer moved; re-read before"
                                    " publishing", current=current)
            if current == version:
                return {"version": version, "changed": False,
                        "previous": current,
                        "body": json.loads(row["body_json"])}
            if pointer is not None and row["seq"] < pointer["seq"]:
                raise ApiError(
                    409,
                    f"refusing to publish backwards: {version} (seq {row['seq']})"
                    f" predates published {current} (seq {pointer['seq']})",
                    current=current, attempted=version)
            ts = now()
            if pointer is not None:
                conn.execute("UPDATE versions SET status='superseded' WHERE seq=?",
                             (pointer["seq"],))
            conn.execute("UPDATE versions SET status='published', published_at=?"
                         " WHERE seq=?", (ts, row["seq"]))
            conn.execute(
                "INSERT INTO pointer (id, seq, updated_at) VALUES (1,?,?)"
                " ON CONFLICT(id) DO UPDATE SET seq=excluded.seq,"
                " updated_at=excluded.updated_at", (row["seq"], ts))
        with self.lock, self.db.tx() as conn:
            conn.execute(
                "INSERT INTO access_log (ts, kind, version, body_sha256, remote,"
                " user_agent) VALUES (?,?,?,?,?,?)",
                (now(), "publish", version, row["body_sha256"], "", ""))
        return {"version": version, "changed": True, "previous": current,
                "body": json.loads(row["body_json"])}

    def row(self, vid: str):
        row = self.db.one("SELECT * FROM versions WHERE version=?", (vid,))
        if row is None:
            raise ApiError(404, f"unknown version {vid}")
        return row

    def published_row(self):
        return self.db.one("SELECT v.* FROM pointer p JOIN versions v"
                           " ON v.seq=p.seq")

    def list_versions(self) -> list[dict]:
        return self.db.q(
            "SELECT version, body_sha256, fingerprint, scope_hash, status,"
            " created_at, published_at FROM versions ORDER BY seq")

    def log_access(self, kind: str, version: str | None, body_sha: str | None,
                   remote: str, ua: str) -> None:
        with self.lock, self.db.tx() as conn:
            conn.execute(
                "INSERT INTO access_log (ts, kind, version, body_sha256,"
                " remote, user_agent) VALUES (?,?,?,?,?,?)",
                (now(), kind, version, body_sha, remote, ua))

    def access_log(self, limit: int = 500) -> list[dict]:
        rows = self.db.q("SELECT * FROM access_log ORDER BY id DESC LIMIT ?",
                         (limit,))
        return list(reversed(rows))


STORE: ConfigStore


@route("GET", r"/health")
def health(handler, body):
    pub = STORE.published_row()
    return {"ok": True, "published": pub["version"] if pub else None,
            "versions": len(STORE.list_versions())}


@route("POST", r"/versions")
def create_version(handler, body):
    extra = set(body) - {"mapping", "scope", "version"}
    if extra:
        raise ApiError(400, f"unknown fields: {sorted(extra)}")
    if "mapping" not in body or "scope" not in body:
        raise ApiError(400, "a version requires BOTH mapping and scope;"
                            " partial configuration updates are refused")
    if "version" in body and not isinstance(body["version"], str):
        raise ApiError(400, "version must be a string")
    return 201, STORE.create_version(body["mapping"], body["scope"],
                                     body.get("version"))


@route("GET", r"/versions")
def list_versions(handler, body):
    pub = STORE.published_row()
    return {"versions": STORE.list_versions(),
            "published": pub["version"] if pub else None}


@route("GET", r"/versions/(?P<vid>[^/]+)")
def get_version(handler, vid, body):
    row = STORE.row(vid)
    handler.send_raw(200, row["body_json"].encode("utf-8"), "application/json", {
        "ETag": f'"{row["body_sha256"]}"',
        "X-Config-Version": row["version"],
        "X-Config-Body-SHA256": row["body_sha256"],
        "X-Config-Fingerprint": row["fingerprint"],
        "X-Config-Scope-Hash": row["scope_hash"],
    })
    STORE.log_access("version_read", row["version"], row["body_sha256"],
                     handler.client_address[0],
                     handler.headers.get("User-Agent", ""))
    return None


@route("GET", r"/published")
def get_published(handler, body):
    row = STORE.published_row()
    if row is None:
        raise ApiError(404, "nothing has been published yet")
    handler.send_raw(200, row["body_json"].encode("utf-8"), "application/json", {
        "ETag": f'"{row["body_sha256"]}"',
        "X-Config-Version": row["version"],
        "X-Config-Body-SHA256": row["body_sha256"],
        "X-Config-Fingerprint": row["fingerprint"],
        "X-Config-Scope-Hash": row["scope_hash"],
    })
    STORE.log_access("published_read", row["version"], row["body_sha256"],
                     handler.client_address[0],
                     handler.headers.get("User-Agent", ""))
    return None


@route("POST", r"/publish")
def publish(handler, body):
    extra = set(body) - {"version", "expected_current"}
    if extra:
        raise ApiError(400, f"unknown fields: {sorted(extra)}")
    version = body.get("version")
    if not isinstance(version, str) or not version:
        raise ApiError(400, "publish requires version")
    return STORE.publish(version, body.get("expected_current"))


@route("GET", r"/access")
def access(handler, body):
    return {"access": STORE.access_log()}


def make_server(host: str, port: int, db_path: str,
                verbose: bool = False) -> ThreadingHTTPServer:
    global STORE
    JsonHandler.routes = collect_routes(globals())
    STORE = ConfigStore(db_path)
    srv = ThreadingHTTPServer((host, port), JsonHandler)
    srv.daemon_threads = True
    srv.verbose = verbose
    return srv


def seed(store: ConfigStore, path: str, version: str) -> dict:
    """Create+publish a seed pair; idempotent for restarts."""
    spec = json.loads(Path(path).read_text(encoding="utf-8"))
    existing = store.published_row()
    if existing and existing["version"] == version:
        return json.loads(existing["body_json"])
    if store.db.one("SELECT 1 FROM versions WHERE version=?", (version,)):
        return store.publish(version, None)["body"]
    created = store.create_version(spec["mapping"], spec["scope"], version)
    return store.publish(created["version"], None)["body"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--seed", help="JSON file {mapping, scope} to create and publish")
    ap.add_argument("--seed-version", default="v-seed")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)
    srv = make_server(args.host, args.port, args.db, args.verbose)
    body = seed(STORE, args.seed, args.seed_version) if args.seed else None
    print(json.dumps({"url": f"http://{args.host}:{srv.server_address[1]}",
                      "db": str(Path(args.db).resolve()),
                      "seeded": body["version"] if body else None,
                      "body_sha256": body["body_sha256"] if body else None}),
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
