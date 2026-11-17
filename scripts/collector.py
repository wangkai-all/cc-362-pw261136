#!/usr/bin/env python3
"""Entry collector: one source, one prepared directory, live config reads.

Each instance is a separate process serving one entry:

* ``POST /prepare``  read the LIVE config (no cache fallback; a cached body
  arrives flagged ``source="cache"`` and is always refused), bind it to the
  task's pinned body hash, read its source with the task nonce, apply mapping
  and range, write staged rows to a private directory, return a report.
* ``POST /rollback`` delete the prepared directory for a task.
* ``POST /observe``  re-read the published config and report what it saw, for
  the console's per-entry confirmation view.  This never reports rows.
* ``GET  /state``    inspect prepared artifacts.
* ``GET  /health``

The collector keeps NO copy of the config between calls.  If the config
service is unreachable the read fails; it never falls back to a stale body.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collect import sources, wire  # noqa: E402
from collect.http_json import (ApiError, JsonHandler, collect_routes,  # noqa: E402
                               route)
from collect.protocol import validate_body  # noqa: E402
from collect.util import canonical, read_json, sha256_hex, utcnow, write_json_atomic  # noqa: E402


class Collector:
    def __init__(self, entry_id: str, kind: str, source: dict, work_root: str,
                 config_url: str, source_origin: str, ua: str,
                 hub_url: str | None = None):
        self.entry_id = entry_id
        self.kind = kind
        self.source = dict(source)
        if kind == "file":
            self.source["path"] = os.path.abspath(self.source["path"])
        self.work_root = Path(work_root)
        self.config_url = config_url.rstrip("/")
        self.source_origin = source_origin.rstrip("/")
        self.ua = ua
        self.lock = threading.RLock()
        self.work_root.mkdir(parents=True, exist_ok=True)
        self.purged_stale = self._purge_stale()
        self._hub = hub_url.rstrip("/") if hub_url else None
        self._stop = threading.Event()
        self.link_up = True           # entry's own egress (config + source)
        self.link_reason = ""
        if self._hub:
            threading.Thread(target=self._heartbeat_loop,
                             name=f"heartbeat-{entry_id}", daemon=True).start()

    def close(self) -> None:
        self._stop.set()

    def _heartbeat_loop(self) -> None:
        """Presence is reported by the entry process, not inferred by the sink."""
        while not self._stop.is_set():
            try:
                wire.post_json(self._hub + f"/entries/{self.entry_id}/heartbeat",
                               {"pid": os.getpid(), "kind": self.kind},
                               ua=self.ua, timeout=3.0)
            except (wire.WireError, ValueError):
                pass
            self._stop.wait(2.0)

    def _purge_stale(self) -> list[str]:
        """Discard staged state left by a previous process lifetime.

        Staged rows are never submitted by the entry; the sink holds its own
        copy.  An entry that restarts therefore cannot resurrect a prepared
        directory, and a task it was part of is either already terminal or
        gets aborted by the sink's watchdog.  The purge is recorded.
        """
        import shutil
        base = self.work_root / self.entry_id
        if not base.exists():
            return []
        purged = []
        for child in sorted(base.iterdir()):
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
                purged.append(child.name)
        if purged:
            write_json_atomic(base / "startup_purge.json", {
                "entry_id": self.entry_id, "at": utcnow(), "purged": purged})
        return purged

    def _remove_prepared(self, task_id: str) -> bool:
        """A committed task no longer needs its staged directory."""
        import shutil
        directory = self._task_dir(task_id)
        if directory.exists():
            with self.lock:
                shutil.rmtree(directory, ignore_errors=True)
            return True
        return False

    # -------------------------------------------------------------- config
    def read_live_config(self, pinned_sha: str | None = None) -> dict:
        """GET the config service directly.  No cache, no retry-from-disk."""
        if not self.link_up:
            raise ApiError(503, f"entry link down: {self.link_reason or 'severed'}")
        url = f"{self.config_url}/published"
        status, raw, headers = wire.get_raw(url, ua=self.ua)
        if status != 200:
            raise ApiError(503, f"config service returned HTTP {status}")
        body = wire.decode_json(raw)
        validate_body(body)
        etag = headers.get("ETag", "").strip('"')
        actual = sha256_hex(raw)
        evidence = {
            "source": "live",
            "url": url,
            "http_status": status,
            "body_sha256": body["body_sha256"],
            "etag": etag,
            "response_sha256": actual,
            "canonical_checked": True,
            "version": body["version"],
            "fingerprint": body["fingerprint"],
            "scope_hash": body["scope_hash"],
            "mapping": body["mapping"],
            "scope": body["scope"],
            "read_at": utcnow(),
            "read_id": sha256_hex(canonical([self.entry_id, url, actual,
                                             utcnow()])),
        }
        if etag and etag != body["body_sha256"]:
            raise ApiError(502, "config ETag disagrees with body seal")
        if pinned_sha is not None and body["body_sha256"] != pinned_sha:
            raise ApiError(409, "config service now serves a different body"
                                " than the task pinned",
                           pinned=pinned_sha, served=body["body_sha256"])
        return {"body": body, "evidence": evidence}

    # ------------------------------------------------------------ prepare
    def prepare(self, task_id: str, version: str | None, pinned_sha: str,
                nonce: str, challenge_path: str | None = None) -> dict:
        loaded = self.read_live_config(pinned_sha)
        body, evidence = loaded["body"], loaded["evidence"]
        if version is not None and body["version"] != version:
            raise ApiError(409, f"served version {body['version']} != task"
                                f" version {version}")
        source = dict(self.source)
        if self.kind == "page":
            # A page entry reads exactly the ids named by the scope it just
            # loaded.  If it was also configured with explicit ids, those must
            # not reach outside the config's range.
            scope_ids = list(body["scope"].get("ids") or [])
            source["ids"] = scope_ids
            configured = set(self.source.get("ids") or [])
            extra = configured - set(scope_ids)
            if extra:
                raise ApiError(409, f"page source ids not covered by scope:"
                                    f" {sorted(extra)}")

        if not self.link_up:
            raise ApiError(503, f"entry link down: {self.link_reason or 'severed'}")
        result = sources.collect(self.kind, source, self.source_origin, body,
                                 entry_id=self.entry_id, nonce=nonce,
                                 challenge_path=challenge_path)
        prepared = {
            "entry_id": self.entry_id,
            "task_id": task_id,
            "nonce": nonce,
            "prepared_at": utcnow(),
            "config": evidence,
            "source_evidence": result["source_evidence"],
            "digest": result["digest"],
            "rows": result["rows"],
        }
        with self.lock:
            self._write_prepared(task_id, prepared)
        return {
            "entry_id": self.entry_id,
            "task_id": task_id,
            "token": sha256_hex(canonical([task_id, self.entry_id,
                                           prepared["digest"],
                                           prepared["config"]["body_sha256"]])),
            "config": evidence,
            "source_evidence": result["source_evidence"],
            "digest": result["digest"],
            "record_count": len(result["rows"]),
            "rows": result["rows"],
        }

    def _task_dir(self, task_id: str) -> Path:
        if not task_id or "/" in task_id or ".." in task_id:
            raise ApiError(400, "invalid task id")
        return self.work_root / self.entry_id / task_id

    def _write_prepared(self, task_id: str, prepared: dict) -> None:
        directory = self._task_dir(task_id)
        directory.mkdir(parents=True, exist_ok=True)
        write_json_atomic(directory / "rows.json", {
            "digest": prepared["digest"], "rows": prepared["rows"]})
        write_json_atomic(directory / "prepared.json", prepared)
        # Best-effort: the directory is the staged state; its manifest binds
        # it to the exact config body and task nonce.
        (directory / "PREPARED").write_text(
            canonical({"task_id": task_id, "nonce": prepared["nonce"],
                       "body_sha256": prepared["config"]["body_sha256"],
                       "digest": prepared["digest"]}) + "\n", encoding="utf-8")

    def rollback(self, task_id: str) -> dict:
        import shutil
        directory = self._task_dir(task_id)
        existed = directory.exists()
        with self.lock:
            if existed:
                shutil.rmtree(directory)
        return {"ok": True, "entry_id": self.entry_id, "task_id": task_id,
                "removed": existed}

    def observe(self) -> dict:
        loaded = self.read_live_config()
        return {"entry_id": self.entry_id, "config": loaded["evidence"]}

    def state(self) -> dict:
        base = self.work_root / self.entry_id
        tasks = []
        if base.exists():
            for child in sorted(base.iterdir()):
                if not child.is_dir():
                    continue
                info = {"task_id": child.name}
                try:
                    prepared = read_json(child / "prepared.json")
                    info.update({
                        "prepared_at": prepared.get("prepared_at"),
                        "digest": prepared.get("digest"),
                        "record_count": len(prepared.get("rows") or []),
                        "config_body_sha256": (prepared.get("config") or {}).get("body_sha256"),
                        "config_version": (prepared.get("config") or {}).get("version"),
                    })
                except (OSError, ValueError):
                    info["incomplete"] = True
                tasks.append(info)
        return {"entry_id": self.entry_id, "kind": self.kind,
                "source": self.source, "prepared": tasks,
                "link_up": self.link_up}


COLLECTOR: Collector


@route("GET", r"/health")
def health(handler, body):
    return {"ok": True, "entry_id": COLLECTOR.entry_id,
            "pid": os.getpid(), "kind": COLLECTOR.kind}


@route("GET", r"/state")
def state(handler, body):
    return COLLECTOR.state()


@route("POST", r"/prepare")
def prepare(handler, body):
    required = ("task_id", "body_sha256", "nonce")
    missing = [k for k in required if not isinstance(body.get(k), str) or not body[k]]
    if missing:
        raise ApiError(400, f"prepare missing {missing}")
    return COLLECTOR.prepare(body["task_id"], body.get("version"),
                             body["body_sha256"], body["nonce"],
                             body.get("challenge_path"))


@route("POST", r"/rollback")
def rollback(handler, body):
    task_id = body.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise ApiError(400, "rollback requires task_id")
    return COLLECTOR.rollback(task_id)


@route("POST", r"/release")
def release(handler, body):
    task_id = body.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise ApiError(400, "release requires task_id")
    removed = COLLECTOR._remove_prepared(task_id)
    return {"ok": True, "entry_id": COLLECTOR.entry_id,
            "task_id": task_id, "removed": removed}


@route("POST", r"/admin/link")
def admin_link(handler, body):
    """Simulate this entry losing its own network link.

    Unlike the mock's provider-side outage, this cuts BOTH the config read and
    the source read, which is what "the entry went away" means.  Recovery is
    symmetrical and does not restore any prepared state.
    """
    up = body.get("up")
    if not isinstance(up, bool):
        raise ApiError(400, "need up: bool")
    with COLLECTOR.lock:
        COLLECTOR.link_up = up
        COLLECTOR.link_reason = "" if up else (body.get("reason") or "severed")
    if not up:
        COLLECTOR._purge_stale()
    return {"ok": True, "entry_id": COLLECTOR.entry_id, "link_up": up}


@route("POST", r"/observe")
def observe(handler, body):
    return COLLECTOR.observe()


@route("POST", r"/confirm")
def confirm(handler, body):
    """Confirmation for the barrier gate: a live read, no rows."""
    pinned = body.get("body_sha256")
    if not isinstance(pinned, str) or not pinned:
        raise ApiError(400, "confirm requires body_sha256")
    loaded = COLLECTOR.read_live_config(pinned)
    return {"entry_id": COLLECTOR.entry_id, "body_sha256": pinned,
            "config": loaded["evidence"]}


def make_server(host: str, port: int, entry_id: str, kind: str, source: dict,
                work_root: str, config_url: str, source_origin: str,
                hub_url: str | None = None,
                verbose: bool = False) -> ThreadingHTTPServer:
    global COLLECTOR
    JsonHandler.routes = collect_routes(globals())
    COLLECTOR = Collector(entry_id, kind, source, work_root, config_url,
                          source_origin, ua=f"collector/{entry_id}",
                          hub_url=hub_url)
    srv = ThreadingHTTPServer((host, port), JsonHandler)
    srv.daemon_threads = True
    srv.verbose = verbose
    return srv


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entry-id", required=True)
    ap.add_argument("--kind", required=True, choices=list(sources.SOURCE_KINDS))
    ap.add_argument("--source", required=True, help="JSON object source spec")
    ap.add_argument("--work-root", required=True)
    ap.add_argument("--config-url", required=True)
    ap.add_argument("--source-origin", required=True,
                    help="this entry's private route into the mock estate")
    ap.add_argument("--hub-url", help="sink URL; enables presence heartbeats")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)
    source = json.loads(args.source)
    srv = make_server(args.host, args.port, args.entry_id, args.kind, source,
                      args.work_root, args.config_url, args.source_origin,
                      args.hub_url, args.verbose)
    print(json.dumps({"url": f"http://{args.host}:{srv.server_address[1]}",
                      "entry_id": args.entry_id, "kind": args.kind,
                      "pid": os.getpid()}), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
