#!/usr/bin/env python3
"""Sink + orchestrator.

Owns tasks, drives the three entry collectors, independently re-reads every
source, and publishes all rows of a task in one transaction.

Gates
-----
``barrier``   No source is read until EVERY entry has confirmed the pinned
              config body with a live read.  Then all entries prepare, the
              sink verifies independently, and the task publishes.
``prefetch``  Entries prepare independently as soon as they are dispatched
              (each reads its own config + source and stages rows).  The task
              still stops before publication until every entry verifies.

Publish modes
-------------
``auto``      Commit as soon as every entry verifies.
``manual``    Stop in ``ready`` with rows staged but unpublished.  A manual
              commit re-checks the published version AND re-reads every
              source, so a delayed publish can never ship stale rows.

Failure
-------
Any failed entry aborts the whole task: staged rows are deleted, every
reachable entry is told to remove its prepared directory, and unreachable
entries get a durable rollback-queue row that is flushed on their next
heartbeat.  Evidence columns are retained so the failure stays auditable.

Stale-version safety
--------------------
A task pins the body hash at creation.  Every prepare must present that exact
hash; the sink re-reads the config service itself; publication is refused if
the pointer has moved to another version; and the config service refuses to
publish backwards.  An old entry that comes back online can therefore never
push an outdated pair into a commit.
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import hashlib
import hmac
import json
import os
import secrets
import sys
import threading
import traceback
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collect import sources, wire  # noqa: E402
from collect.db import DB, dumps, loads  # noqa: E402
from collect.http_json import (ApiError, JsonHandler, collect_routes,  # noqa: E402
                               route)
from collect.protocol import validate_body  # noqa: E402
from collect.util import canonical, sha256_hex, utcnow  # noqa: E402

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS entries (
    entry_id      TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,
    url           TEXT NOT NULL,
    mock_prefix   TEXT NOT NULL,
    source_json   TEXT NOT NULL,
    registered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entry_status (
    entry_id             TEXT PRIMARY KEY,
    last_seen            TEXT,
    pid                  INTEGER,
    observed_version     TEXT,
    observed_body_sha256 TEXT,
    observed_at          TEXT,
    observation_json     TEXT
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id      TEXT PRIMARY KEY,
    gate         TEXT NOT NULL CHECK (gate IN ('barrier','prefetch')),
    publish_mode TEXT NOT NULL CHECK (publish_mode IN ('auto','manual')),
    state        TEXT NOT NULL,
    version      TEXT NOT NULL,
    body_json    TEXT NOT NULL,
    body_sha256  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    finished_at  TEXT,
    note         TEXT
);

CREATE TABLE IF NOT EXISTS task_entries (
    task_id         TEXT NOT NULL,
    entry_id        TEXT NOT NULL,
    state           TEXT NOT NULL,
    nonce           TEXT NOT NULL,
    config_evidence TEXT,
    source_evidence TEXT,
    verification    TEXT,
    digest          TEXT,
    record_count    INTEGER NOT NULL DEFAULT 0,
    token           TEXT,
    error           TEXT,
    updated_at      TEXT,
    PRIMARY KEY (task_id, entry_id)
);

CREATE TABLE IF NOT EXISTS records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     TEXT NOT NULL,
    entry_id    TEXT NOT NULL,
    item_id     TEXT NOT NULL,
    title       TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    evidence    TEXT NOT NULL,
    published   INTEGER NOT NULL DEFAULT 0,
    UNIQUE (task_id, entry_id, item_id)
);

CREATE TABLE IF NOT EXISTS rollback_queue (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id   TEXT NOT NULL,
    entry_id  TEXT NOT NULL,
    reason    TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    done_at   TEXT,
    UNIQUE (task_id, entry_id)
);

CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       TEXT NOT NULL,
    kind     TEXT NOT NULL,
    entry_id TEXT,
    version  TEXT,
    detail   TEXT NOT NULL
);
"""

OPEN_STATES = ("pending", "confirming", "preparing", "verifying", "ready")


def now() -> str:
    return utcnow()


class Sink:
    def __init__(self, db_path: str, run_dir: str, config_url: str,
                 mock_url: str, verbose: bool = False):
        self.db = DB(db_path, SCHEMA)
        self.run_dir = Path(run_dir)
        self.config_url = config_url.rstrip("/")
        self.mock_url = mock_url.rstrip("/")
        self.verbose = verbose
        self.lock = threading.RLock()
        self.task_locks: dict[str, threading.RLock] = {}
        self._stop = threading.Event()
        key_path = self.run_dir / "secrets" / "source.key"
        self.source_key = bytes.fromhex(key_path.read_text(encoding="utf-8").strip())
        threading.Thread(target=self._watchdog, name="sink-watchdog",
                         daemon=True).start()

    def close(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------- helpers
    def _event(self, conn, kind: str, detail: str, entry_id: str | None = None,
               version: str | None = None) -> None:
        conn.execute(
            "INSERT INTO events (ts, kind, entry_id, version, detail)"
            " VALUES (?,?,?,?,?)", (now(), kind, entry_id, version, detail))

    def _task_lock(self, task_id: str) -> threading.RLock:
        with self.lock:
            return self.task_locks.setdefault(task_id, threading.RLock())

    def _entry(self, entry_id: str) -> dict:
        row = self.db.one("SELECT * FROM entries WHERE entry_id=?", (entry_id,))
        if row is None:
            raise ApiError(404, f"unknown entry {entry_id}")
        row["source"] = loads(row.pop("source_json"))
        return row

    def _entries(self) -> list[dict]:
        rows = self.db.q("SELECT * FROM entries ORDER BY entry_id")
        for row in rows:
            row["source"] = loads(row.pop("source_json"))
        return rows

    # ------------------------------------------------------------- config
    def read_published_config(self) -> dict | None:
        try:
            status, raw, headers = wire.get_raw(
                self.config_url + "/published", ua="sink")
        except wire.WireError as exc:
            raise ApiError(503, f"config service unreachable: {exc}") from exc
        if status == 404:
            return None
        if status != 200:
            raise ApiError(502, f"config service returned HTTP {status}")
        body = wire.decode_json(raw)
        validate_body(body)
        etag = headers.get("ETag", "").strip('"')
        if etag and etag != body["body_sha256"]:
            raise ApiError(502, "config ETag does not match the sealed body")
        return body

    def _guard_pinned(self, pinned: str) -> dict:
        published = self.read_published_config()
        if published is None:
            raise ApiError(409, "configuration was unpublished")
        if published["body_sha256"] != pinned:
            raise ApiError(409, "published configuration moved past the task pin",
                           pinned=pinned, published=published["body_sha256"])
        return published

    # ------------------------------------------------------------- entries
    def register_entries(self, spec: dict) -> list[dict]:
        ts = now()
        with self.lock, self.db.tx() as conn:
            for entry in spec["entries"]:
                if entry["kind"] not in sources.SOURCE_KINDS:
                    raise ApiError(400, f"bad kind {entry['kind']!r}")
                conn.execute(
                    "INSERT INTO entries (entry_id, kind, url, mock_prefix,"
                    " source_json, registered_at) VALUES (?,?,?,?,?,?)"
                    " ON CONFLICT(entry_id) DO UPDATE SET kind=excluded.kind,"
                    " url=excluded.url, mock_prefix=excluded.mock_prefix,"
                    " source_json=excluded.source_json",
                    (entry["entry_id"], entry["kind"],
                     entry["collector_url"].rstrip("/"),
                     entry.get("mock_prefix", ""), dumps(entry["source"]), ts))
                conn.execute(
                    "INSERT OR IGNORE INTO entry_status (entry_id) VALUES (?)",
                    (entry["entry_id"],))
        return self._entries()

    def observe(self, entry_id: str, config: dict) -> dict:
        self._entry(entry_id)
        published = self.read_published_config()
        matches = bool(published and config.get("body_sha256")
                       == published["body_sha256"] and config.get("source") == "live")
        with self.lock, self.db.tx() as conn:
            # ``last_seen`` is owned exclusively by the entry's own heartbeat.
            # An observation proxied by the console proves nothing about the
            # entry being alive -- it may be the console recording that the
            # entry is unreachable.
            conn.execute(
                "INSERT INTO entry_status (entry_id, observed_version,"
                " observed_body_sha256, observed_at, observation_json)"
                " VALUES (?,?,?,?,?) ON CONFLICT(entry_id) DO UPDATE SET"
                " observed_version=excluded.observed_version,"
                " observed_body_sha256=excluded.observed_body_sha256,"
                " observed_at=excluded.observed_at,"
                " observation_json=excluded.observation_json",
                (entry_id, config.get("version"),
                 config.get("body_sha256"), now(), dumps(config)))
            self._event(conn, "entry_observed",
                        f"{entry_id} live={config.get('source')}"
                        f" body={str(config.get('body_sha256'))[:12]}"
                        f" matches_published={matches}",
                        entry_id=entry_id, version=config.get("version"))
        return {"ok": True, "matches_published": matches,
                "published": published["version"] if published else None}

    def heartbeat(self, entry_id: str, body: dict) -> dict:
        self._entry(entry_id)
        with self.lock, self.db.tx() as conn:
            conn.execute(
                "INSERT INTO entry_status (entry_id, last_seen, pid)"
                " VALUES (?,?,?) ON CONFLICT(entry_id) DO UPDATE SET"
                " last_seen=excluded.last_seen, pid=excluded.pid",
                (entry_id, now(), body.get("pid")))
        now_state = []
        flushed = self._flush_rollbacks(entry_id)
        published = None
        try:
            cfg = self.read_published_config()
            published = cfg["version"] if cfg else None
        except ApiError:
            pass
        return {"ok": True, "published": published,
                "rollbacks_flushed": flushed,
                "aborted_tasks": now_state}

    def _abort_lost_tasks(self, stale_after: float = 12.0) -> list[str]:
        """Abort tasks that are waiting on an entry which has gone silent.

        ``ready`` is deliberately excluded: its rows are already verified and
        a later publish re-verifies them, so silence cannot corrupt it.
        """
        waiting = self.db.q(
            "SELECT DISTINCT t.task_id, t.state FROM tasks t"
            " JOIN task_entries e ON e.task_id=t.task_id"
            " WHERE e.state NOT IN ('verified','failed','rolled_back')"
            " AND t.state IN ('confirming','preparing','verifying')")
        aborted = []
        for task_id in {row["task_id"] for row in waiting}:
            silent = []
            for entry in self._entries():
                status = self.db.one(
                    "SELECT last_seen FROM entry_status WHERE entry_id=?",
                    (entry["entry_id"],))
                seen = status["last_seen"] if status else None
                if not self._fresh(seen, seconds=stale_after):
                    task_entry = self.db.one(
                        "SELECT state FROM task_entries WHERE task_id=?"
                        " AND entry_id=?", (task_id, entry["entry_id"]))
                    if task_entry and task_entry["state"] not in (
                            "verified", "failed", "rolled_back",
                            "confirmed", "prepared"):
                        silent.append(entry["entry_id"])
            if not silent:
                continue
            reason = ("entries stopped reporting while the task was in flight: "
                      + ", ".join(silent))
            with self._task_lock(task_id):
                self._abort(task_id, reason)
            aborted.append(task_id)
        return aborted

    def _watchdog(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(2.0)
            try:
                self._abort_lost_tasks()
            except Exception:  # noqa: BLE001
                traceback.print_exc()

    def _queue_rollback(self, conn, task_id: str, entry_id: str,
                        reason: str) -> None:
        conn.execute(
            "INSERT INTO rollback_queue (task_id, entry_id, reason, queued_at)"
            " VALUES (?,?,?,?) ON CONFLICT(task_id, entry_id) DO UPDATE SET"
            " reason=excluded.reason, queued_at=excluded.queued_at",
            (task_id, entry_id, reason, now()))

    def _flush_rollbacks(self, entry_id: str) -> int:
        """Tell a (re)connected entry to discard every still-pending task."""
        pending = self.db.q(
            "SELECT * FROM rollback_queue WHERE entry_id=? AND done_at IS NULL",
            (entry_id,))
        entry = self._entry(entry_id)
        flushed = 0
        for row in pending:
            try:
                wire.post_json(entry["url"] + "/rollback",
                               {"task_id": row["task_id"],
                                "reason": row["reason"]},
                               ua="sink", timeout=5.0)
            except (wire.WireError, ValueError):
                continue
            with self.lock, self.db.tx() as conn:
                conn.execute("UPDATE rollback_queue SET done_at=? WHERE id=?",
                             (now(), row["id"]))
                self._event(conn, "prepared_state_withdrawn",
                            f"{row['task_id']} rolled back at {entry_id}",
                            entry_id=entry_id)
            flushed += 1
        return flushed

    def entries_view(self) -> list[dict]:
        try:
            published = self.read_published_config()
        except ApiError:
            published = None
        out = []
        for entry in self._entries():
            status = self.db.one(
                "SELECT * FROM entry_status WHERE entry_id=?", (entry["entry_id"],))
            item = {
                "entry_id": entry["entry_id"], "kind": entry["kind"],
                "url": entry["url"], "source": entry["source"],
                "prepared_state": self._prepared_state(entry),
            }
            observation = loads(status["observation_json"]) if status else None
            item.update({
                "last_seen": status["last_seen"] if status else None,
                "pid": status["pid"] if status else None,
                "reachable": self._fresh(status["last_seen"]) if status else False,
                "observation": observation,
                "observed_version": status["observed_version"] if status else None,
                # A match requires a LIVE read of the published body.  A
                # cached/unreachable report can never be shown as a match.
                "matches_published": bool(
                    published and observation
                    and observation.get("source") == "live"
                    and observation.get("body_sha256") == published["body_sha256"]),
                "published": published["version"] if published else None,
            })
            out.append(item)
        return out

    def _prepared_state(self, entry: dict) -> dict:
        try:
            status, raw, _ = wire.request("GET", entry["url"] + "/state",
                                          ua="sink", timeout=3.0)
            if status == 200:
                return wire.decode_json(raw)
        except (wire.WireError, ValueError):
            pass
        return {"unreachable": True}

    @staticmethod
    def _fresh(ts: str | None, seconds: float = 8.0) -> bool:
        if not ts:
            return False
        from datetime import datetime, timezone
        try:
            then = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            return False
        return (datetime.now(timezone.utc) - then).total_seconds() <= seconds

    # --------------------------------------------------------------- tasks
    def create_task(self, gate: str, publish_mode: str,
                    version: str | None) -> dict:
        if gate not in ("barrier", "prefetch"):
            raise ApiError(400, "gate must be barrier or prefetch")
        if publish_mode not in ("auto", "manual"):
            raise ApiError(400, "publish must be auto or manual")
        if not self._entries():
            raise ApiError(409, "no entries registered")
        published = self.read_published_config()
        if published is None:
            raise ApiError(409, "no configuration has been published")
        if version is not None and version != published["version"]:
            raise ApiError(409, f"requested version {version} is not the published"
                                f" version {published['version']}")
        task_id = f"t-{secrets.token_hex(5)}"
        ts = now()
        with self.lock, self.db.tx() as conn:
            conn.execute(
                "INSERT INTO tasks (task_id, gate, publish_mode, state, version,"
                " body_json, body_sha256, created_at, updated_at)"
                " VALUES (?,?,?,'pending',?,?,?,?,?)",
                (task_id, gate, publish_mode, published["version"],
                 dumps(published), published["body_sha256"], ts, ts))
            for entry in self._entries():
                conn.execute(
                    "INSERT INTO task_entries (task_id, entry_id, state, nonce,"
                    " updated_at) VALUES (?,?,'pending',?,?)",
                    (task_id, entry["entry_id"], secrets.token_hex(16), ts))
            self._event(conn, "task_created",
                        f"{task_id} gate={gate} publish={publish_mode}"
                        f" pinned={published['body_sha256'][:12]}",
                        version=published["version"])
        threading.Thread(target=self._run, args=(task_id,),
                         name=f"task-{task_id}", daemon=True).start()
        return self.task(task_id)

    def _run(self, task_id: str) -> None:
        lock = self._task_lock(task_id)
        if not lock.acquire(blocking=False):
            return
        try:
            self._drive(task_id)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self._abort(task_id, f"orchestrator error: {type(exc).__name__}: {exc}")
        finally:
            lock.release()

    def _state(self, task_id: str, state: str, note: str | None = None) -> None:
        fields = {"state": state, "updated_at": now()}
        if note is not None:
            fields["note"] = note
        keys = ", ".join(f"{k}=?" for k in fields)
        with self.lock, self.db.tx() as conn:
            conn.execute(f"UPDATE tasks SET {keys} WHERE task_id=?",
                         (*fields.values(), task_id))

    def _entry_state(self, task_id: str, entry_id: str, state: str,
                     **extra) -> None:
        fields = {"state": state, "updated_at": now(), **extra}
        keys = ", ".join(f"{k}=?" for k in fields)
        with self.lock, self.db.tx() as conn:
            conn.execute(f"UPDATE task_entries SET {keys}"
                         " WHERE task_id=? AND entry_id=?",
                         (*fields.values(), task_id, entry_id))

    def _drive(self, task_id: str) -> None:
        task = self.task(task_id)
        entries = self._entries()
        version, pinned = task["version"], task["body_sha256"]
        nonces = {e["entry_id"]: e["nonce"] for e in task["entries"]}

        if task["gate"] == "barrier":
            self._state(task_id, "confirming",
                        "waiting for every entry to confirm the pinned body")
            ok, problems = self._confirm_all(task_id, entries, version, pinned)
            if not ok:
                self._abort(task_id, "barrier gate not met: " + "; ".join(problems))
                return

        self._state(task_id, "preparing")
        failures = self._prepare_all(task_id, entries, version, pinned, nonces)
        if failures:
            self._abort(task_id, "preparation failed: " + "; ".join(failures))
            return

        self._state(task_id, "verifying")
        failures = self._verify_all(task_id, entries)
        if failures:
            self._abort(task_id, "verification failed: " + "; ".join(failures))
            return

        if task["publish_mode"] == "auto":
            ok, why = self._commit(task_id, "auto")
            if not ok:
                self._abort(task_id, f"auto publish refused: {why}")
        else:
            self._state(task_id, "ready",
                        "all entries verified; rows staged and awaiting publish")
            with self.lock, self.db.tx() as conn:
                self._event(conn, "task_ready",
                            f"{task_id} verified; publication deferred",
                            version=version)

    # ------------------------------------------------------------ dispatch
    def _call(self, entry: dict, path: str, payload: dict) -> tuple[dict | None, str]:
        try:
            return wire.post_json(entry["url"] + path, payload, ua="sink",
                                  timeout=20.0), ""
        except (wire.WireError, ValueError) as exc:
            return None, str(exc)

    def _fan(self, entries, path, payload_for) -> dict[str, tuple[dict | None, str]]:
        results: dict[str, tuple[dict | None, str]] = {}
        with futures.ThreadPoolExecutor(max_workers=max(1, len(entries))) as pool:
            futs = {pool.submit(self._call, entry, path,
                                payload_for(entry)): entry for entry in entries}
            for fut in futures.as_completed(futs):
                entry = futs[fut]
                try:
                    results[entry["entry_id"]] = fut.result()
                except Exception as exc:  # noqa: BLE001
                    results[entry["entry_id"]] = (None, str(exc))
        return results

    def _confirm_all(self, task_id, entries, version, pinned):
        results = self._fan(entries, "/confirm", lambda e: {
            "task_id": task_id, "version": version, "body_sha256": pinned})
        problems = []
        for entry in entries:
            name = entry["entry_id"]
            report, error = results[name]
            if report is None:
                self._entry_state(task_id, name, "failed", error=error)
                problems.append(f"{name}: {error or 'no confirmation'}")
                continue
            evidence = report.get("config") or {}
            ok = (evidence.get("source") == "live"
                  and evidence.get("body_sha256") == pinned
                  and evidence.get("version") == version
                  and evidence.get("etag", "").strip('"') == pinned)
            self._entry_state(task_id, name,
                              "confirmed" if ok else "failed",
                              config_evidence=dumps(evidence),
                              error=None if ok else "stale/non-live confirmation")
            with self.lock, self.db.tx() as conn:
                self._event(conn,
                            "entry_confirmed" if ok else "entry_confirm_rejected",
                            f"{name} live={evidence.get('source')}"
                            f" body={str(evidence.get('body_sha256'))[:12]}",
                            entry_id=name, version=version)
            if not ok:
                problems.append(f"{name}: did not confirm the pinned body live")
        return not problems, problems

    def _prepare_all(self, task_id, entries, version, pinned, nonces):
        def payload(entry: dict) -> dict:
            name = entry["entry_id"]
            payload = {"task_id": task_id, "version": version,
                       "body_sha256": pinned, "nonce": nonces[name]}
            if entry["kind"] == "file":
                # For the file entry the proof of a real read is a per-task
                # nonce file placed next to the source; it is read in the same
                # call that reads the directory file itself.
                payload["challenge_path"] = self._write_challenge(
                    task_id, name, nonces[name])
            return payload

        results = self._fan(entries, "/prepare", payload)
        failures = []
        for entry in entries:
            name = entry["entry_id"]
            report, error = results[name]
            if report is None:
                self._entry_state(task_id, name, "failed", error=error)
                with self.lock, self.db.tx() as conn:
                    self._event(conn, "entry_prepare_failed", error or "",
                                entry_id=name, version=version)
                failures.append(f"{name}: {error or 'no report'}")
                continue
            problem = self._accept_report(task_id, entry, report, pinned, version)
            if problem:
                self._entry_state(task_id, name, "failed", error=problem,
                                  config_evidence=dumps(report.get("config")),
                                  source_evidence=dumps(report.get("source_evidence")))
                failures.append(f"{name}: {problem}")
                continue
            self._entry_state(
                task_id, name, "prepared",
                config_evidence=dumps(report.get("config")),
                source_evidence=dumps(report.get("source_evidence")),
                digest=report.get("digest"), token=report.get("token"),
                record_count=int(report.get("record_count") or 0), error=None)
            self._store_rows(task_id, name, report)
            with self.lock, self.db.tx() as conn:
                self._event(conn, "entry_prepared",
                            f"{name} rows={report.get('record_count')}"
                            f" digest={str(report.get('digest'))[:12]}",
                            entry_id=name, version=version)
        return failures

    def _write_challenge(self, task_id: str, entry_id: str, nonce: str) -> str:
        directory = self.run_dir / "work" / "challenges"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{task_id}-{entry_id}.nonce"
        path.write_text(nonce + "\n", encoding="utf-8")
        return str(path)

    def _accept_report(self, task_id: str, entry: dict, report: dict,
                       pinned: str, version: str) -> str | None:
        """Cheap structural checks; cryptographic/read checks happen in verify."""
        if report.get("task_id") != task_id:
            return "report belongs to another task"
        if report.get("entry_id") != entry["entry_id"]:
            return "report claims another entry"
        config = report.get("config") or {}
        if config.get("source") != "live":
            return "config was not read live"
        if config.get("body_sha256") != pinned:
            return "report used a different config body than the task pin"
        if config.get("version") != version:
            return "report used another config version"
        if config.get("etag", "").strip('"') != pinned:
            return "config ETag does not match the pinned body"
        rows = report.get("rows")
        if not isinstance(rows, list):
            return "report has no rows"
        if sha256_hex(canonical(rows)) != report.get("digest"):
            return "row digest does not match the rows sent"
        if len(rows) != int(report.get("record_count") or len(rows)):
            return "record count disagrees with rows sent"
        evidence = report.get("source_evidence") or {}
        if entry["kind"] != "file":
            nonce = None
            for row in self.task(task_id)["entries"]:
                if row["entry_id"] == entry["entry_id"]:
                    nonce = row["nonce"]
            urls = [p["url"] for p in evidence.get("pages") or []] or \
                evidence.get("urls") or []
            receipts = evidence.get("receipts") or {}
            echoed = evidence.get("echoed_nonces") or {}
            digests = {p["url"]: p.get("raw_sha256")
                       for p in evidence.get("pages") or []}
            if not urls:
                return "no source URLs in evidence"
            for url in urls:
                if echoed.get(url) != nonce:
                    return f"source nonce not echoed for {url}"
                receipt = receipts.get(url, "")
                digest = digests.get(url, evidence.get("url_digests", {}).get(url))
                if not receipt or not digest:
                    return f"missing source receipt for {url}"
                if not hmac.compare_digest(
                        receipt, self._receipt(nonce, url, digest, entry)):
                    return f"invalid source receipt for {url}"
        return None

    def _receipt(self, nonce: str, url: str, digest: str, entry: dict) -> str:
        path = urlsplit(url).path
        prefix = (entry.get("mock_prefix") or "").rstrip("/")
        if prefix and path.startswith(prefix + "/"):
            path = path[len(prefix):]
        material = f"{nonce}|{path}|{digest}".encode("utf-8")
        return hmac.new(self.source_key, material, hashlib.sha256).hexdigest()

    def _store_rows(self, task_id: str, entry_id: str, report: dict) -> None:
        config = report.get("config") or {}
        source_sha = sources.source_fingerprint(report.get("source_evidence") or {})
        with self.lock, self.db.tx() as conn:
            conn.execute("DELETE FROM records WHERE task_id=? AND entry_id=?",
                         (task_id, entry_id))
            for row in report["rows"]:
                conn.execute(
                    "INSERT INTO records (task_id, entry_id, item_id, title,"
                    " price_cents, evidence) VALUES (?,?,?,?,?,?)",
                    (task_id, entry_id, row["id"], row["title"],
                     int(row["price_cents"]), dumps({
                         "config_version": config.get("version"),
                         "config_body_sha256": config.get("body_sha256"),
                         "config_etag": config.get("etag"),
                         "source_sha256": source_sha,
                         "entry_digest": report.get("digest"),
                     })))

    # ----------------------------------------------------------- verifying
    def _verify_all(self, task_id, entries) -> list[str]:
        failures = []
        for entry in entries:
            problem = self._verify_entry(task_id, entry)
            if problem:
                failures.append(f"{entry['entry_id']}: {problem}")
        return failures

    def _verify_entry(self, task_id: str, entry: dict) -> str | None:
        """Re-read the source independently and compare with the claim."""
        task = self.task(task_id)
        row = self.db.one(
            "SELECT * FROM task_entries WHERE task_id=? AND entry_id=?",
            (task_id, entry["entry_id"]))
        config = loads(row["config_evidence"]) or {}
        claimed_source = loads(row["source_evidence"]) or {}
        problems: list[str] = []
        body = task["body"]

        if config.get("source") != "live":
            problems.append("config confirmation was not a live read")
        if config.get("body_sha256") != task["body_sha256"]:
            problems.append("config body hash differs from the task pin")
        if config.get("version") != task["version"]:
            problems.append("config version differs from the task pin")
        if config.get("etag", "").strip('"') != task["body_sha256"]:
            problems.append("config ETag differs from the task pin")
        if config.get("fingerprint") != body["fingerprint"]:
            problems.append("config mapping fingerprint differs from the pin")
        if config.get("scope_hash") != body["scope_hash"]:
            problems.append("config scope hash differs from the pin")

        stored = self.db.q(
            "SELECT item_id, title, price_cents FROM records"
            " WHERE task_id=? AND entry_id=? ORDER BY item_id",
            (task_id, entry["entry_id"]))
        stored_rows = [{"id": r["item_id"], "title": r["title"],
                        "price_cents": r["price_cents"]} for r in stored]
        if sources.rows_digest(stored_rows) != row["digest"]:
            problems.append("stored rows do not match the reported digest")
        if len(stored_rows) != int(row["record_count"]):
            problems.append("stored row count differs from the report")

        source = dict(entry["source"])
        if entry["kind"] == "page" and not source.get("ids"):
            source["ids"] = list(body["scope"].get("ids") or [])
        nonce = "verify-" + secrets.token_hex(8)
        challenge = None
        if entry["kind"] == "file":
            challenge = self._write_challenge(task_id, entry["entry_id"] + "-verify",
                                              nonce)
        try:
            recheck = sources.collect(entry["kind"], source, self.mock_url, body,
                                      nonce=nonce, challenge_path=challenge)
        except Exception as exc:  # noqa: BLE001
            problems.append(f"independent re-read failed: {exc}")
            recheck = None

        if recheck is not None:
            if sources.source_fingerprint(recheck["source_evidence"]) != \
                    sources.source_fingerprint(claimed_source):
                problems.append("source bytes differ from what the entry read")
            if recheck["digest"] != row["digest"]:
                problems.append("re-projected digest differs from the entry's")
            if len(recheck["rows"]) != len(stored_rows):
                problems.append("re-read row count differs from stored rows")
            if entry["kind"] == "file":
                if claimed_source.get("challenge") != row["nonce"]:
                    problems.append("file entry did not read the task challenge")

        if problems:
            self._entry_state(task_id, entry["entry_id"], "failed",
                              error="; ".join(problems),
                              verification=dumps({"ok": False,
                                                  "problems": problems,
                                                  "checked_at": now()}))
            with self.lock, self.db.tx() as conn:
                self._event(conn, "entry_verify_failed", "; ".join(problems),
                            entry_id=entry["entry_id"],
                            version=task["version"])
            return "; ".join(problems)

        verification = {
            "ok": True, "checked_at": now(), "independent_read": True,
            "re_read_nonce": nonce,
            "re_read_source_sha256":
                sources.source_fingerprint(recheck["source_evidence"]),
            "re_read_digest": recheck["digest"],
            "config_body_sha256": config["body_sha256"],
            "config_etag": config["etag"],
            "mapping_fingerprint": config["fingerprint"],
            "scope_hash": config["scope_hash"],
        }
        self._entry_state(task_id, entry["entry_id"], "verified",
                          verification=dumps(verification), error=None)
        with self.lock, self.db.tx() as conn:
            self._event(conn, "entry_verified",
                        f"{entry['entry_id']} re-read digest={recheck['digest'][:12]}",
                        entry_id=entry["entry_id"], version=task["version"])
        return None

    # ------------------------------------------------------------ commit
    def _commit(self, task_id: str, reason: str) -> tuple[bool, str]:
        """Re-verify everything, then publish all rows in one transaction."""
        task = self.task(task_id)
        try:
            self._guard_pinned(task["body_sha256"])
        except ApiError as exc:
            return False, str(exc)
        for entry in self._entries():
            problem = self._verify_entry(task_id, entry)
            if problem:
                return False, f"{entry['entry_id']}: {problem}"
        with self.lock, self.db.tx() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?",
                               (task_id,)).fetchone()
            if row is None or row["state"] not in OPEN_STATES:
                return False, f"task is {row['state'] if row else 'missing'}"
            total = conn.execute(
                "SELECT COUNT(*) FROM task_entries WHERE task_id=?",
                (task_id,)).fetchone()[0]
            verified = conn.execute(
                "SELECT COUNT(*) FROM task_entries WHERE task_id=?"
                " AND state='verified'", (task_id,)).fetchone()[0]
            if total == 0 or verified != total:
                return False, "not every entry verified"
            ts = now()
            conn.execute("UPDATE records SET published=1 WHERE task_id=?",
                         (task_id,))
            conn.execute(
                "UPDATE tasks SET state='committed', updated_at=?, finished_at=?,"
                " note=? WHERE task_id=?",
                (ts, ts, f"published ({reason})", task_id))
            self._event(conn, "task_committed",
                        f"{task_id} rows published ({reason})",
                        version=task["version"])
        # Staged directories are no longer needed once rows are published.
        for entry in self._entries():
            try:
                wire.post_json(entry["url"] + "/release", {"task_id": task_id},
                               ua="sink", timeout=5.0)
            except (wire.WireError, ValueError):
                continue
        return True, "ok"

    def commit(self, task_id: str) -> dict:
        task = self.task(task_id)
        if task["state"] != "ready":
            raise ApiError(409, f"task is {task['state']}, not ready")
        with self._task_lock(task_id):
            ok, why = self._commit(task_id, "manual")
            if not ok:
                self._abort(task_id, f"manual publish refused: {why}")
                raise ApiError(409, f"publish refused: {why}")
        return self.task(task_id)

    def abort(self, task_id: str, reason: str) -> dict:
        task = self.task(task_id)
        if task["state"] not in OPEN_STATES:
            raise ApiError(409, f"task is already {task['state']}")
        with self._task_lock(task_id):
            self._abort(task_id, reason or "manual abort")
        return self.task(task_id)

    def _abort(self, task_id: str, reason: str) -> None:
        with self.lock, self.db.tx() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE task_id=?",
                                (task_id,)).fetchone()
            if task is None or task["state"] not in OPEN_STATES:
                return
            ts = now()
            purged = conn.execute(
                "DELETE FROM records WHERE task_id=? AND published=0",
                (task_id,)).rowcount
            conn.execute(
                "UPDATE tasks SET state='aborted', updated_at=?, finished_at=?,"
                " note=? WHERE task_id=?", (ts, ts, reason, task_id))
            conn.execute(
                "UPDATE task_entries SET state='rolled_back', updated_at=?,"
                " error=COALESCE(error, ?) WHERE task_id=?"
                " AND state != 'failed'", (ts, reason, task_id))
            for entry in self._entries():
                self._queue_rollback(conn, task_id, entry["entry_id"], reason)
            self._event(conn, "task_aborted",
                        f"{task_id} aborted: {reason}; purged_staged_rows={purged}",
                        version=task["version"])

        self._withdraw_prepared(task_id, reason)

    def _withdraw_prepared(self, task_id: str, reason: str) -> dict[str, bool]:
        """Ask every registered entry to delete this task's staged directory.

        Returns ``{entry_id: reached}``.  Unreached entries keep their
        rollback-queue row, which is flushed on their next heartbeat (or is
        moot after a restart, since a restart purges staged state itself).
        """
        reached: dict[str, bool] = {}
        for entry in self._entries():
            try:
                wire.post_json(entry["url"] + "/rollback",
                               {"task_id": task_id, "reason": reason},
                               ua="sink", timeout=5.0)
            except (wire.WireError, ValueError):
                reached[entry["entry_id"]] = False
                continue
            reached[entry["entry_id"]] = True
            with self.lock, self.db.tx() as conn:
                conn.execute(
                    "UPDATE rollback_queue SET done_at=?"
                    " WHERE task_id=? AND entry_id=? AND done_at IS NULL",
                    (now(), task_id, entry["entry_id"]))
                self._event(conn, "prepared_state_withdrawn",
                            f"{task_id} staged state removed from"
                            f" {entry['entry_id']}",
                            entry_id=entry["entry_id"])
        return reached

    # --------------------------------------------------------------- views
    def task(self, task_id: str) -> dict:
        task = self.db.one("SELECT * FROM tasks WHERE task_id=?", (task_id,))
        if task is None:
            raise ApiError(404, f"unknown task {task_id}")
        out = dict(task)
        out["body"] = loads(out.pop("body_json"))
        out["entries"] = []
        for row in self.db.q(
                "SELECT * FROM task_entries WHERE task_id=? ORDER BY entry_id",
                (task_id,)):
            e = dict(row)
            for key in ("config_evidence", "source_evidence", "verification"):
                e[key] = loads(e[key])
            e["stored_rows"] = self.db.scalar(
                "SELECT COUNT(*) FROM records WHERE task_id=? AND entry_id=?",
                (task_id, e["entry_id"]))
            e["published_rows"] = self.db.scalar(
                "SELECT COUNT(*) FROM records WHERE task_id=? AND entry_id=?"
                " AND published=1", (task_id, e["entry_id"]))
            out["entries"].append(e)
        out["stored_rows"] = self.db.scalar(
            "SELECT COUNT(*) FROM records WHERE task_id=?", (task_id,))
        out["published_rows"] = self.db.scalar(
            "SELECT COUNT(*) FROM records WHERE task_id=? AND published=1",
            (task_id,))
        out["rollbacks_pending"] = self.db.scalar(
            "SELECT COUNT(*) FROM rollback_queue WHERE task_id=? AND done_at IS NULL",
            (task_id,))
        return out


SINK: Sink


@route("GET", r"/health")
def health(handler, body):
    return {"ok": True, "entries": len(SINK._entries())}


@route("GET", r"/entries")
def list_entries(handler, body):
    return {"entries": SINK.entries_view()}


@route("POST", r"/entries")
def register_entries(handler, body):
    """Register or update entries (idempotent).  Used by provisioning."""
    entries = body.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ApiError(400, "body needs a non-empty entries list")
    for entry in entries:
        if not isinstance(entry, dict) or not all(
                k in entry for k in ("entry_id", "kind", "collector_url",
                                     "source")):
            raise ApiError(400, "each entry needs entry_id, kind,"
                                " collector_url and source")
    return {"entries": SINK.register_entries({"entries": entries})}


@route("POST", r"/entries/(?P<entry_id>[^/]+)/observe")
def observe(handler, entry_id, body):
    config = body.get("config")
    if not isinstance(config, dict):
        raise ApiError(400, "observe requires a config evidence object")
    return SINK.observe(entry_id, config)


@route("POST", r"/entries/(?P<entry_id>[^/]+)/heartbeat")
def heartbeat(handler, entry_id, body):
    return SINK.heartbeat(entry_id, body)


@route("POST", r"/tasks")
def create_task(handler, body):
    extra = set(body) - {"gate", "publish", "version"}
    if extra:
        raise ApiError(400, f"unknown fields: {sorted(extra)}")
    return 201, SINK.create_task(body.get("gate", "barrier"),
                                 body.get("publish", "auto"),
                                 body.get("version"))


@route("GET", r"/tasks")
def list_tasks(handler, body):
    return {"tasks": SINK.db.q(
        "SELECT task_id, gate, publish_mode, state, version, created_at,"
        " updated_at, note FROM tasks ORDER BY created_at DESC, task_id DESC"
        " LIMIT 50")}


@route("GET", r"/tasks/(?P<task_id>[^/]+)")
def get_task(handler, task_id, body):
    return SINK.task(task_id)


@route("POST", r"/tasks/(?P<task_id>[^/]+)/commit")
def commit_task(handler, task_id, body):
    return SINK.commit(task_id)


@route("POST", r"/tasks/(?P<task_id>[^/]+)/abort")
def abort_task(handler, task_id, body):
    return SINK.abort(task_id, body.get("reason", "manual abort"))


@route("GET", r"/records")
def records(handler, body):
    task_id = (handler.query.get("task") or [None])[0]
    if task_id:
        rows = SINK.db.q("SELECT * FROM records WHERE task_id=?"
                         " ORDER BY entry_id, item_id", (task_id,))
    else:
        rows = SINK.db.q("SELECT * FROM records WHERE published=1"
                         " ORDER BY task_id, entry_id, item_id")
    return {"records": rows}


@route("GET", r"/events")
def events(handler, body):
    rows = SINK.db.q("SELECT * FROM events ORDER BY id DESC LIMIT 500")
    return {"events": list(reversed(rows))}


@route("GET", r"/published")
def published(handler, body):
    body_cfg = SINK.read_published_config()
    return {"body": body_cfg}


def make_server(host: str, port: int, db_path: str, run_dir: str,
                config_url: str, mock_url: str,
                verbose: bool = False) -> ThreadingHTTPServer:
    global SINK
    JsonHandler.routes = collect_routes(globals())
    SINK = Sink(db_path, run_dir, config_url, mock_url, verbose)
    srv = ThreadingHTTPServer((host, port), JsonHandler)
    srv.daemon_threads = True
    srv.verbose = verbose
    return srv


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--config-url", required=True)
    ap.add_argument("--mock-url", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--entries", help="JSON file describing the entries")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)
    srv = make_server(args.host, args.port, args.db, args.run_dir,
                      args.config_url, args.mock_url, args.verbose)
    if args.entries:
        spec = json.loads(Path(args.entries).read_text(encoding="utf-8"))
        SINK.register_entries(spec)
    print(json.dumps({"url": f"http://{args.host}:{srv.server_address[1]}",
                      "db": str(Path(args.db).resolve())}), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
