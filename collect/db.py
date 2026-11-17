"""SQLite connection helpers with one connection per thread.

Both servers are threaded.  A shared ``sqlite3.Connection`` cannot be used
from several threads, so each thread lazily opens its own connection (WAL
mode, 30 s busy timeout).  Logical read-modify-write sequences are serialised
with a process-wide ``RLock`` in the owning store; SQLite's ``BEGIN IMMEDIATE``
serialises writers across processes too.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path: str | Path, schema: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(schema)
    finally:
        conn.close()


class DB:
    def __init__(self, path: str | Path, schema: str):
        self.path = str(path)
        init_db(self.path, schema)
        self._local = threading.local()

    @property
    def conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = connect(self.path)
            self._local.conn = conn
        return conn

    def q(self, sql: str, params=()) -> list[dict]:
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def one(self, sql: str, params=()):
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row is not None else None

    def scalar(self, sql: str, params=()):
        row = self.conn.execute(sql, params).fetchone()
        return row[0] if row is not None else None

    def execute(self, sql: str, params=()) -> int:
        with self.tx() as conn:
            return conn.execute(sql, params).rowcount

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")


def loads(value):
    if value is None:
        return None
    import json
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def dumps(obj) -> str:
    import json
    return json.dumps(obj, sort_keys=True)
