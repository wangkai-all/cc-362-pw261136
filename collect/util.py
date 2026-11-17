"""Shared helpers.  No hidden state, no process-local caching of config."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def mapping_fingerprint(mapping: dict) -> str:
    return sha256_hex(canonical(mapping))


def pair_scope_hash(mapping: dict, scope: dict) -> str:
    """ONE hash over mapping AND scope together.

    Collectors report this value after loading.  Because the pair is hashed
    jointly, an entry cannot claim the new mapping while holding the old range:
    the sink recomputes the hash from the published pair and any mix fails.
    """
    return sha256_hex(canonical({"mapping": mapping, "scope": scope}))


def atomic_write(path: str | Path, data: bytes) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def write_json_atomic(path: str | Path, obj) -> None:
    atomic_write(path, (json.dumps(obj, indent=2, sort_keys=True) + "\n").encode("utf-8"))


def read_json(path: str | Path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
