"""Read one source through a validated config pair (mapping + scope).

The *same* projection code runs in every collector and in the sink's verifier.
The sink never trusts a collector's rows: it re-reads the source itself and
compares the raw-byte digest, the projected digest, and the mock's per-read
receipt.  Receipts are HMACs over ``nonce|path|sha256(body)`` made with a key
the collectors never see, so a collector can only produce a valid receipt by
actually fetching that URL with the task's nonce.  Cached bytes carry an old
nonce and are rejected.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from html.parser import HTMLParser
from http.client import HTTPException
from typing import Any
from urllib.parse import urlsplit

from .protocol import rows_digest
from .util import canonical, sha256_hex

SOURCE_KINDS = ("api", "file", "page")


class SourceError(RuntimeError):
    pass


# ------------------------------------------------------------------ fetching

def http_get(url: str, *, timeout: float = 10.0,
             nonce: str | None = None) -> tuple[int, bytes, dict]:
    headers = {"User-Agent": "estate-collector/1.0", "Cache-Control": "no-store"}
    if nonce:
        headers["X-Read-Nonce"] = nonce
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)
    except (urllib.error.URLError, TimeoutError, OSError, HTTPException) as exc:
        raise SourceError(f"GET {url} failed: {exc}") from exc


class _ProductPageParser(HTMLParser):
    """Strict parser for the mock product page's data block.

    Pages expose aliases (trade sku vs catalogue code, label vs title, pennies
    vs pounds) so either config preset can genuinely re-read the same bytes.
    """

    TEXT_FIELDS = ("label", "title", "price_cents", "price", "in_stock", "stock")
    ATTR_FIELDS = ("sku", "code", "id")

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.item: dict[str, Any] | None = None
        self._capture: str | None = None
        self._buf: list[str] = []

    def handle_starttag(self, tag, attrs):
        attrs = {k: (v or "") for k, v in attrs}
        cls = attrs.get("class", "").split()
        if tag == "div" and "product" in cls:
            self.item = {}
            for name in self.ATTR_FIELDS:
                value = attrs.get(name, attrs.get(f"data-{name}", ""))
                if value:
                    self.item[name] = value
            return
        if self.item is not None and tag in ("h1", "span"):
            for name in cls:
                if name in self.TEXT_FIELDS:
                    self._capture = name
                    self._buf = []
                    return

    def handle_data(self, data):
        if self._capture is not None:
            self._buf.append(data)

    def handle_endtag(self, tag):
        if self._capture is not None and tag in ("h1", "span"):
            text = "".join(self._buf).strip()
            if self.item is not None:
                self.item[self._capture] = text
            self._capture = None
            self._buf = []


def parse_product_page(raw: bytes, expected_id: str) -> dict:
    parser = _ProductPageParser()
    parser.feed(raw.decode("utf-8", "replace"))
    item = parser.item
    if item is None:
        raise SourceError("page has no product block")
    ids = {item.get(k) for k in ("sku", "code", "id") if item.get(k)}
    if expected_id and expected_id not in ids:
        raise SourceError(f"page ids {sorted(ids)} do not include {expected_id}")
    if not ids:
        raise SourceError("page product block has no id")
    return item


def _need(source: dict, key: str) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not value:
        raise SourceError(f"source spec missing {key!r}")
    return value


def _read_file(path: str) -> tuple[bytes, int, int]:
    import os
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        st = os.stat(path)
    except OSError as exc:
        raise SourceError(f"cannot read directory file {path}: {exc}") from exc
    return raw, st.st_size, st.st_mtime_ns


def read_source(kind: str, source: dict, origin: str, *, entry_id: str | None = None,
                timeout: float = 10.0, nonce: str | None = None,
                challenge_path: str | None = None) -> dict:
    """Return raw items plus byte/receipt evidence of what was actually read."""
    if kind not in SOURCE_KINDS:
        raise SourceError(f"unknown source kind {kind!r}")

    if kind == "api":
        url = origin.rstrip("/") + _need(source, "path")
        status, raw, headers = http_get(url, timeout=timeout, nonce=nonce)
        if status != 200:
            raise SourceError(f"api returned HTTP {status}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise SourceError(f"api payload is not JSON: {exc}") from exc
        items = payload.get("items")
        if not isinstance(items, list):
            raise SourceError("api payload has no items list")
        digest = sha256_hex(raw)
        return {"items": items, "source_evidence": {
            "kind": "api", "urls": [url], "http_status": status,
            "raw_sha256": digest, "raw_bytes": len(raw),
            "content_type": headers.get("Content-Type", ""),
            "url_digests": {url: digest},
            "receipts": {url: headers.get("X-Source-Receipt", "")},
            "echoed_nonces": {url: headers.get("X-Echo-Nonce", "")},
        }}

    if kind == "file":
        from pathlib import Path
        path = Path(_need(source, "path"))
        raw, size, mtime_ns = _read_file(str(path))
        try:
            payload = json.loads(raw.decode("utf-8"))
        except ValueError as exc:
            raise SourceError(f"file payload is not JSON: {exc}") from exc
        items = payload.get("items")
        if not isinstance(items, list):
            raise SourceError("file payload has no items list")
        challenges = payload.get("_read_challenges")
        challenge = (challenges or {}).get(entry_id) if entry_id else None
        if challenge_path:
            # The sink writes a fresh per-task nonce beside the file source.
            # Reading it is the file entry's proof that it performed a real
            # read for THIS task rather than replaying an earlier snapshot.
            try:
                with open(challenge_path, "r", encoding="utf-8") as fh:
                    challenge = fh.read().strip()
            except OSError as exc:
                raise SourceError(f"cannot read challenge {challenge_path}: {exc}") from exc
        return {"items": items, "source_evidence": {
            "kind": "file", "paths": [str(path.resolve())],
            "raw_sha256": sha256_hex(raw), "raw_bytes": len(raw),
            "size": size, "mtime_ns": mtime_ns,
            "entry_id": entry_id, "challenge": challenge,
            "challenge_path": challenge_path,
            "all_challenges": challenges,
        }}

    # page
    prefix = _need(source, "path")
    ids = source.get("ids")
    if not isinstance(ids, list) or not ids or any(not isinstance(i, str) for i in ids):
        raise SourceError("page source needs a non-empty ids list")
    base = origin.rstrip("/") + prefix
    if not base.endswith("/"):
        base += "/"
    items, pages = [], []
    receipts: dict[str, str] = {}
    echoed: dict[str, str] = {}
    for item_id in ids:
        url = base + item_id + ".html"
        status, raw, headers = http_get(url, timeout=timeout, nonce=nonce)
        if status != 200:
            raise SourceError(f"page {url} returned HTTP {status}")
        digest = sha256_hex(raw)
        receipts[url] = headers.get("X-Source-Receipt", "")
        echoed[url] = headers.get("X-Echo-Nonce", "")
        pages.append({"url": url, "http_status": status,
                      "raw_sha256": digest, "raw_bytes": len(raw)})
        items.append(parse_product_page(raw, item_id))
    return {"items": items, "source_evidence": {
        "kind": "page", "urls": [p["url"] for p in pages], "pages": pages,
        "receipts": receipts, "echoed_nonces": echoed,
        # Aggregate over page bytes only (not URLs): the entry reads through
        # its private /e/<entry>/ prefix while the sink re-reads directly, so
        # URLs legitimately differ while the bytes must not.
        "raw_sha256": sha256_hex(canonical([p["raw_sha256"] for p in pages])),
        "raw_bytes": sum(p["raw_bytes"] for p in pages),
    }}


# ---------------------------------------------------------------- projection

def project(items: list[dict], mapping: dict, scope: dict) -> list[dict]:
    """Apply mapping + range; deterministic ordering by id."""
    id_field, title_field, price_field = (mapping["id"], mapping["title"],
                                          mapping["price"])
    stock_field = mapping.get("stock")
    wanted = set(scope.get("ids") or [])
    price_max = scope.get("price_max_cents")
    exclude_zero = bool(scope.get("exclude_zero_stock"))
    rows, seen = [], set()
    for item in items:
        if not isinstance(item, dict):
            raise SourceError("source item is not an object")
        try:
            item_id = str(item[id_field])
            title = str(item[title_field])
            price = int(item[price_field])
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceError(f"item does not match mapping: {exc!r}") from exc
        if item_id in seen:
            raise SourceError(f"duplicate id {item_id!r} in source")
        seen.add(item_id)
        if wanted and item_id not in wanted:
            continue
        if price_max is not None and price > price_max:
            continue
        if exclude_zero and stock_field:
            try:
                stock = int(item[stock_field])
            except (KeyError, TypeError, ValueError) as exc:
                raise SourceError(f"item stock does not match mapping: {exc!r}") from exc
            if stock == 0:
                continue
        rows.append({"id": item_id, "title": title, "price_cents": price})
    rows.sort(key=lambda r: r["id"])
    return rows


def collect(kind: str, source: dict, origin: str, body: dict, *,
            entry_id: str | None = None, timeout: float = 10.0,
            nonce: str | None = None, challenge_path: str | None = None) -> dict:
    """Read + project under one already-validated config body."""
    read = read_source(kind, source, origin, entry_id=entry_id,
                       timeout=timeout, nonce=nonce,
                       challenge_path=challenge_path)
    rows = project(read["items"], body["mapping"], body["scope"])
    return {
        "rows": rows,
        "digest": rows_digest(rows),
        "source_evidence": read["source_evidence"],
    }


def source_fingerprint(evidence: dict) -> str:
    """The part of source evidence that must agree between two real reads.

    URLs are excluded on purpose: an entry reads through its own private
    prefix (``/e/<entry>/...``) while the sink re-reads directly, so the same
    bytes live at different URLs.  The ordered body digests must still match.
    """
    kind = evidence.get("kind")
    if kind in ("api", "file"):
        return str(evidence.get("raw_sha256"))
    if kind == "page":
        pages = evidence.get("pages") or []
        return sha256_hex(canonical([p.get("raw_sha256") for p in pages]))
    return sha256_hex(canonical(evidence))


def url_path(url: str) -> str:
    return urlsplit(url).path
