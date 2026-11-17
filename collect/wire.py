"""Tiny HTTP client used for control-plane calls (JSON) and config reads."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any


class WireError(RuntimeError):
    def __init__(self, message: str, status: int | None = None,
                 payload: Any = None, headers: dict | None = None):
        super().__init__(message)
        self.status = status
        self.payload = payload
        self.headers = headers or {}


def request(method: str, url: str, body: Any = None, *, ua: str = "estate-client",
            timeout: float = 10.0) -> tuple[int, bytes, dict]:
    data = None
    headers = {"User-Agent": ua, "Cache-Control": "no-store",
               "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise WireError(f"{method} {url} failed: {exc}") from exc


def get_raw(url: str, *, ua: str, timeout: float = 10.0) -> tuple[int, bytes, dict]:
    return request("GET", url, None, ua=ua, timeout=timeout)


def decode_json(raw: bytes) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise WireError(f"response is not JSON: {exc}") from exc


def get_json(url: str, *, ua: str, timeout: float = 10.0) -> Any:
    status, raw, headers = request("GET", url, None, ua=ua, timeout=timeout)
    payload = decode_json(raw) if raw else None
    if status >= 400:
        raise WireError(f"GET {url} -> HTTP {status}",
                        status=status, payload=payload, headers=headers)
    return payload


def post_json(url: str, body: Any, *, ua: str, timeout: float = 10.0) -> Any:
    status, raw, headers = request("POST", url, body, ua=ua, timeout=timeout)
    payload = decode_json(raw) if raw else None
    if status >= 400:
        message = payload.get("error") if isinstance(payload, dict) else None
        raise WireError(message or f"POST {url} -> HTTP {status}",
                        status=status, payload=payload, headers=headers)
    return payload
