"""Wire format shared by config service, collectors, sink and web console.

A config body always carries the *pair*: mapping + scope, each hash, and a
seal over the whole core.  There is deliberately no endpoint or message that
carries a mapping without a scope (or vice versa).
"""
from __future__ import annotations

import json
from typing import Any

from .util import canonical, mapping_fingerprint, pair_scope_hash, sha256_hex

REQUIRED_MAPPING_FIELDS = ("id", "title", "price")
OPTIONAL_MAPPING_FIELDS = ("stock",)

_BODY_CORE_FIELDS = ("version", "mapping", "scope", "fingerprint",
                     "scope_hash", "created_at")
_BODY_FIELDS = _BODY_CORE_FIELDS + ("body_sha256",)


class ConfigError(ValueError):
    pass


def validate_mapping(mapping: Any) -> dict:
    if not isinstance(mapping, dict):
        raise ConfigError("mapping must be a JSON object")
    keys = set(mapping)
    unknown = keys - (set(REQUIRED_MAPPING_FIELDS) | set(OPTIONAL_MAPPING_FIELDS))
    if unknown:
        raise ConfigError(f"unsupported mapping fields: {sorted(unknown)}")
    missing = set(REQUIRED_MAPPING_FIELDS) - keys
    if missing:
        raise ConfigError(f"mapping missing fields: {sorted(missing)}")
    for key, value in mapping.items():
        if not isinstance(value, str) or not value:
            raise ConfigError(f"mapping[{key!r}] must be a non-empty string")
    if len(set(mapping.values())) != len(mapping):
        raise ConfigError("mapping source fields must be distinct")
    return mapping


def validate_scope(scope: Any) -> dict:
    if not isinstance(scope, dict):
        raise ConfigError("scope must be a JSON object")
    unknown = set(scope) - {"ids", "price_max_cents", "exclude_zero_stock"}
    if unknown:
        raise ConfigError(f"unsupported scope fields: {sorted(unknown)}")
    ids = scope.get("ids", [])
    if not isinstance(ids, list) or any(not isinstance(i, str) for i in ids):
        raise ConfigError("scope.ids must be a list of strings")
    if "price_max_cents" in scope:
        value = scope["price_max_cents"]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ConfigError("scope.price_max_cents must be a non-negative integer")
    if "exclude_zero_stock" in scope and not isinstance(scope["exclude_zero_stock"], bool):
        raise ConfigError("scope.exclude_zero_stock must be a boolean")
    return scope


def build_body(version: str, mapping: dict, scope: dict, created_at: str) -> dict:
    validate_mapping(mapping)
    validate_scope(scope)
    core = {
        "version": version,
        "mapping": mapping,
        "scope": scope,
        "fingerprint": mapping_fingerprint(mapping),
        "scope_hash": pair_scope_hash(mapping, scope),
        "created_at": created_at,
    }
    body = dict(core)
    body["body_sha256"] = sha256_hex(canonical(core))
    validate_body(body)
    return body


def validate_body(body: Any) -> dict:
    """Reject any body where the pair, hashes and seal do not agree."""
    if not isinstance(body, dict):
        raise ConfigError("config body must be an object")
    unknown = set(body) - set(_BODY_FIELDS)
    if unknown:
        raise ConfigError(f"unsupported config body fields: {sorted(unknown)}")
    missing = set(_BODY_FIELDS) - set(body)
    if missing:
        raise ConfigError(f"config body missing fields: {sorted(missing)}")
    validate_mapping(body["mapping"])
    validate_scope(body["scope"])
    if body["fingerprint"] != mapping_fingerprint(body["mapping"]):
        raise ConfigError("fingerprint does not match mapping")
    if body["scope_hash"] != pair_scope_hash(body["mapping"], body["scope"]):
        raise ConfigError("scope_hash does not cover this exact mapping+scope pair")
    core = {k: body[k] for k in _BODY_CORE_FIELDS}
    if body["body_sha256"] != sha256_hex(canonical(core)):
        raise ConfigError("body_sha256 does not seal this body")
    return body


def body_from_cache(cached: dict) -> dict:
    """A collector's local cache is the same wire format; validate before use."""
    return validate_body(cached)


def body_digest(body: dict) -> str:
    return body["body_sha256"]


def rows_digest(rows: list[dict]) -> str:
    return sha256_hex(canonical(rows))


def dumps(obj) -> str:
    return json.dumps(obj, sort_keys=True, indent=2)
