"""Pure unit tests for the protocol layer (no processes)."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from collect.presets import PRESETS, expected_rows  # noqa: E402
from collect.protocol import (ConfigError, build_body, rows_digest,  # noqa: E402
                              validate_body, validate_mapping, validate_scope)
from collect.sources import SourceError, project  # noqa: E402
from collect.util import pair_scope_hash  # noqa: E402


class TestBodyPairing(unittest.TestCase):
    def test_body_seals_pair(self):
        body = build_body("v1", PRESETS["v1"]["mapping"],
                          PRESETS["v1"]["scope"], "2026-11-17T00:00:00Z")
        validate_body(body)
        self.assertEqual(body["scope_hash"], pair_scope_hash(
            body["mapping"], body["scope"]))

    def test_tampered_mapping_is_rejected(self):
        body = build_body("v1", PRESETS["v1"]["mapping"],
                          PRESETS["v1"]["scope"], "2026-11-17T00:00:00Z")
        body["mapping"] = dict(body["mapping"], title="title")
        with self.assertRaises(ConfigError):
            validate_body(body)

    def test_swapped_scope_is_rejected(self):
        body = build_body("v1", PRESETS["v1"]["mapping"],
                          PRESETS["v1"]["scope"], "2026-11-17T00:00:00Z")
        body["scope"] = PRESETS["v2"]["scope"]
        with self.assertRaises(ConfigError):
            validate_body(body)

    def test_mixed_pair_fails_its_joint_hash(self):
        mapping = PRESETS["v2"]["mapping"]
        scope = PRESETS["v1"]["scope"]
        body = build_body("mixed", mapping, scope, "2026-11-17T00:00:00Z")
        # It is internally consistent, but its scope hash cannot equal either
        # real preset's hash, so no task pinned to a preset would accept it.
        validate_body(body)
        self.assertNotEqual(body["scope_hash"],
                            pair_scope_hash(PRESETS["v1"]["mapping"],
                                            PRESETS["v1"]["scope"]))
        self.assertNotEqual(body["scope_hash"],
                            pair_scope_hash(PRESETS["v2"]["mapping"],
                                            PRESETS["v2"]["scope"]))

    def test_scope_validation(self):
        for bad in ({"ids": "A-100"}, {"price_max_cents": -1},
                    {"exclude_zero_stock": "yes"}, {"unknown": 1}):
            with self.assertRaises(ConfigError, msg=bad):
                validate_scope(bad)

    def test_mapping_validation(self):
        for bad in ({"id": "sku", "title": "label"},
                    {"id": "sku", "title": "label", "price": "price_cents",
                     "colour": "colour"},
                    {"id": "sku", "title": "label", "price": "price_cents",
                     "stock": ""}):
            with self.assertRaises(ConfigError, msg=bad):
                validate_mapping(bad)


class TestProjection(unittest.TestCase):
    def test_projection_is_deterministic_and_sorted(self):
        from collect.presets import ITEMS
        rows = project(list(reversed(ITEMS)), PRESETS["v2"]["mapping"],
                       PRESETS["v2"]["scope"])
        self.assertEqual(rows, expected_rows("v2"))
        self.assertEqual([r["id"] for r in rows], sorted(r["id"] for r in rows))

    def test_unmapped_item_raises(self):
        with self.assertRaises(SourceError):
            project([{"totally": "different"}], PRESETS["v2"]["mapping"],
                    PRESETS["v2"]["scope"])

    def test_duplicate_ids_raise(self):
        items = [{"sku": "A-100", "label": "x", "price_cents": 1, "in_stock": 1},
                 {"sku": "A-100", "label": "y", "price_cents": 2, "in_stock": 1}]
        with self.assertRaises(SourceError):
            project(items, PRESETS["v1"]["mapping"], PRESETS["v1"]["scope"])

    def test_range_is_applied(self):
        rows = project([{"sku": "A-100", "label": "x", "price_cents": 999,
                         "in_stock": 1},
                        {"sku": "A-101", "label": "y", "price_cents": 1001,
                         "in_stock": 1}], PRESETS["v1"]["mapping"],
                       PRESETS["v1"]["scope"])
        self.assertEqual(rows, [{"id": "A-100", "title": "x",
                                 "price_cents": 999}])

    def test_digest_depends_on_content(self):
        a = [{"id": "A", "title": "t", "price_cents": 1}]
        b = [{"id": "A", "title": "t", "price_cents": 2}]
        self.assertNotEqual(rows_digest(a), rows_digest(b))


if __name__ == "__main__":
    unittest.main(verbosity=2)
