"""Preset config pairs and source specs for the local demo.

V1 and V2 differ in BOTH halves.  They use different source field names and
different id ranges, so combining V2's mapping with V1's scope yields visibly
wrong output (no rows, or rows with empty titles) instead of passing silently.
"""
from __future__ import annotations

PRESETS = {
    "v1": {
        "mapping": {"id": "sku", "title": "label", "price": "price_cents",
                    "stock": "in_stock"},
        "scope": {"ids": ["A-100", "A-101", "A-102", "B-200", "B-201"],
                  "price_max_cents": 1000},
    },
    "v2": {
        "mapping": {"id": "code", "title": "title", "price": "price",
                    "stock": "stock"},
        "scope": {"ids": ["Z-100", "Z-101", "Z-102", "Z-300", "Z-301"],
                  "exclude_zero_stock": True},
    },
}

# The union the mock must expose.  Every alias is on both the JSON rows and
# the HTML pages so the two config pairs can genuinely re-read the source.
ITEMS = [
    {"sku": "A-100", "code": "Z-100", "label": "Copper fitting 12mm (trade)",
     "title": "Copper Fitting 12 mm", "price_cents": 349, "price": 349,
     "in_stock": 5, "stock": 5},
    {"sku": "A-101", "code": "Z-101", "label": "Copper fitting 15mm (trade)",
     "title": "Copper Fitting 15 mm", "price_cents": 429, "price": 429,
     "in_stock": 0, "stock": 0},
    {"sku": "A-102", "code": "Z-102", "label": "Copper elbow 12mm (trade)",
     "title": "Copper Elbow 12 mm", "price_cents": 512, "price": 512,
     "in_stock": 7, "stock": 7},
    {"sku": "B-200", "code": "Z-200", "label": "Brass valve 1/2in (trade)",
     "title": "Brass Valve 1/2 in", "price_cents": 1899, "price": 1899,
     "in_stock": 3, "stock": 3},
    {"sku": "B-201", "code": "Z-201", "label": "Brass valve 3/4in (trade)",
     "title": "Brass Valve 3/4 in", "price_cents": 2299, "price": 2299,
     "in_stock": 0, "stock": 0},
    {"sku": "C-300", "code": "Z-300", "label": "Solder ring 22mm (trade)",
     "title": "Solder Ring 22 mm", "price_cents": 275, "price": 275,
     "in_stock": 55, "stock": 55},
    {"sku": "C-301", "code": "Z-301", "label": "Flux paste 100g (trade)",
     "title": "Flux Paste 100 g", "price_cents": 845, "price": 845,
     "in_stock": 21, "stock": 21},
    {"sku": "D-400", "code": "Z-400", "label": "Pipe clip 15mm bag/100 (trade)",
     "title": "Pipe Clip 15 mm bag of 100", "price_cents": 735, "price": 735,
     "in_stock": 33, "stock": 33},
]

ALL_IDS = [item["sku"] for item in ITEMS]
PAGE_PREFIX = "/product/"


def page_ids() -> list[str]:
    """Every union id; the page collector is told which ids its config needs."""
    return sorted({item["sku"] for item in ITEMS} | {item["code"] for item in ITEMS})


def expected_rows(preset_name: str) -> list[dict]:
    """Independently computed expected output (hand-written, not via project())."""
    preset = PRESETS[preset_name]
    mapping, scope = preset["mapping"], preset["scope"]
    wanted = set(scope.get("ids") or [])
    price_max = scope.get("price_max_cents")
    exclude_zero = bool(scope.get("exclude_zero_stock"))
    rows = []
    for item in ITEMS:
        item_id = str(item[mapping["id"]])
        if wanted and item_id not in wanted:
            continue
        price = int(item[mapping["price"]])
        if price_max is not None and price > price_max:
            continue
        if exclude_zero and int(item[mapping["stock"]]) == 0:
            continue
        rows.append({"id": item_id, "title": str(item[mapping["title"]]),
                     "price_cents": price})
    rows.sort(key=lambda r: r["id"])
    return rows
