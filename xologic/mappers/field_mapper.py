"""
Maps a single XOlogic feed row (pandas Series) to a BigCommerce product payload dict.
"""
import json
import logging
import os
import re
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

CATEGORY_MAP_PATH = os.path.join(os.path.dirname(__file__), "..", "category_map.json")

_category_map: dict | None = None


def _load_category_map() -> dict:
    global _category_map
    if _category_map is None:
        with open(CATEGORY_MAP_PATH, "r", encoding="utf-8") as f:
            _category_map = json.load(f)
    return _category_map


def _str(value: Any) -> str | None:
    """Return stripped string or None if blank/NaN."""
    if pd.isna(value):
        return None
    s = str(value).strip()
    return s if s else None


def _num(value: Any) -> float | None:
    """Extract leading numeric value from a string like '0.300 L' or '2.3000 IN'."""
    if pd.isna(value):
        return None
    match = re.match(r"[\d.]+", str(value).strip())
    if match:
        try:
            return float(match.group())
        except ValueError:
            return None
    return None


def _build_description(row: pd.Series) -> str:
    """Build description HTML: Short Description + appended link/text lines."""
    parts = [_str(row.get("Short Description")) or ""]

    link_fields = {
        "Extra-Installation Link": "Installation",
        "Extra-Line Drawing Link": "Line Drawing",
        "Extra-Spec Sheet": "Spec Sheet",
        "Extra-Tech Drawing Link": "Tech Drawing",
        "Extra-Warranty Link": "Warranty",
        "Extra-Brochure": "Brochure",
        "Extra-Video Clip": "Video",
    }
    for col, label in link_fields.items():
        url = _str(row.get(col))
        if url:
            parts.append(f'<a href="{url}">{label}</a>')

    unspsc = _str(row.get("Extra-UNSPSC"))
    if unspsc:
        parts.append(f"UNSPSC: {unspsc}")

    return "<br>\n".join(p for p in parts if p)


def _build_custom_fields(row: pd.Series) -> list[dict]:
    """Build custom_fields array from mapped columns."""
    fields = []

    def add(name: str, value: Any) -> None:
        v = _str(value)
        if v:
            fields.append({"name": name, "value": v})

    finish = _str(row.get("Variant-Finish")) or _str(row.get("Standard-Finish"))
    add("Finish", finish)
    add("Style", row.get("Standard-Style"))
    add("Length", row.get("Extra-Length"))

    return fields


def _build_images(row: pd.Series) -> list[dict]:
    """Build images array from Image Path (primary thumbnail)."""
    images = []
    url = _str(row.get("Image Path"))
    if url:
        images.append({"image_url": url, "is_thumbnail": True})
    return images


def _build_categories(row: pd.Series) -> list[int]:
    """Resolve category IDs from category_map.json."""
    cat_map = _load_category_map()
    ids = []

    # Always include the Lutron parent
    lutron_id = cat_map.get("Lutron")
    if lutron_id:
        ids.append(lutron_id)

    subcategory = _str(row.get("Standard-Subcategory"))
    if subcategory:
        sub_id = cat_map.get(subcategory)
        if sub_id:
            ids.append(sub_id)
        else:
            log.warning("No category mapping found for subcategory: %s", subcategory)

    return ids


def map_row(row: pd.Series) -> dict:
    """
    Map one XOlogic feed row to a BigCommerce product payload.
    Returns a dict ready to POST or PUT to the BC API.
    """
    sku = f"LU-{row['Item Number']}"

    payload: dict = {
        "sku": sku,
        "name": _str(row.get("Item Name")) or sku,
        "type": "physical",
        "price": 0,        # price not in scope for this feed; set to 0
        "weight": _num(row.get("Extra-Weight")) or 0,
        "description": _build_description(row),
        "custom_fields": _build_custom_fields(row),
        "images": _build_images(row),
        "categories": _build_categories(row),
    }

    gtin = _str(row.get("GTIN"))
    if gtin:
        payload["gtin"] = gtin

    width = _num(row.get("Width"))
    if width is not None:
        payload["width"] = width

    height = _num(row.get("Height"))
    if height is not None:
        payload["height"] = height

    return payload
