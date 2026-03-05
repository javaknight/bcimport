"""BaseMapper: generic XOlogic → BigCommerce product mapper.

Subclass this for each vendor and override methods that differ.
"""
import json
import logging
import os
import re
from typing import Any, ClassVar

import pandas as pd

log = logging.getLogger(__name__)


class BaseMapper:
    """Abstract base class for vendor-specific field mappers.

    Subclasses must set the ClassVar attributes below and may override
    any _build_* method to customise behaviour for their vendor.
    """

    # --- Required vendor configuration (set by subclass) ---
    VENDOR_ID: ClassVar[int]
    PRODUCT_TYPES: ClassVar[set[int]]
    SKU_PREFIX: ClassVar[str]
    CATEGORY_MAP_FILE: ClassVar[str]
    ROOT_CATEGORY: ClassVar[str]    # e.g. "Electrical Hardware"
    VENDOR_CATEGORY: ClassVar[str]  # e.g. "Lutron"

    # --- Optional vendor configuration ---
    BRAND_ID: ClassVar[int | None] = None
    ENRICHERS: ClassVar[list] = []  # list of BaseEnricher subclasses to run before mapping
    SKIP_ITEM_NUMBERS: ClassVar[frozenset[str]] = frozenset()
    PDF_DAV_SUBDIR: ClassVar[str | None] = None
    PDF_LINK_COLUMNS: ClassVar[list[str]] = []

    # --- Per-instance category map cache ---
    _category_map: dict | None = None

    # ------------------------------------------------------------------
    # Channel IDs
    # ------------------------------------------------------------------

    @property
    def channel_ids(self) -> list[int]:
        """Return BC channel IDs to assign products to after creation.

        Reads CHANNEL_IDS (comma-separated) from env. Override in a subclass
        for vendor-specific channel configuration.
        """
        raw = os.environ.get("CHANNEL_IDS", os.environ.get("CHANNEL_ID", "1"))
        return [int(c.strip()) for c in raw.split(",") if c.strip()]

    # ------------------------------------------------------------------
    # Category map
    # ------------------------------------------------------------------

    def _get_category_map(self) -> dict:
        if self._category_map is None:
            map_path = os.path.join(
                os.path.dirname(__file__), "..", self.CATEGORY_MAP_FILE
            )
            with open(map_path, "r", encoding="utf-8") as f:
                self._category_map = json.load(f)
        return self._category_map

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def map_row(self, row: pd.Series) -> dict:
        """Map one XOlogic feed row to a BigCommerce product payload.

        Raises ValueError if the row is missing required fields (e.g. image).
        """
        sku = build_sku(self.SKU_PREFIX, str(row["Item Number"]))

        images = self._build_images(row)
        if not images:
            raise ValueError(f"No image URL — row skipped (Item Number: {row['Item Number']})")

        payload: dict = {
            "sku": sku,
            "name": _str(row.get("Item Name")) or sku,
            "type": "physical",
            "is_visible": False,
            "price": 0,        # default; overridden by mapper subclass if pricing data available
            "retail_price": 0, # always explicitly cleared; set by subclass only if MSRP is desired
            "weight": _num(row.get("Extra-Weight")) or 0,
            "mpn": str(row["Item Number"]),
            "description": self._build_description(row),
            "custom_fields": self._build_custom_fields(row),
            "images": images,
            "categories": self._build_categories(row),
        }

        if self.BRAND_ID is not None:
            payload["brand_id"] = self.BRAND_ID

        gtin = _str(row.get("GTIN"))
        if gtin:
            payload["gtin"] = gtin

        width = _num(row.get("Width"))
        if width is not None:
            payload["width"] = width

        height = _num(row.get("Height"))
        if height is not None:
            payload["height"] = height

        for img in payload["images"]:
            img["description"] = payload["name"]

        return payload

    # ------------------------------------------------------------------
    # Overrideable build helpers
    # ------------------------------------------------------------------

    def build_price_patch(self, row: pd.Series) -> dict | None:  # pylint: disable=unused-argument
        """Return {price, cost_price} for a human-created product price patch, or None.

        Override in vendor subclasses that have computable pricing.
        """
        return None

    def _build_description(self, row: pd.Series) -> str:
        """Build description HTML. Override per vendor as needed."""
        parts = [_str(row.get("Short Description")) or ""]
        unspsc = _str(row.get("Extra-UNSPSC"))
        if unspsc:
            parts.append(f"UNSPSC: {unspsc}")
        return "<br>\n".join(p for p in parts if p)

    def _build_custom_fields(self, row: pd.Series) -> list[dict]:
        """Build custom_fields array. Override per vendor as needed."""
        fields: list[dict] = []
        finish = _str(row.get("Variant-Finish")) or _str(row.get("Standard-Finish"))
        _add_field(fields, "Color/ Finish", finish)
        _add_field(fields, "Style", _str(row.get("Standard-Style")))
        _add_field(fields, "Length", _str(row.get("Extra-Length")))
        return fields

    def _build_images(self, row: pd.Series) -> list[dict]:
        """Build images array from Image Path."""
        url = _str(row.get("Image Path"))
        if url:
            return [{"image_url": url, "is_thumbnail": True}]
        return []

    def _build_categories(self, row: pd.Series) -> list[int]:
        """Resolve [root, vendor, subcategory] category IDs via the category map."""
        cat_map = self._get_category_map()
        ids: list[int] = []

        root_id = cat_map.get(self.ROOT_CATEGORY)
        if root_id:
            ids.append(root_id)

        vendor_id = cat_map.get(self.VENDOR_CATEGORY)
        if vendor_id:
            ids.append(vendor_id)

        subcategory = _str(row.get("Standard-Subcategory"))
        if subcategory:
            sub_id = cat_map.get(subcategory)
            if sub_id:
                ids.append(sub_id)
            else:
                log.warning("No category mapping for subcategory: %s", subcategory)

        return ids


# ------------------------------------------------------------------
# Module-level helpers (shared across all mappers)
# ------------------------------------------------------------------

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


def _add_field(fields: list[dict], name: str, value: str | None) -> None:
    """Append a custom field dict if value is non-empty."""
    if value:
        fields.append({"name": name, "value": value})


MAX_SKU_LENGTH = 30


def build_sku(prefix: str, item_number: str) -> str:
    """Build a BC/Acumatica-safe SKU capped at MAX_SKU_LENGTH characters.

    Strategy:
    1. {prefix}{item_number}                     — if <= 15, use as-is
    2. {prefix}{item_number with hyphens removed} — if <= 15, use stripped form
    3. Raise ValueError                           — row goes to error report
    """
    raw = f"{prefix}{item_number}"
    if len(raw) <= MAX_SKU_LENGTH:
        return raw
    stripped = f"{prefix}{item_number.replace('-', '')}"
    if len(stripped) <= MAX_SKU_LENGTH:
        return stripped
    raise ValueError(
        f"SKU too long (>{MAX_SKU_LENGTH} chars) even after stripping hyphens: "
        f"{raw!r} -> {stripped!r}"
    )
