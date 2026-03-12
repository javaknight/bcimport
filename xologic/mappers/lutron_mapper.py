"""Lutron-specific mapper: XOlogic feed row → BigCommerce product payload.

Extends BaseMapper with Lutron field quirks:
- SKU prefix LU-
- brand_id 45
- Description link columns contain pre-built <a> tags; Extra-Spec Sheet is a raw URL
- PDF URLs are rewritten deterministically to BC-hosted /content/ URLs
"""
import logging
import os

import pandas as pd

import vendors.lutron as lutron_cfg
from enrichers.lutron_pricing_enricher import LutronPricingEnricher
from mappers.base_mapper import BaseMapper, _num, _str
from mappers.pdf_links import build_description_link_or_none

log = logging.getLogger(__name__)

class LutronMapper(BaseMapper):
    VENDOR_ID = lutron_cfg.VENDOR_ID
    PRODUCT_TYPES = lutron_cfg.PRODUCT_TYPES
    SKU_PREFIX = lutron_cfg.SKU_PREFIX
    BRAND_ID = lutron_cfg.BRAND_ID
    CATEGORY_MAP_FILE = lutron_cfg.CATEGORY_MAP_FILE
    ROOT_CATEGORY = lutron_cfg.ROOT_CATEGORY
    VENDOR_CATEGORY = lutron_cfg.VENDOR_CATEGORY
    ENRICHERS = [LutronPricingEnricher]
    SKIP_ITEM_NUMBERS = getattr(lutron_cfg, "SKIP_ITEM_NUMBERS", frozenset())
    PDF_DAV_SUBDIR = getattr(lutron_cfg, "PDF_DAV_SUBDIR", "lutron/pdfs")
    PDF_LINK_COLUMNS = getattr(lutron_cfg, "PDF_LINK_COLUMNS", [])

    def _map_row(self, row: pd.Series) -> dict:
        payload = super()._map_row(row)
        payload["name"] = _str(row.get("Short Description")) or payload["name"]

        upc = _str(row.get("Pricing-UPC"))
        if upc:
            payload["upc"] = upc

        my_price = _num(row.get("Pricing-MyPrice"))
        map_price = _num(row.get("Pricing-MAP")) or None  # treat 0 as absent
        if my_price is not None:
            payload["cost_price"] = my_price
            if map_price is not None:
                # Sell at MAP; never mark up above it
                payload["price"] = map_price
            else:
                payload["price"] = round(my_price * lutron_cfg.PRICE_MARKUP, 2)

        if not payload.get("price"):
            raise ValueError(
                f"No pricing data for Item Number {row['Item Number']} — skipped"
            )

        depth = self._resolve_depth(payload)
        if depth is not None:
            payload["depth"] = depth

        return payload

    def _resolve_depth(self, payload: dict) -> float | None:
        """Depth value for BC physical products.

        Rule order (Lutron):
        1) Use custom field Length when present.
        2) Otherwise, use the smaller of width/height when present.
        3) Otherwise, leave unset.
        """
        for field in payload.get("custom_fields", []):
            if field.get("name") == "Length":
                length_value = _str(field.get("value"))
                if length_value:
                    return _num(length_value)
                break

        width = payload.get("width")
        height = payload.get("height")
        candidates = [v for v in (width, height) if isinstance(v, (int, float))]
        if not candidates:
            return None
        return float(min(candidates))

    def build_price_patch(self, row: pd.Series) -> dict | None:
        """Return price fields for patching a human-created BC product."""
        my_price = _num(row.get("Pricing-MyPrice"))
        if my_price is None:
            return None
        map_price = _num(row.get("Pricing-MAP")) or None  # treat 0 as absent
        if map_price is not None:
            return {
                "price": map_price,
                "cost_price": my_price,
            }
        return {
            "price": round(my_price * lutron_cfg.PRICE_MARKUP, 2),
            "cost_price": my_price,
        }

    def _build_description(self, row: pd.Series) -> str:
        """Item Name + pre-built link fields + Spec Sheet (raw URL) + UNSPSC.

        PDF URLs are rewritten deterministically to BC-hosted /content/ paths.
        """
        parts = [_str(row.get("Short Description")) or ""]
        content_base_url = os.environ.get("BC_CONTENT_BASE_URL", "").rstrip("/")

        for col in self.PDF_LINK_COLUMNS:
            val = _str(row.get(col))
            if not val:
                continue
            rendered = build_description_link_or_none(val, col, self.PDF_DAV_SUBDIR, content_base_url)
            if rendered:
                parts.append(rendered)

        unspsc = _str(row.get("Extra-UNSPSC"))
        if unspsc:
            parts.append(f"UNSPSC: {unspsc}")

        return "<br>\n".join(p for p in parts if p)
