"""Lutron-specific mapper: XOlogic feed row → BigCommerce product payload.

Extends BaseMapper with Lutron field quirks:
- SKU prefix LU-
- brand_id 45
- Description link columns contain pre-built <a> tags; Extra-Spec Sheet is a raw URL
"""
import logging

import pandas as pd

import vendors.lutron as lutron_cfg
from enrichers.lutron_pricing_enricher import LutronPricingEnricher
from mappers.base_mapper import BaseMapper, _num, _str

log = logging.getLogger(__name__)

# These columns contain complete <a href="...">...</a> strings already
_PREBUILT_LINK_COLS = [
    "Extra-Installation Link",
    "Extra-Line Drawing Link",
    "Extra-Tech Drawing Link",
    "Extra-Warranty Link",
    "Extra-Brochure",
    "Extra-Video Clip",
]


class LutronMapper(BaseMapper):
    VENDOR_ID = lutron_cfg.VENDOR_ID
    PRODUCT_TYPES = lutron_cfg.PRODUCT_TYPES
    SKU_PREFIX = lutron_cfg.SKU_PREFIX
    BRAND_ID = lutron_cfg.BRAND_ID
    CATEGORY_MAP_FILE = lutron_cfg.CATEGORY_MAP_FILE
    ROOT_CATEGORY = lutron_cfg.ROOT_CATEGORY
    VENDOR_CATEGORY = lutron_cfg.VENDOR_CATEGORY
    ENRICHERS = [LutronPricingEnricher]

    def map_row(self, row: pd.Series) -> dict:
        payload = super().map_row(row)

        upc = _str(row.get("Pricing-UPC"))
        if upc:
            payload["upc"] = upc

        list_price = _num(row.get("Pricing-ListPrice"))
        if list_price is not None:
            payload["retail_price"] = list_price

        my_price = _num(row.get("Pricing-MyPrice"))
        if my_price is not None:
            payload["cost_price"] = my_price
            payload["price"] = round(my_price * lutron_cfg.PRICE_MARKUP, 2)

        if not payload.get("price"):
            raise ValueError(
                f"No pricing data for Item Number {row['Item Number']} — skipped"
            )

        return payload

    def _build_description(self, row: pd.Series) -> str:
        """Short Description + pre-built link fields + Spec Sheet (raw URL) + UNSPSC."""
        parts = [_str(row.get("Short Description")) or ""]

        for col in _PREBUILT_LINK_COLS:
            val = _str(row.get(col))
            if val:
                parts.append(val)

        # Extra-Spec Sheet contains a raw URL, not a pre-built anchor tag
        spec_url = _str(row.get("Extra-Spec Sheet"))
        if spec_url:
            parts.append(f'<a href="{spec_url}">Spec Sheet</a>')

        unspsc = _str(row.get("Extra-UNSPSC"))
        if unspsc:
            parts.append(f"UNSPSC: {unspsc}")

        return "<br>\n".join(p for p in parts if p)
