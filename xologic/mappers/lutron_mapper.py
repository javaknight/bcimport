"""Lutron-specific mapper: XOlogic feed row → BigCommerce product payload.

Extends BaseMapper with Lutron field quirks:
- SKU prefix LU-
- brand_id 45
- Description link columns contain pre-built <a> tags; Extra-Spec Sheet is a raw URL
"""
import logging

import pandas as pd

import vendors.lutron as lutron_cfg
from mappers.base_mapper import BaseMapper, _str

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
    ENRICHERS = []  # LutronPricingEnricher will be added here when ready

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
