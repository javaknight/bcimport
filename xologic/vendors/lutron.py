"""Lutron vendor configuration.

Single source of truth for Lutron-specific settings used by
the mapper, prime_categories, and activate_categories tools.
"""

VENDOR_ID = 4460
PRODUCT_TYPES = {0, 1, 4}
SKU_PREFIX = "LU-"
BRAND_ID = 45

CATEGORY_MAP_FILE = "lutron_category_map.json"
ROOT_CATEGORY = "Electrical"
VENDOR_CATEGORY = "Lutron"

# Selling price = My Price * PRICE_MARKUP  (subject to change)
PRICE_MARKUP = 1.22

SUBCATEGORIES = [
    "Cables",
    "Connectors",
    "Cords",
    "Dimmers",
    "Fan Wall Mount Controls",
    "Lighting Accessories",
    "Lighting Kits",
    "Lightswitches",
    "Other Lighting Controls",
    "Other Specialty Items",
    "Power Supplies",
    "Sensors",
    "Switch Plates",
    "Transformers",
    "Utility Accessories",
]

# PDF mirroring/link rewrite configuration
PDF_DAV_SUBDIR = "catalog/documents/lutron/pdfs"
PDF_LINK_COLUMNS = [
    "Extra-Installation Link",
    "Extra-Line Drawing Link",
    "Extra-Tech Drawing Link",
    "Extra-Warranty Link",
    "Extra-Brochure",
    "Extra-Video Clip",
    "Extra-Spec Sheet",
]

# Human-created products: skip import (no LU- SKU), but patch pricing on the
# existing BC product via MPN lookup on each run.
SKIP_ITEM_NUMBERS: frozenset[str] = frozenset([
    "DITT300BL",
    "TT-300 BR",
    "TT-300-W",
    "TTCL-100H-BL",
    "TTCL-100H-BR",
    "TTCL-100H-WH",
    "LUT-MLC",
    "RPFDU10BR",
    "RPFDU10WH",
    "LCTRP-253P-IV",
    "PD-3PCL-WH",
    "P-PKG1P-WH",
    "DVRF-5NS-WH",
    "CTRP-253P-WH",
    "MA-PRO-WH",
    "MA-R-WH",
    "RRD-PRO-WH",
    "AYCL-153P-WH",
])
