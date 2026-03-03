"""Lutron vendor configuration.

Single source of truth for Lutron-specific settings used by
the mapper, prime_categories, and activate_categories tools.
"""

VENDOR_ID = 4460
PRODUCT_TYPES = {0, 1, 4}
SKU_PREFIX = "LU-"
BRAND_ID = 45

CATEGORY_MAP_FILE = "lutron_category_map.json"
ROOT_CATEGORY = "Electrical Hardware"
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
