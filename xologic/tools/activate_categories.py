"""
Sets is_visible=True for all categories in a vendor's category map.
Run this when you're ready to make the category tree live in the storefront.

Run via:
    make activate-lutron-categories
or directly:
    python tools/activate_categories.py --vendor lutron
"""
import argparse
import importlib
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bigc import BigCommerceAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def activate_categories(map_path: str) -> None:
    if not os.path.exists(map_path):
        log.error("Category map not found at %s — run prime-categories first", map_path)
        sys.exit(1)

    with open(map_path, encoding="utf-8") as f:
        category_map: dict[str, int] = json.load(f)

    client = BigCommerceAPI(
        store_hash=os.environ["BC_STORE_HASH"],
        access_token=os.environ["BC_ACCESS_TOKEN"],
    )

    for name, category_id in category_map.items():
        client.categories_v3.update(category_id, data={"is_visible": True})
        log.info("Activated: %s (id=%d)", name, category_id)

    log.info("Done — %d categories set to is_visible=True", len(category_map))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Activate BC categories for a vendor")
    parser.add_argument("--vendor", required=True, help="Vendor name (e.g. lutron)")
    args = parser.parse_args()

    try:
        cfg = importlib.import_module(f"vendors.{args.vendor.lower()}")
    except ModuleNotFoundError:
        print(f"ERROR: no vendor config found for '{args.vendor}' (expected vendors/{args.vendor.lower()}.py)")
        sys.exit(1)

    vendor_map_path = os.path.join(os.path.dirname(__file__), "..", cfg.CATEGORY_MAP_FILE)
    activate_categories(vendor_map_path)
