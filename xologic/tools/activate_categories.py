"""
Sets is_visible=True for all categories in category_map.json.
Run this when you're ready to make the Lutron category tree live in the storefront.

Run via: make activate-categories
"""
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bc.client import BCClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

CATEGORY_MAP_PATH = os.path.join(os.path.dirname(__file__), "..", "category_map.json")


def activate_categories() -> None:
    if not os.path.exists(CATEGORY_MAP_PATH):
        log.error("category_map.json not found at %s — run prime-categories first", CATEGORY_MAP_PATH)
        sys.exit(1)

    with open(CATEGORY_MAP_PATH, encoding="utf-8") as f:
        category_map: dict[str, int] = json.load(f)

    client = BCClient()

    for name, category_id in category_map.items():
        client.update_category(category_id, is_visible=True)
        log.info("Activated: %s (id=%d)", name, category_id)

    log.info("Done — %d categories set to is_visible=True", len(category_map))


if __name__ == "__main__":
    activate_categories()
