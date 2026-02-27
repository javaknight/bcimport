"""
One-shot script: creates/verifies the Lutron category tree in BigCommerce
and writes category_map.json for use by the main processor.

Run via: make prime-categories
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

# Subcategories to create under Lutron. Extend this list as needed.
LUTRON_SUBCATEGORIES = [
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
    "Utility Accessories"
]


def _teardown_existing(client: BCClient) -> None:
    """If category_map.json exists, delete all listed categories from BC then remove the file."""
    if not os.path.exists(CATEGORY_MAP_PATH):
        return

    with open(CATEGORY_MAP_PATH, encoding="utf-8") as f:
        category_map: dict[str, int] = json.load(f)

    if not category_map:
        os.remove(CATEGORY_MAP_PATH)
        return

    log.info("Existing category_map.json found — removing %d categories from BC", len(category_map))

    # Delete in reverse order so subcategories are removed before the parent
    for name, category_id in reversed(list(category_map.items())):
        log.info("Deleting category: %s (id=%d)", name, category_id)
        client.delete_category(category_id)  # raises on failure — halts run
        log.info("Deleted: %s", name)

    os.remove(CATEGORY_MAP_PATH)
    log.info("Removed %s", CATEGORY_MAP_PATH)


def prime_categories() -> None:
    client = BCClient()

    _teardown_existing(client)

    # Fetch existing categories
    existing = client.get_categories()
    by_name: dict[str, dict] = {c["name"]: c for c in existing}

    category_map: dict[str, int] = {}

    # Ensure Lutron parent exists (created hidden; activate separately)
    if "Lutron" in by_name:
        lutron_id = by_name["Lutron"]["id"]
        log.info("Lutron parent already exists: id=%d", lutron_id)
    else:
        lutron_id = client.create_category("Lutron", parent_id=0, is_visible=False)
        log.info("Created Lutron parent category: id=%d", lutron_id)

    category_map["Lutron"] = lutron_id

    # Ensure each subcategory exists under Lutron
    for name in LUTRON_SUBCATEGORIES:
        existing_sub = next(
            (c for c in existing if c["name"] == name and c["parent_id"] == lutron_id),
            None,
        )
        if existing_sub:
            sub_id = existing_sub["id"]
            log.info("Subcategory already exists: %s id=%d", name, sub_id)
        else:
            sub_id = client.create_category(name, parent_id=lutron_id, is_visible=False)
            log.info("Created subcategory: %s id=%d", name, sub_id)

        category_map[name] = sub_id

    # Write map
    with open(CATEGORY_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(category_map, f, indent=2)

    log.info("Wrote %s with %d entries", CATEGORY_MAP_PATH, len(category_map))


if __name__ == "__main__":
    prime_categories()
