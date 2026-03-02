"""
One-shot script: creates/verifies the Electrical Hardware -> Lutron category
hierarchy in BigCommerce and writes category_map.json for use by the main
processor.

Run via: make prime-categories
"""
import json
import logging
import os

from bigc import BigCommerceAPI


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

CATEGORY_MAP_PATH = os.path.join(os.path.dirname(__file__), "..", "category_map.json")
ELECTRICAL_HARDWARE = "Electrical Hardware"

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


def _category_tree_id(category: dict) -> int | None:
    """Return tree_id from category payload, regardless of BC response field naming."""
    return category.get("tree_id") or category.get("category_tree_id")


def _teardown_existing(client: BigCommerceAPI) -> None:
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
        client.categories_v3.delete(category_id)  # raises on failure — halts run
        log.info("Deleted: %s", name)

    os.remove(CATEGORY_MAP_PATH)
    log.info("Removed %s", CATEGORY_MAP_PATH)


def prime_categories() -> None:
    client = BigCommerceAPI(
        store_hash=os.environ["BC_STORE_HASH"],
        access_token=os.environ["BC_ACCESS_TOKEN"],
    )
    category_tree = int(os.environ["CATEGORY_TREE"])

    _teardown_existing(client)

    # Fetch existing categories
    existing = list(client.categories_v3.all())
    existing_in_tree = [c for c in existing if _category_tree_id(c) == category_tree]

    category_map: dict[str, int] = {}

    # Ensure Electrical Hardware root exists in the configured tree
    electrical_hardware = next(
        (
            c
            for c in existing_in_tree
            if c["name"] == ELECTRICAL_HARDWARE and c.get("parent_id") == 0
        ),
        None,
    )
    if electrical_hardware:
        electrical_hardware_id = electrical_hardware["id"]
        log.info(
            "%s root already exists in tree %d: id=%d",
            ELECTRICAL_HARDWARE,
            category_tree,
            electrical_hardware_id,
        )
    else:
        result = client.categories_v3.create(data={
            "name": ELECTRICAL_HARDWARE,
            "parent_id": 0,
            "is_visible": False,
            "tree_id": category_tree,
        })
        electrical_hardware_id = result["id"]
        log.info(
            "Created %s root in tree %d: id=%d",
            ELECTRICAL_HARDWARE,
            category_tree,
            electrical_hardware_id,
        )

    category_map[ELECTRICAL_HARDWARE] = electrical_hardware_id

    # Ensure Lutron exists under Electrical Hardware
    existing_lutron = next(
        (
            c
            for c in existing_in_tree
            if c["name"] == "Lutron" and c.get("parent_id") == electrical_hardware_id
        ),
        None,
    )
    if existing_lutron:
        lutron_id = existing_lutron["id"]
        log.info("Lutron already exists under %s: id=%d", ELECTRICAL_HARDWARE, lutron_id)
    else:
        result = client.categories_v3.create(data={
            "name": "Lutron",
            "parent_id": electrical_hardware_id,
            "is_visible": False,
            "tree_id": category_tree,
        })
        lutron_id = result["id"]
        log.info("Created Lutron under %s: id=%d", ELECTRICAL_HARDWARE, lutron_id)

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
            result = client.categories_v3.create(data={
                "name": name,
                "parent_id": lutron_id,
                "is_visible": False,
                "tree_id": category_tree,
            })
            sub_id = result["id"]
            log.info("Created subcategory: %s id=%d", name, sub_id)

        category_map[name] = sub_id

    # Write map
    with open(CATEGORY_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(category_map, f, indent=2)

    log.info("Wrote %s with %d entries", CATEGORY_MAP_PATH, len(category_map))


if __name__ == "__main__":
    prime_categories()
