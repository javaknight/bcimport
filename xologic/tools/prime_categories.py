"""
Creates/verifies a vendor category hierarchy in BigCommerce and writes a
vendor-specific category map JSON for use by the processor.

Default behavior is additive: existing categories and their IDs are preserved,
only missing categories are created.  Use --teardown to delete and recreate
the entire tree (WARNING: all existing product category assignments will be lost).

Run via:
    make prime-lutron-categories
    make prime-teardown-lutron-categories   # full reset
or directly:
    python tools/prime_categories.py --vendor lutron
    python tools/prime_categories.py --vendor lutron --teardown
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


def _category_tree_id(category: dict) -> int | None:
    """Return tree_id from category payload, regardless of BC response field naming."""
    return category.get("tree_id") or category.get("category_tree_id")


def _teardown_existing(client: BigCommerceAPI, map_path: str) -> None:
    """If the category map file exists, delete all listed categories from BC then remove the file."""
    if not os.path.exists(map_path):
        return

    with open(map_path, encoding="utf-8") as f:
        category_map: dict[str, int] = json.load(f)

    if not category_map:
        os.remove(map_path)
        return

    log.info("Existing %s found — removing %d categories from BC", map_path, len(category_map))

    # Delete in reverse order so subcategories are removed before parents
    for name, category_id in reversed(list(category_map.items())):
        log.info("Deleting category: %s (id=%d)", name, category_id)
        client.categories_v3.delete(category_id)
        log.info("Deleted: %s", name)

    os.remove(map_path)
    log.info("Removed %s", map_path)


def prime_categories(vendor_cfg, teardown: bool = False) -> None:
    """Create/verify the vendor category hierarchy and write the category map file.

    If teardown=True, deletes the entire existing tree first (IDs will change).
    Default (teardown=False) is additive: adds missing categories, preserves existing ones.
    """
    client = BigCommerceAPI(
        store_hash=os.environ["BC_STORE_HASH"],
        access_token=os.environ["BC_ACCESS_TOKEN"],
    )
    category_tree = int(os.environ["CATEGORY_TREE"])

    map_path = os.path.join(os.path.dirname(__file__), "..", vendor_cfg.CATEGORY_MAP_FILE)
    if teardown:
        log.warning("--teardown specified: deleting existing category tree for %s", vendor_cfg.VENDOR_CATEGORY)
        _teardown_existing(client, map_path)
        existing_map: dict[str, int] = {}
    elif os.path.exists(map_path):
        log.info("Category map exists — running in additive mode (use --teardown to reset)")
        with open(map_path, encoding="utf-8") as f:
            existing_map = json.load(f)
    else:
        existing_map = {}

    # Fetch existing categories from BC
    existing = list(client.categories_v3.all())
    existing_by_id: dict[int, dict] = {c["id"]: c for c in existing}

    category_map: dict[str, int] = dict(existing_map)  # start from what we already know

    def _ensure_category(
        name: str, parent_id: int, existing_id: int | None
    ) -> int:
        """Return BC category ID, creating or updating as needed."""
        if existing_id is not None:
            bc_cat = existing_by_id.get(existing_id)
            if bc_cat is None:
                log.warning("Category id=%d not found in BC — recreating: %s", existing_id, name)
            elif bc_cat["name"] != name:
                log.info("Renaming category id=%d: %r → %r", existing_id, bc_cat["name"], name)
                client.categories_v3.update(existing_id, data={"name": name})
                return existing_id
            else:
                log.info("Category OK: %s id=%d", name, existing_id)
                return existing_id
        # Create new
        result = client.categories_v3.create(data={
            "name": name,
            "parent_id": parent_id,
            "is_visible": False,
            "tree_id": category_tree,
        })
        new_id = result["id"]
        log.info("Created category: %s id=%d", name, new_id)
        return new_id

    # Root category (e.g. "Electrical Hardware")
    root_id = _ensure_category(
        vendor_cfg.ROOT_CATEGORY,
        parent_id=0,
        existing_id=category_map.get(vendor_cfg.ROOT_CATEGORY),
    )
    category_map[vendor_cfg.ROOT_CATEGORY] = root_id

    # Vendor category (e.g. "Lutron") under root
    vendor_cat_id = _ensure_category(
        vendor_cfg.VENDOR_CATEGORY,
        parent_id=root_id,
        existing_id=category_map.get(vendor_cfg.VENDOR_CATEGORY),
    )
    category_map[vendor_cfg.VENDOR_CATEGORY] = vendor_cat_id

    # Subcategories under vendor
    for name in vendor_cfg.SUBCATEGORIES:
        sub_id = _ensure_category(
            name,
            parent_id=vendor_cat_id,
            existing_id=category_map.get(name),
        )
        category_map[name] = sub_id

    # Write map
    with open(map_path, "w", encoding="utf-8") as f:
        json.dump(category_map, f, indent=2)

    log.info("Wrote %s with %d entries", map_path, len(category_map))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create/verify BC category hierarchy for a vendor")
    parser.add_argument("--vendor", required=True, help="Vendor name (e.g. lutron)")
    parser.add_argument("--teardown", action="store_true", help="Delete and recreate the entire tree (WARNING: loses all product category assignments)")
    args = parser.parse_args()

    try:
        cfg = importlib.import_module(f"vendors.{args.vendor.lower()}")
    except ModuleNotFoundError:
        print(f"ERROR: no vendor config found for '{args.vendor}' (expected vendors/{args.vendor.lower()}.py)")
        sys.exit(1)

    prime_categories(cfg, teardown=args.teardown)
