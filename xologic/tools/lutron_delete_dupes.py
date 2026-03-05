"""One-time tool: delete LU- dupe products that duplicate human-created BC entries.

Looks up the LU-{item} SKU for each item in SKIP_ITEM_NUMBERS. Reports which
actually exist (some may not have been imported) and with --apply deletes them.

Dry-run by default.

Usage:
    make lutron-delete-dupes          # dry-run
    make lutron-delete-dupes APPLY=1  # actually delete
"""
import argparse
import logging
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bc.client import BCClient
from mappers.base_mapper import build_sku
import vendors.lutron as lutron_cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def find_dupes(client: BCClient, skip_item_numbers: frozenset[str]) -> list[dict]:
    """Look up LU- SKUs for all skip items and return those that exist in BC."""
    skus = []
    for item in sorted(skip_item_numbers):
        try:
            sku = build_sku(lutron_cfg.SKU_PREFIX, item)
            skus.append((item, sku))
        except ValueError:
            skus.append((item, f"{lutron_cfg.SKU_PREFIX}{item}"))  # use raw for lookup attempt

    sku_list = [s for _, s in skus]
    sku_to_id = client.lookup_skus(sku_list)

    found = []
    for item, sku in skus:
        if sku in sku_to_id:
            found.append({"item_number": item, "sku": sku, "bc_id": sku_to_id[sku]})
        else:
            log.info("No dupe found in BC for %s (%s)", item, sku)
    return found


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete LU- dupe products for human-created BC entries (dry-run by default)"
    )
    parser.add_argument("--apply", action="store_true", help="Actually delete products in BC")
    parser.add_argument(
        "--report",
        default="output/lutron_delete_dupes.xlsx",
        help="Path for report (default: output/lutron_delete_dupes.xlsx)",
    )
    args = parser.parse_args()

    client = BCClient()
    skip_items = getattr(lutron_cfg, "SKIP_ITEM_NUMBERS", frozenset())
    if not skip_items:
        log.info("SKIP_ITEM_NUMBERS is empty — nothing to do.")
        return

    log.info("Checking %d skip items for LU- dupes in BC...", len(skip_items))
    dupes = find_dupes(client, skip_items)

    if not dupes:
        log.info("No LU- dupes found in BC.")
        return

    log.info("Found %d LU- dupe(s) in BC:", len(dupes))
    for d in dupes:
        log.info("  %s (BC id %d)", d["sku"], d["bc_id"])

    rows = []
    if args.apply:
        log.info("--apply: deleting %d product(s)...", len(dupes))
        for d in dupes:
            try:
                client.delete_product(d["bc_id"])
                log.info("Deleted %s (BC id %d)", d["sku"], d["bc_id"])
                rows.append({**d, "status": "deleted", "error": ""})
            except Exception as exc:  # pylint: disable=broad-except
                log.error("Failed to delete %s (BC id %d): %s", d["sku"], d["bc_id"], exc)
                rows.append({**d, "status": "failed", "error": str(exc)})
    else:
        log.info("Dry-run — pass --apply to delete. No changes made.")
        rows = [{**d, "status": "would_delete", "error": ""} for d in dupes]

    report_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", args.report)
    )
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    pd.DataFrame(rows).to_excel(report_path, index=False, engine="openpyxl")
    log.info("Report written: %s", report_path)


if __name__ == "__main__":
    main()
