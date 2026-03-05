"""One-time tool: find Lutron SKUs in BC that exceed 15 chars and repair them.

Dry-run by default — shows what would change without touching BC.
Pass --apply to actually rename.

Usage:
    make lutron-sku-repair          # dry-run
    make lutron-sku-repair-dev      # dry-run, explicit env
    make lutron-sku-repair-prod     # dry-run, prod env

    make lutron-sku-repair APPLY=1  # apply changes
"""
import argparse
import logging
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bc.client import BATCH_SIZE, BCClient
from mappers.base_mapper import MAX_SKU_LENGTH, build_sku

SKU_PREFIX = "LU-"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def find_long_skus(client: BCClient) -> list[dict]:
    """Page through all LU- products and return those with SKU > MAX_SKU_LENGTH."""
    long_skus = []
    count = 0
    log.info("Scanning BC for LU- products (SKU > %d chars)...", MAX_SKU_LENGTH)
    for product in client.iter_products(**{"include_fields": "id,sku"}):
        sku = product["sku"]
        if not sku.startswith(SKU_PREFIX):
            continue
        count += 1
        if len(sku) > MAX_SKU_LENGTH:
            long_skus.append({"id": product["id"], "sku": sku})
    log.info(
        "Scanned %d LU- products, found %d with SKU > %d chars",
        count, len(long_skus), MAX_SKU_LENGTH,
    )
    return long_skus


def compute_repairs(long_skus: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (repairable, unfixable) lists."""
    repairable = []
    unfixable = []
    for row in long_skus:
        sku = row["sku"]
        item_number = sku[len(SKU_PREFIX):]
        try:
            new_sku = build_sku(SKU_PREFIX, item_number)
            repairable.append({"bc_id": row["id"], "old_sku": sku, "new_sku": new_sku})
        except ValueError as exc:
            unfixable.append({"bc_id": row["id"], "old_sku": sku, "error": str(exc)})
    return repairable, unfixable


def apply_repairs(client: BCClient, repairable: list[dict]) -> list[dict]:
    """PUT SKU renames in batches of 10. Returns list of failed rows."""
    failed = []
    for i in range(0, len(repairable), BATCH_SIZE):
        batch = repairable[i : i + BATCH_SIZE]
        payloads = [{"id": r["bc_id"], "sku": r["new_sku"]} for r in batch]
        status, body = client.update_products(payloads)
        if status in (200, 207):
            for r in batch:
                log.info("Renamed: %s -> %s", r["old_sku"], r["new_sku"])
            if status == 207:
                log.warning("Batch %d: 207 partial success — %s", i, body)
        else:
            log.error("Batch %d failed (HTTP %s): %s", i, status, body)
            failed.extend(batch)
        time.sleep(0.2)
    return failed


def write_report(
    repairable: list[dict],
    unfixable: list[dict],
    failed: list[dict],
    path: str,
) -> None:
    failed_ids = {r["bc_id"] for r in failed}
    rows = [
        {
            "bc_id": r["bc_id"],
            "old_sku": r["old_sku"],
            "new_sku": r["new_sku"],
            "status": "failed" if r["bc_id"] in failed_ids else "renamed",
            "error": "",
        }
        for r in repairable
    ] + [
        {
            "bc_id": e["bc_id"],
            "old_sku": e["old_sku"],
            "new_sku": "",
            "status": "unfixable",
            "error": e["error"],
        }
        for e in unfixable
    ]
    if not rows:
        log.info("No long SKUs found — no report written.")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(rows).to_excel(path, index=False, engine="openpyxl")
    log.info("Report written: %s (%d rows)", path, len(rows))


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair Lutron SKUs > 15 chars in BC")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually rename SKUs in BC (default: dry-run only)",
    )
    parser.add_argument(
        "--report",
        default="output/lutron_sku_repair.xlsx",
        help="Path for repair report (default: output/lutron_sku_repair.xlsx)",
    )
    args = parser.parse_args()

    client = BCClient()
    long_skus = find_long_skus(client)

    if not long_skus:
        log.info("No SKUs need repair.")
        return

    repairable, unfixable = compute_repairs(long_skus)
    log.info(
        "Repairable: %d  |  Unfixable (still too long after stripping): %d",
        len(repairable), len(unfixable),
    )

    failed: list[dict] = []
    if args.apply:
        log.info("--apply: renaming %d SKUs in BC...", len(repairable))
        failed = apply_repairs(client, repairable)
        log.info(
            "Done: %d renamed, %d failed, %d unfixable",
            len(repairable) - len(failed), len(failed), len(unfixable),
        )
    else:
        log.info("Dry-run — no changes made. Pass --apply to rename.")
        for r in repairable:
            log.info("  WOULD RENAME: %s -> %s", r["old_sku"], r["new_sku"])
        for e in unfixable:
            log.warning("  UNFIXABLE: %s — %s", e["old_sku"], e["error"])

    report_path = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", args.report)
    )
    write_report(repairable, unfixable, failed, report_path)


if __name__ == "__main__":
    main()
