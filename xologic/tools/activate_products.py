"""
Activate products from a specific import run.

Searches for all products tagged with the given run ID (stored as the
bcimport/awsbatch metafield) and sets is_visible=true on each.

Usage:
    python tools/activate_products.py --run-id 20260303-131158
"""
import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bc.client import BCClient, BATCH_SIZE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def activate_products(run_id: str) -> None:
    start = time.time()
    client = BCClient()

    log.info("Searching for products with run ID: %s", run_id)
    metafields = client.search_products_by_metafield("bcimport", "awsbatch", run_id)
    product_ids = [mf["resource_id"] for mf in metafields]

    if not product_ids:
        log.warning("No products found for run ID: %s", run_id)
        return

    log.info("Found %d product(s) — setting is_visible=true", len(product_ids))

    n_success = 0
    n_errors = 0

    for i in range(0, len(product_ids), BATCH_SIZE):
        chunk = product_ids[i : i + BATCH_SIZE]
        payloads = [{"id": pid, "is_visible": True} for pid in chunk]
        status, body = client.update_products(payloads)
        if status == 200:
            n_success += len(chunk)
        else:
            n_errors += len(chunk)
            log.error(
                "Batch activate failed (status %d) for product IDs %s: %s",
                status, chunk, body,
            )

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    log.info(
        "Activated %d/%d products. %d errors. Elapsed: %dm %ds",
        n_success, len(product_ids), n_errors, mins, secs,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Set is_visible=true for all products from a given import run"
    )
    parser.add_argument(
        "--run-id", required=True,
        help="Run ID from the import log (e.g. 20260303-131158)",
    )
    args = parser.parse_args()
    activate_products(args.run_id)


if __name__ == "__main__":
    main()
