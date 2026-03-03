"""
Delete all products from a specific import run.

Searches for products tagged with the given run ID (bcimport/awsbatch metafield)
and deletes them from BigCommerce. Requires explicit --confirm flag.

Usage:
    python tools/delete_run_products.py --run-id 20260303-131158 --confirm
"""
import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bigc import BigCommerceAPI
from bc.client import BCClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def delete_run_products(run_id: str) -> None:
    start = time.time()
    client = BCClient()

    log.info("Searching for products with run ID: %s", run_id)
    metafields = client.search_products_by_metafield("bcimport", "awsbatch", run_id)
    product_ids = [mf["resource_id"] for mf in metafields]

    if not product_ids:
        log.warning("No products found for run ID: %s", run_id)
        return

    log.info("Found %d product(s) — deleting", len(product_ids))

    store_hash = os.environ["BC_STORE_HASH"]
    access_token = os.environ["BC_ACCESS_TOKEN"]
    bc = BigCommerceAPI(store_hash=store_hash, access_token=access_token)

    n_success = 0
    n_errors = 0

    for pid in product_ids:
        client._throttle()
        try:
            bc.products_v3.delete(pid)
            n_success += 1
            log.debug("Deleted product %d", pid)
        except Exception as exc:  # pylint: disable=broad-except
            n_errors += 1
            log.error("Failed to delete product %d: %s", pid, exc)

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    log.info(
        "Deleted %d/%d products. %d errors. Elapsed: %dm %ds",
        n_success, len(product_ids), n_errors, mins, secs,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Delete all products from a given import run (emergency rollback)"
    )
    parser.add_argument(
        "--run-id", required=True,
        help="Run ID from the import log (e.g. 20260303-131158)",
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="Required: confirm you intend to permanently delete these products",
    )
    args = parser.parse_args()

    if not args.confirm:
        print(
            f"This will permanently delete all products from run {args.run_id}.\n"
            f"Re-run with --confirm to proceed."
        )
        sys.exit(1)

    delete_run_products(args.run_id)


if __name__ == "__main__":
    main()
