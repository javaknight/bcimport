"""
POC: verify BigCommerce cross-product metafield search.

Usage:
    python tools/poc_metafield_search.py --product-id <BC_PRODUCT_ID>

The script:
1. Checks whether an existing awsbatch metafield is already on the product.
   - If found: uses its value for the search; does NOT delete it afterwards.
   - If not found: creates a temporary metafield (value=poc-test), searches,
     then cleans up.
2. Calls GET /v3/catalog/products/metafields?namespace=bcimport&key=awsbatch&value=<value>
3. Validates the product appears in the results.

Requires BC_STORE_HASH and BC_ACCESS_TOKEN in the environment (source an .env file first).
"""

import argparse
import os
import sys

from bigc import BigCommerceAPI
from bigc.exceptions import BigCommerceException

NAMESPACE = "bcimport"
KEY = "awsbatch"
POC_VALUE = "poc-test"


def main() -> None:
    parser = argparse.ArgumentParser(description="POC: BC cross-product metafield search")
    parser.add_argument("--product-id", type=int, required=True, help="BC product ID to inspect/test")
    args = parser.parse_args()
    product_id: int = args.product_id

    client = BigCommerceAPI(
        store_hash=os.environ["BC_STORE_HASH"],
        access_token=os.environ["BC_ACCESS_TOKEN"],
    )

    # 1. Check for an existing metafield on this product
    print(f"[1] Checking for existing '{NAMESPACE}/{KEY}' metafield on product {product_id} ...")
    try:
        existing = list(
            client.api_v3.get_many(
                f"/catalog/products/{product_id}/metafields",
                params={"namespace": NAMESPACE, "key": KEY},
            )
        )
    except BigCommerceException as exc:
        print(f"ERROR fetching product metafields: {exc}")
        sys.exit(1)

    created_mf_id: int | None = None

    if existing:
        mf = existing[0]
        search_value = mf["value"]
        print(f"    Found existing metafield id={mf['id']}  value={search_value!r}")
        print("    Skipping creation; will search using this value.")
    else:
        print(f"    None found. Creating temporary metafield (value={POC_VALUE!r}) ...")
        try:
            mf = client.api_v3.post(
                f"/catalog/products/{product_id}/metafields",
                data={
                    "namespace": NAMESPACE,
                    "key": KEY,
                    "value": POC_VALUE,
                    "permission_set": "read",
                },
            )
        except BigCommerceException as exc:
            print(f"ERROR creating metafield: {exc}")
            sys.exit(1)
        created_mf_id = mf["id"]
        search_value = POC_VALUE
        print(f"    Created metafield id={created_mf_id}")

    # 2. Cross-product search
    print(f"\n[2] Searching across all products: namespace={NAMESPACE!r} key={KEY!r} value={search_value!r} ...")
    try:
        results = list(
            client.api_v3.get_many(
                "/catalog/products/metafields",
                params={"namespace": NAMESPACE, "key": KEY, "value": search_value},
            )
        )
    except BigCommerceException as exc:
        print(f"ERROR searching metafields: {exc}")
        results = []

    print(f"    Found {len(results)} result(s) total")

    matched = [r for r in results if r.get("resource_id") == product_id]
    if matched:
        print(f"    PASS — product {product_id} appears in results: {matched[0]}")
    else:
        print(f"    FAIL — product {product_id} NOT found in results")
        for r in results:
            print(f"      {r}")

    # 3. Cleanup (only if we created the metafield)
    if created_mf_id is not None:
        print(f"\n[3] Deleting temporary metafield id={created_mf_id} ...")
        try:
            client.api_v3.delete(f"/catalog/products/{product_id}/metafields/{created_mf_id}")
            print("    Deleted OK")
        except BigCommerceException as exc:
            print(f"    WARNING: could not delete metafield: {exc}")
    else:
        print("\n[3] Skipping cleanup (metafield was pre-existing).")

    print("\nDone.")


if __name__ == "__main__":
    main()
