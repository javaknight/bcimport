"""
BigCommerce API client wrapper.
Handles SKU lookup, single POST (create), batch PUT (update), and rate limiting.
"""
import logging
import os
import time
from typing import Any

import bigc

log = logging.getLogger(__name__)

# BC standard plan: ~150 req / 30s. Conservative limit.
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 30  # seconds
BATCH_SIZE = 10


class BCClient:
    def __init__(self) -> None:
        store_hash = os.environ["BC_STORE_HASH"]
        access_token = os.environ["BC_ACCESS_TOKEN"]
        self._client = bigc.BigCommerceClient(
            store_hash=store_hash,
            access_token=access_token,
        )
        self._request_count = 0
        self._window_start = time.monotonic()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Pause if we are approaching the rate limit window."""
        self._request_count += 1
        elapsed = time.monotonic() - self._window_start
        if self._request_count >= RATE_LIMIT_REQUESTS:
            sleep_for = RATE_LIMIT_WINDOW - elapsed
            if sleep_for > 0:
                log.debug("Rate limit: sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)
            self._request_count = 0
            self._window_start = time.monotonic()

    # ------------------------------------------------------------------
    # SKU lookup — resolves existing BC product IDs by SKU
    # ------------------------------------------------------------------

    def lookup_skus(self, skus: list[str]) -> dict[str, int]:
        """
        Return a mapping of {sku: bc_id} for all SKUs that exist in BC.
        Paginates through results in chunks of 250.
        """
        found: dict[str, int] = {}
        chunk_size = 250

        for i in range(0, len(skus), chunk_size):
            chunk = skus[i : i + chunk_size]
            sku_csv = ",".join(chunk)
            self._throttle()
            response = self._client.v3.get(
                "/catalog/products",
                params={"sku:in": sku_csv, "limit": chunk_size, "include_fields": "id,sku"},
            )
            for product in response.get("data", []):
                found[product["sku"]] = product["id"]

        log.info("SKU lookup: %d/%d found in BC", len(found), len(skus))
        return found

    # ------------------------------------------------------------------
    # Create (POST) — one at a time
    # ------------------------------------------------------------------

    def create_product(self, payload: dict) -> tuple[int, dict]:
        """
        POST a single product. Returns (status_code, response_body).
        """
        self._throttle()
        try:
            response = self._client.v3.post("/catalog/products", json=payload)
            return 200, response
        except bigc.BigCommerceApiError as exc:
            return exc.status_code, {"errors": exc.errors}

    # ------------------------------------------------------------------
    # Update (batch PUT) — up to BATCH_SIZE at a time
    # ------------------------------------------------------------------

    def update_products(self, payloads: list[dict]) -> tuple[int, dict]:
        """
        PUT a batch of up to 10 products (each must include 'id').
        Returns (status_code, response_body).
        """
        assert len(payloads) <= BATCH_SIZE, f"Batch size must be <= {BATCH_SIZE}"
        self._throttle()
        try:
            response = self._client.v3.put("/catalog/products", json=payloads)
            return 200, response
        except bigc.BigCommerceApiError as exc:
            return exc.status_code, {"errors": exc.errors}

    # ------------------------------------------------------------------
    # Category helpers (used by prime_categories)
    # ------------------------------------------------------------------

    def get_categories(self) -> list[dict]:
        """Return all categories from BC."""
        self._throttle()
        response = self._client.v3.get("/catalog/categories", params={"limit": 250})
        return response.get("data", [])

    def create_category(self, name: str, parent_id: int = 0, is_visible: bool = False) -> int:
        """Create a category and return its BC ID."""
        self._throttle()
        response = self._client.v3.post(
            "/catalog/categories",
            json={"name": name, "parent_id": parent_id, "is_visible": is_visible},
        )
        return response["data"]["id"]

    def update_category(self, category_id: int, **fields) -> None:
        """Update arbitrary fields on a category by ID."""
        self._throttle()
        self._client.v3.put(f"/catalog/categories/{category_id}", json=fields)

    def delete_category(self, category_id: int) -> None:
        """Delete a category by ID. Raises on failure."""
        self._throttle()
        self._client.v3.delete(f"/catalog/categories/{category_id}")
