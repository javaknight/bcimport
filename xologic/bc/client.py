"""
BigCommerce API client wrapper.
Handles SKU lookup, single POST (create), batch PUT (update), and rate limiting.
"""
import logging
import os
import time
from typing import Any

from bigc import BigCommerceAPI
from bigc.exceptions import BigCommerceException

log = logging.getLogger(__name__)

# BC standard plan: ~150 req / 30s. Conservative limit.
RATE_LIMIT_REQUESTS = 100
RATE_LIMIT_WINDOW = 30  # seconds
BATCH_SIZE = 10


class BCClient:
    def __init__(self) -> None:
        store_hash = os.environ["BC_STORE_HASH"]
        access_token = os.environ["BC_ACCESS_TOKEN"]
        self._client = BigCommerceAPI(
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
            for product in self._client.products_v3.all(
                params={"sku:in": sku_csv, "include_fields": "id,sku"},
            ):
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
            response = self._client.products_v3.create(data=payload)
            return 200, response
        except BigCommerceException as exc:
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
            response = self._client.api_v3.put("/catalog/products", data=payloads)
            return 200, response
        except BigCommerceException as exc:
            return exc.status_code, {"errors": exc.errors}

    # ------------------------------------------------------------------
    # Category helpers (used by prime_categories)
    # ------------------------------------------------------------------

    def get_categories(self) -> list[dict]:
        """Return all categories from BC."""
        self._throttle()
        return list(self._client.categories_v3.all())

    def create_category(
        self,
        name: str,
        parent_id: int = 0,
        is_visible: bool = False,
        tree_id: int | None = None,
    ) -> int:
        """Create a category and return its BC ID."""
        self._throttle()
        payload: dict[str, Any] = {"name": name, "parent_id": parent_id, "is_visible": is_visible}
        if tree_id is not None:
            payload["tree_id"] = tree_id
        response = self._client.categories_v3.create(data=payload)
        return response["id"]

    def update_category(self, category_id: int, **fields) -> None:
        """Update arbitrary fields on a category by ID."""
        self._throttle()
        self._client.categories_v3.update(category_id, data=fields)

    def delete_category(self, category_id: int) -> None:
        """Delete a category by ID. Raises on failure."""
        self._throttle()
        self._client.categories_v3.delete(category_id)
