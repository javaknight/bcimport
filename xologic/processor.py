"""
Main orchestrator for XOlogic → BigCommerce product import.

Usage:
    python processor.py --feed-dir input/
    python processor.py --feed path/to/feed.xlsx
"""
import argparse
import fnmatch
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from bc.client import BCClient, BATCH_SIZE
from mappers import get_mapper, get_mapper_class
from readers.xlsx_reader import load_feed
from utilities.pdf_mirror import mirror_feed_pdfs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
ERROR_FILE = os.path.join(OUTPUT_DIR, "error_items.xlsx")
WARNINGS_FILE = os.path.join(OUTPUT_DIR, "warnings.xlsx")


def find_feed(feed_dir: str, enricher_classes: list | None = None) -> str:
    """Return the product feed .xlsx in feed_dir.

    Files matching any enricher's FILENAME_PATTERN are excluded so they are
    never mistaken for the product feed. Raises if zero or multiple candidates
    remain after exclusion.
    """
    all_xlsx = sorted(Path(feed_dir).glob("*.xlsx"))
    if not all_xlsx:
        raise FileNotFoundError(f"No .xlsx files found in {feed_dir}")

    enricher_patterns = [
        getattr(cls, "FILENAME_PATTERN", None)
        for cls in (enricher_classes or [])
    ]
    enricher_patterns = [p for p in enricher_patterns if p]

    candidates = [
        p for p in all_xlsx
        if not any(fnmatch.fnmatch(p.name, pat) for pat in enricher_patterns)
    ]

    excluded = set(all_xlsx) - set(candidates)
    for p in excluded:
        log.info("find_feed: ignoring enricher file %s", p.name)

    if not candidates:
        raise FileNotFoundError(f"No product feed found in {feed_dir} (all .xlsx files are enricher inputs)")
    if len(candidates) > 1:
        names = [p.name for p in candidates]
        raise FileNotFoundError(
            f"Ambiguous product feed in {feed_dir} — {len(candidates)} unrecognised files: {names}. "
            "Remove extras or pass --feed explicitly."
        )
    return str(candidates[0])


def write_error_report(rows: list[dict], path: str) -> None:
    """Write failed rows to an xlsx that can be fixed and re-fed."""
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_excel(path, index=False, engine="openpyxl")
    log.info("Wrote error report: %s (%d rows)", path, len(df))


def write_warnings_report(rows: list[dict], path: str) -> None:
    """Write 207 partial-success rows to xlsx for human review."""
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_excel(path, index=False, engine="openpyxl")
    log.info("Wrote warnings report: %s (%d rows)", path, len(df))


def run(
    feed_path: str,
    vendor: str,
    limit: int | None = None,
    skus: list[str] | None = None,
    update_categories: bool = False,
) -> None:
    start = time.time()
    log.info("Starting import from %s", feed_path)

    mapper = get_mapper(vendor)
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    log.info(
        "Run ID: %s  Vendor: %s  Channels: %s  Update categories: %s",
        run_id,
        vendor,
        mapper.channel_ids,
        update_categories,
    )

    feed_df = load_feed(feed_path, mapper.VENDOR_ID, mapper.PRODUCT_TYPES)

    # Run enrichers (in-memory join of supplemental data sources)
    feed_dir = os.path.dirname(os.path.abspath(feed_path))
    for enricher_cls in mapper.ENRICHERS:
        enricher = enricher_cls(feed_dir=feed_dir)
        log.info("Running enricher: %s", enricher_cls.__name__)
        feed_df = enricher.enrich(feed_df)

    if skus:
        feed_df = feed_df[feed_df["Item Number"].astype(str).isin([str(s) for s in skus])]
        log.info("--sku filter applied: %d row(s) matched", len(feed_df))
    if limit is not None:
        feed_df = feed_df.head(limit)
        log.info("--limit %d applied", limit)

    link_columns = list(getattr(mapper, "PDF_LINK_COLUMNS", []))
    dav_subdir = getattr(mapper, "PDF_DAV_SUBDIR", None)
    if dav_subdir and link_columns:
        log.info(
            "Mirroring PDF assets to BC WebDAV for %d row(s) using /content/%s",
            len(feed_df),
            dav_subdir,
        )
        mirror_feed_pdfs(
            feed_df=feed_df,
            link_columns=link_columns,
            dav_subdir=dav_subdir,
        )

    # --- Phase 0: handle human-created (skipped) items ---
    # These are not imported via our SKU scheme; instead we patch their pricing
    # on the existing BC product found via MPN lookup.
    client = BCClient()
    skip_item_numbers = getattr(mapper, "SKIP_ITEM_NUMBERS", frozenset())
    if skip_item_numbers:
        skip_mask = feed_df["Item Number"].astype(str).isin(skip_item_numbers)
        skip_df = feed_df[skip_mask]
        feed_df = feed_df[~skip_mask].reset_index(drop=True)
        if not skip_df.empty:
            log.info(
                "%d human-created item(s) found in feed — skipping import, patching pricing via MPN",
                len(skip_df),
            )
            _patch_human_pricing(skip_df, mapper, client)

    total = len(feed_df)
    log.info("Feed loaded: %d rows after filtering", total)

    if total == 0:
        log.warning("No rows to process; exiting.")
        return

    # --- Phase 1: map rows to payloads ---
    payloads: list[dict] = []
    raw_rows: list[dict] = []
    map_errors: list[dict] = []

    for _, row in feed_df.iterrows():
        try:
            payload = mapper.map_row(row)
            payloads.append(payload)
            raw_rows.append(row.to_dict())
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("map_row failed for row %s: %s", row.get("Item Number"), exc)
            err = row.to_dict()
            err["bc_status_code"] = "MAP_ERROR"
            err["bc_error_message"] = str(exc)
            map_errors.append(err)

    log.info("Mapped %d/%d rows (%d mapping errors)", len(payloads), total, len(map_errors))

    # --- Phase 2: SKU lookup to split creates vs updates ---
    skus = [p["sku"] for p in payloads]
    sku_to_id = client.lookup_skus(skus)
    log.info("SKU lookup: %d existing products found", len(sku_to_id))

    creates: list[tuple[dict, dict]] = []   # (payload, raw_row)
    updates: list[tuple[dict, dict]] = []   # (payload, raw_row)

    for payload, raw in zip(payloads, raw_rows):
        sku = payload["sku"]
        if sku in sku_to_id:
            payload["id"] = sku_to_id[sku]
            payload.pop("is_visible", None)  # never override visibility on updates
            if not update_categories:
                payload.pop("categories", None)  # preserve human-recategorized BC categories
            updates.append((payload, raw))
        else:
            creates.append((payload, raw))

    log.info("%d creates, %d updates", len(creates), len(updates))

    n_success = 0
    n_warnings = 0
    n_errors = 0
    error_rows: list[dict] = [*map_errors]
    warning_rows: list[dict] = []

    # --- Phase 3: creates (POST one at a time) ---
    for payload, raw in creates:
        status, body = client.create_product(payload)
        if status == 200:
            n_success += 1
            product_id = body["id"]
            try:
                client.create_product_metafield(product_id, "bcimport", "awsbatch", run_id)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning("Metafield creation failed for product %d: %s", product_id, exc)
            feed_image_urls = sorted(
                img["image_url"]
                for img in payload.get("images", [])
                if img.get("image_url")
            )
            if feed_image_urls:
                try:
                    client.create_product_metafield(
                        product_id, "bcimport", "image_urls",
                        json.dumps(feed_image_urls),
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    log.warning("image_urls metafield creation failed for product %d: %s", product_id, exc)
            for ch_id in mapper.channel_ids:
                try:
                    client.assign_products_to_channel([product_id], ch_id)
                except Exception as exc:  # pylint: disable=broad-except
                    log.warning("Channel assignment failed for product %d channel %d: %s", product_id, ch_id, exc)
        elif status == 207:
            n_warnings += 1
            warn = raw.copy()
            warn["bc_status_code"] = status
            warn["bc_error_message"] = _extract_message(body)
            warning_rows.append(warn)
        else:  # 409, 422, other errors
            n_errors += 1
            err = raw.copy()
            err["bc_status_code"] = status
            err["bc_error_message"] = _extract_message(body)
            error_rows.append(err)

    # --- Phase 3.5: reconcile custom fields + image URL tracking for updates ---
    # One combined GET per product fetches custom_fields, images, and the
    # bcimport/image_urls metafield.
    #
    # custom_fields: inject existing IDs so BC treats them as updates, not creates.
    #
    # images: compare stored URL set (metafield) against feed URL set:
    #   - identical  → skip images in payload (no-op, no duplicate)
    #   - different  → delete all existing BC images now; keep images in payload
    #                  so BC re-uploads them; queue a metafield write for after
    #                  the batch PUT succeeds.
    #   - no metafield yet (first update after the fix) → treat as "changed"
    image_url_changes: dict[int, dict] = {}  # product_id → {metafield_id, new_value}
    image_desc_patches: dict[int, list[tuple[int, str]]] = {}  # product_id → [(image_id, description)]
    awsbatch_missing: set[int] = set()        # product_ids where awsbatch metafield is absent

    if updates:
        log.info("Fetching existing product data for %d update product(s)…", len(updates))
        for payload, _ in updates:
            product_id = payload["id"]
            try:
                existing = client.get_product_for_update(product_id)

                if payload.get("custom_fields"):
                    payload["custom_fields"] = _reconcile_custom_fields(
                        payload["custom_fields"], existing["custom_fields"]
                    )

                if existing.get("awsbatch_metafield") is None:
                    awsbatch_missing.add(product_id)

                feed_image_urls = sorted(
                    img["image_url"]
                    for img in payload.get("images", [])
                    if img.get("image_url")
                )
                mf = existing.get("image_urls_metafield")
                stored_image_urls = sorted(json.loads(mf["value"])) if mf else []

                if feed_image_urls == stored_image_urls:
                    # URLs unchanged — skip payload images to avoid BC appending duplicates.
                    # But still patch alt-text (description) on existing BC images if stale.
                    expected_desc = payload.get("name", "")
                    patches = [
                        (bc_img["id"], expected_desc)
                        for bc_img in existing["images"]
                        if bc_img.get("description") != expected_desc
                    ]
                    if patches:
                        image_desc_patches[product_id] = patches
                    payload.pop("images", None)
                elif feed_image_urls:
                    # URLs changed — delete existing BC images now, re-upload via payload
                    for bc_img in existing["images"]:
                        try:
                            client.delete_product_image(product_id, bc_img["id"])
                        except Exception as exc:  # pylint: disable=broad-except
                            log.warning(
                                "Could not delete image %d for product %d: %s",
                                bc_img["id"], product_id, exc,
                            )
                    image_url_changes[product_id] = {
                        "metafield_id": mf["id"] if mf else None,
                        "new_value": json.dumps(sorted(feed_image_urls)),
                    }

            except Exception as exc:  # pylint: disable=broad-except
                log.warning(
                    "Could not fetch product data for %d: %s — "
                    "sending without reconciliation (custom fields may fail; images may duplicate)",
                    product_id,
                    exc,
                )

    # --- Phase 4: updates (batch PUT, max BATCH_SIZE per request) ---
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i : i + BATCH_SIZE]
        batch_payloads = [p for p, _ in batch]
        batch_raws = [r for _, r in batch]

        status, body = client.update_products(batch_payloads)

        if status == 200:
            n_success += len(batch)
            for p, _ in batch:
                pid = p["id"]
                if pid in awsbatch_missing:
                    try:
                        client.create_product_metafield(pid, "bcimport", "awsbatch", run_id)
                    except Exception as exc:  # pylint: disable=broad-except
                        log.warning("awsbatch backfill failed for product %d: %s", pid, exc)
                if pid in image_url_changes:
                    change = image_url_changes[pid]
                    try:
                        if change["metafield_id"] is not None:
                            client.update_product_metafield(
                                pid, change["metafield_id"], change["new_value"]
                            )
                        else:
                            client.create_product_metafield(
                                pid, "bcimport", "image_urls", change["new_value"]
                            )
                    except Exception as exc:  # pylint: disable=broad-except
                        log.warning(
                            "image_urls metafield update failed for product %d: %s",
                            pid, exc,
                        )
                if pid in image_desc_patches:
                    for img_id, desc in image_desc_patches[pid]:
                        try:
                            client.update_product_image(pid, img_id, description=desc)
                        except Exception as exc:  # pylint: disable=broad-except
                            log.warning(
                                "Image alt-text update failed for product %d image %d: %s",
                                pid, img_id, exc,
                            )
        elif status == 207:
            # Partial success — determine per-item results if available
            results = body.get("data", []) if isinstance(body, dict) else []
            if results:
                id_to_raw = {p["id"]: r for p, r in zip(batch_payloads, batch_raws)}
                for item in results:
                    item_id = item.get("id")
                    item_status = item.get("status", 207)
                    raw = id_to_raw.get(item_id, {})
                    if item_status == 200:
                        n_success += 1
                    elif item_status == 207:
                        n_warnings += 1
                        warn = raw.copy()
                        warn["bc_status_code"] = item_status
                        warn["bc_error_message"] = _extract_message(item)
                        warning_rows.append(warn)
                    else:
                        n_errors += 1
                        err = raw.copy()
                        err["bc_status_code"] = item_status
                        err["bc_error_message"] = _extract_message(item)
                        error_rows.append(err)
            else:
                # No per-item breakdown: flag entire batch as warnings
                n_warnings += len(batch)
                for raw in batch_raws:
                    warn = raw.copy()
                    warn["bc_status_code"] = 207
                    warn["bc_error_message"] = _extract_message(body)
                    warning_rows.append(warn)
        else:
            n_errors += len(batch)
            for raw in batch_raws:
                err = raw.copy()
                err["bc_status_code"] = status
                err["bc_error_message"] = _extract_message(body)
                error_rows.append(err)

    # --- Phase 5: reports ---
    write_error_report(error_rows, ERROR_FILE)
    write_warnings_report(warning_rows, WARNINGS_FILE)

    elapsed = time.time() - start
    mins, secs = divmod(int(elapsed), 60)
    processed = n_success + n_warnings + n_errors
    log.info(
        "Processed %d/%d: %d success, %d warnings (207), %d errors. Elapsed: %dm %ds. Run ID: %s",
        processed,
        total,
        n_success,
        n_warnings,
        n_errors,
        mins,
        secs,
        run_id,
    )


def _patch_human_pricing(skip_df, mapper, client: BCClient) -> None:
    """Patch price/cost on human-created BC products found via MPN lookup."""
    patched = skipped = failed = 0
    for _, row in skip_df.iterrows():
        item_number = str(row["Item Number"])
        pricing = mapper.build_price_patch(row)
        if pricing is None:
            log.warning("No pricing computable for skipped item %s — skipping patch", item_number)
            skipped += 1
            continue
        try:
            product_id = client.lookup_by_mpn(item_number)
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("MPN lookup failed for %s: %s", item_number, exc)
            failed += 1
            continue
        if product_id is None:
            log.info("Skipped item %s not found in BC via MPN — nothing to patch", item_number)
            skipped += 1
            continue
        try:
            client.patch_product_pricing(product_id, pricing["price"], pricing["cost_price"])
            log.info("Patched pricing for %s (BC id %d): price=%.2f cost=%.2f",
                     item_number, product_id, pricing["price"], pricing["cost_price"])
            patched += 1
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Pricing patch failed for %s (BC id %d): %s", item_number, product_id, exc)
            failed += 1
    log.info("Human-created pricing patch: %d patched, %d not in BC, %d failed",
             patched, skipped, failed)


def _reconcile_custom_fields(
    payload_fields: list[dict], existing_fields: list[dict]
) -> list[dict]:
    """
    Merge custom field IDs from BC into the payload.

    BC's batch PUT treats custom_fields entries without an `id` as creates and
    rejects them if a field with the same name already exists on the product.
    Entries *with* a matching `id` are treated as updates.

    For each field in the payload:
      - If a field with the same name exists in BC, inject its `id`.
      - Otherwise, leave as-is (new field to be created).
    """
    name_to_id = {f["name"]: f["id"] for f in existing_fields}
    reconciled = []
    for field in payload_fields:
        entry = dict(field)
        if field["name"] in name_to_id:
            entry["id"] = name_to_id[field["name"]]
        reconciled.append(entry)
    return reconciled


def _extract_message(body) -> str:
    """Best-effort extraction of an error message from a BC response body."""
    if isinstance(body, dict):
        return (
            body.get("title")
            or body.get("detail")
            or body.get("message")
            or str(body)
        )
    return str(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="XOlogic → BigCommerce importer")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--feed", help="Path to XLSX feed file")
    group.add_argument("--feed-dir", help="Directory containing XLSX feed file(s)")
    parser.add_argument("--vendor", required=True, help="Vendor name (e.g. lutron)")
    parser.add_argument("--limit", type=int, default=None, help="Cap rows processed (useful for testing)")
    parser.add_argument("--sku", nargs="+", metavar="ITEM_NUMBER", help="Filter to specific Item Numbers from the feed")
    parser.add_argument(
        "--update-categories",
        action="store_true",
        help="Allow update payloads to include categories. Default behavior preserves existing BC categories on updates.",
    )
    args = parser.parse_args()

    feed_path = args.feed if args.feed else find_feed(
        args.feed_dir,
        enricher_classes=get_mapper_class(args.vendor).ENRICHERS,
    )
    run(
        feed_path,
        vendor=args.vendor,
        limit=args.limit,
        skus=args.sku,
        update_categories=args.update_categories,
    )


if __name__ == "__main__":
    main()
