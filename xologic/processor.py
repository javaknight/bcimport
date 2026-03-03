"""
Main orchestrator for XOlogic → BigCommerce product import.

Usage:
    python processor.py --feed-dir input/
    python processor.py --feed path/to/feed.xlsx
"""
import argparse
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from bc.client import BCClient, BATCH_SIZE
from mappers.field_mapper import map_row
from readers.xlsx_reader import load_feed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
ERROR_FILE = os.path.join(OUTPUT_DIR, "error_items.xlsx")
WARNINGS_FILE = os.path.join(OUTPUT_DIR, "warnings.xlsx")


def find_feed(feed_dir: str) -> str:
    """Return the first .xlsx file in feed_dir."""
    candidates = sorted(Path(feed_dir).glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"No .xlsx files found in {feed_dir}")
    if len(candidates) > 1:
        log.warning("Multiple feeds found in %s; using %s", feed_dir, candidates[0])
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


def run(feed_path: str, limit: int | None = None, skus: list[str] | None = None) -> None:
    start = time.time()
    log.info("Starting import from %s", feed_path)

    channel_id = int(os.environ["CHANNEL_ID"])
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    log.info("Run ID: %s  Channel: %d", run_id, channel_id)

    feed_df = load_feed(feed_path)
    if skus:
        feed_df = feed_df[feed_df["Item Number"].astype(str).isin([str(s) for s in skus])]
        log.info("--sku filter applied: %d row(s) matched", len(feed_df))
    if limit is not None:
        feed_df = feed_df.head(limit)
        log.info("--limit %d applied", limit)
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
            payload = map_row(row)
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
    client = BCClient()
    skus = [p["sku"] for p in payloads]
    sku_to_id = client.lookup_skus(skus)
    log.info("SKU lookup: %d existing products found", len(sku_to_id))

    creates: list[tuple[dict, dict]] = []   # (payload, raw_row)
    updates: list[tuple[dict, dict]] = []   # (payload, raw_row)

    for payload, raw in zip(payloads, raw_rows):
        sku = payload["sku"]
        if sku in sku_to_id:
            payload["id"] = sku_to_id[sku]
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
            try:
                client.assign_products_to_channel([product_id], channel_id)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning("Channel assignment failed for product %d: %s", product_id, exc)
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

    # --- Phase 4: updates (batch PUT, max BATCH_SIZE per request) ---
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i : i + BATCH_SIZE]
        batch_payloads = [p for p, _ in batch]
        batch_raws = [r for _, r in batch]

        status, body = client.update_products(batch_payloads)

        if status == 200:
            n_success += len(batch)
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
        "Processed %d/%d: %d success, %d warnings (207), %d errors. Elapsed: %dm %ds",
        processed,
        total,
        n_success,
        n_warnings,
        n_errors,
        mins,
        secs,
    )


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
    parser.add_argument("--limit", type=int, default=None, help="Cap rows processed (useful for testing)")
    parser.add_argument("--sku", nargs="+", metavar="ITEM_NUMBER", help="Filter to specific Item Numbers from the feed")
    args = parser.parse_args()

    feed_path = args.feed if args.feed else find_feed(args.feed_dir)
    run(feed_path, limit=args.limit, skus=args.sku)


if __name__ == "__main__":
    main()
