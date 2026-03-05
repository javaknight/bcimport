"""CLI wrapper for mirroring vendor PDF assets to BigCommerce WebDAV."""
import argparse
import importlib
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _pdf_mirror_module():
    return importlib.import_module("utilities.pdf_mirror")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def mirror(vendor: str, feed_dir: str, dry_run: bool = False) -> None:
    pdf_mirror = _pdf_mirror_module()
    mapper_mod = importlib.import_module(f"mappers.{vendor}_mapper")
    mapper_cls = getattr(mapper_mod, f"{vendor.capitalize()}Mapper")
    mapper = mapper_cls()

    link_columns = list(getattr(mapper, "PDF_LINK_COLUMNS", []))
    dav_subdir = getattr(mapper, "PDF_DAV_SUBDIR", None)
    if not dav_subdir or not link_columns:
        raise ValueError(
            f"Mapper for vendor {vendor!r} has no PDF mirror configuration"
        )

    candidates = sorted(Path(feed_dir).glob("*.xlsx"))
    if not candidates:
        raise FileNotFoundError(f"No .xlsx feed files found in {feed_dir}")
    if len(candidates) > 1:
        log.warning("Multiple feeds found in %s; using %s", feed_dir, candidates[0])
    feed_path = str(candidates[0])

    feed_df = pdf_mirror.load_filtered_feed(feed_path, mapper.VENDOR_ID, mapper.PRODUCT_TYPES)
    log.info("Feed rows after filter: %d", len(feed_df))

    pdf_mirror.mirror_feed_pdfs(
        feed_df=feed_df,
        link_columns=link_columns,
        dav_subdir=dav_subdir,
        dry_run=dry_run,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Mirror vendor PDFs to BC WebDAV")
    parser.add_argument("--vendor", required=True, help="Vendor name, e.g. lutron")
    parser.add_argument(
        "--feed-dir",
        default="input/",
        help="Directory containing the feed XLSX (default: input/)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading",
    )
    args = parser.parse_args()
    mirror(args.vendor, args.feed_dir, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
