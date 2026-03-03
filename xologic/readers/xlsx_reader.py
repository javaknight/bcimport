"""
Reads and filters the XOlogic XLSX feed for a given vendor.
"""
import logging
import os

import pandas as pd

log = logging.getLogger(__name__)


def load_feed(path: str, vendor_id: int, product_types: set[int]) -> pd.DataFrame:
    """
    Read the XOlogic XLSX feed and return rows matching the given vendor
    and product type filters.
    Raises FileNotFoundError if the file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Feed file not found: {path}")

    log.info("Reading feed: %s", path)
    df = pd.read_excel(path, dtype_backend="numpy_nullable")
    total = len(df)
    log.info("Total rows in feed: %d", total)

    df = df[
        (df["VendorID"] == vendor_id) &
        (df["Product Type"].isin(product_types))
    ].reset_index(drop=True)

    log.info(
        "Rows after filter (VendorID=%d, Product Type in %s): %d",
        vendor_id, product_types, len(df),
    )
    return df
