"""
Reads and filters the XOlogic XLSX feed.
"""
import logging
import os

import pandas as pd

log = logging.getLogger(__name__)

VENDOR_ID = 4460
PRODUCT_TYPES = {0, 1, 4}


def load_feed(path: str) -> pd.DataFrame:
    """
    Read the XOlogic XLSX feed and return rows matching VendorID and Product Type filters.
    Raises FileNotFoundError if the file does not exist.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Feed file not found: {path}")

    log.info("Reading feed: %s", path)
    df = pd.read_excel(path, dtype_backend="numpy_nullable")
    total = len(df)
    log.info("Total rows in feed: %d", total)

    df = df[
        (df["VendorID"] == VENDOR_ID) &
        (df["Product Type"].isin(PRODUCT_TYPES))
    ].reset_index(drop=True)

    log.info("Rows after filter (VendorID=%d, Product Type in %s): %d", VENDOR_ID, PRODUCT_TYPES, len(df))
    return df
