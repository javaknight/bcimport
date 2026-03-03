"""Lutron pricing enricher.

Reads the Lutron pricing spreadsheet and joins pricing columns onto the
XOlogic DataFrame by Item Number.

Added columns:
    Pricing-UPC        ← UPC Code
    Pricing-ListPrice  ← List Price  (→ BC retail_price)
    Pricing-MyPrice    ← My Price    (→ BC cost_price; price = MyPrice * PRICE_MARKUP)

Pricing file columns (row 1 = headers):
    Model Number, Item Name, Description, UPC Code, List Price,
    My Price, MAP, Estimated Lead Time (Days)
"""
import glob
import logging
import os

import pandas as pd

from enrichers.base_enricher import BaseEnricher

log = logging.getLogger(__name__)

# Mapping from pricing-sheet column names → internal Pricing- column names
_COLUMN_MAP = {
    "UPC Code":   "Pricing-UPC",
    "List Price": "Pricing-ListPrice",
    "My Price":   "Pricing-MyPrice",
}


class LutronPricingEnricher(BaseEnricher):
    """Joins Lutron pricing data onto the XOlogic feed by Item Number."""

    FILENAME_PATTERN = "lutron-pricing*.xlsx"

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        pricing_path = self._find_pricing_file()
        if pricing_path is None:
            log.warning(
                "LutronPricingEnricher: no file matching '%s' found in %s — "
                "pricing columns will be absent",
                self.FILENAME_PATTERN,
                self.feed_dir,
            )
            return df

        log.info("LutronPricingEnricher: loading %s", pricing_path)
        pricing_df = pd.read_excel(pricing_path, engine="calamine", dtype=str)

        # Keep only the join key + columns we care about
        keep = ["Model Number"] + list(_COLUMN_MAP.keys())
        missing = [c for c in keep if c not in pricing_df.columns]
        if missing:
            log.warning(
                "LutronPricingEnricher: pricing file missing expected columns: %s",
                missing,
            )
            keep = [c for c in keep if c in pricing_df.columns]

        pricing_df = pricing_df[keep].rename(columns=_COLUMN_MAP)

        # Normalise join key to string for a clean merge
        pricing_df["Model Number"] = pricing_df["Model Number"].astype(str).str.strip()
        df = df.copy()
        df["_item_str"] = df["Item Number"].astype(str).str.strip()

        before = len(df)
        df = df.merge(
            pricing_df,
            left_on="_item_str",
            right_on="Model Number",
            how="left",
        ).drop(columns=["_item_str", "Model Number"])

        matched = df["Pricing-UPC"].notna().sum() if "Pricing-UPC" in df.columns else 0
        log.info(
            "LutronPricingEnricher: %d/%d rows matched pricing data",
            matched,
            before,
        )
        return df

    def _find_pricing_file(self) -> str | None:
        if not self.feed_dir:
            return None
        pattern = os.path.join(self.feed_dir, self.FILENAME_PATTERN)
        matches = sorted(glob.glob(pattern))
        if not matches:
            return None
        if len(matches) > 1:
            log.warning(
                "LutronPricingEnricher: multiple pricing files found; using %s",
                matches[0],
            )
        return matches[0]
