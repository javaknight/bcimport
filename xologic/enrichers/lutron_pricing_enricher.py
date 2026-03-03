"""Lutron pricing enricher (stub).

Reads the Lutron pricing spreadsheet and joins pricing columns onto the
XOlogic DataFrame by Item Number.

Added columns (prefixed 'Pricing-' to avoid collision with XOlogic columns):
    Pricing-UnitCost, Pricing-MSRP, Pricing-UPC  (others TBD)

TODO: implement when pricing file format is confirmed.
"""
import logging

import pandas as pd

from enrichers.base_enricher import BaseEnricher

log = logging.getLogger(__name__)


class LutronPricingEnricher(BaseEnricher):
    """Joins Lutron pricing data onto the XOlogic feed by Item Number."""

    FILENAME_PATTERN = "lutron_pricing*.xlsx"

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        # TODO: locate file matching FILENAME_PATTERN in input dir,
        #       read it, merge on Item Number, prefix columns with 'Pricing-'
        log.debug("LutronPricingEnricher: not yet implemented — skipping")
        return df
