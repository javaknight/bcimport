"""Abstract base class for feed enrichers."""
import logging
from abc import ABC, abstractmethod

import pandas as pd

log = logging.getLogger(__name__)


class BaseEnricher(ABC):
    """Reads a supplemental data source and merges new columns onto the feed DataFrame.

    Enriched columns should use a vendor-specific prefix (e.g. 'Pricing-') to
    avoid shadowing XOlogic columns.  If a conflict does occur, a warning is logged.
    """

    @abstractmethod
    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge supplemental data onto df and return the enriched DataFrame."""
