"""Tests for LutronMapper pricing logic (MAP / fallback to markup)."""
import pandas as pd
import pytest

import vendors.lutron as lutron_cfg
from mappers.lutron_mapper import LutronMapper

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_DUMMY_IMAGE = [{"url": "https://example.com/img.jpg", "is_thumbnail": True}]
_DUMMY_CATEGORIES = [1, 2]
_DUMMY_CATEGORY_MAP = {"Lutron": 1, "Lightswitches": 2}


def _base_row(**overrides) -> pd.Series:
    """Minimal feed row that satisfies LutronMapper.map_row."""
    data = {
        "Item Number": "MA-AS-WH",
        "Item Name": "Test Item",
        "Short Description": "A test product",
        "Extra-Weight": "1.0 LB",
        "GTIN": None,
        "Width": None,
        "Height": None,
        "Standard-Subcategory": "Lightswitches",
        "Standard-Finish": None,
        "Variant-Finish": None,
        "Standard-Style": None,
        "Extra-Length": None,
        "Image Path": None,
        "Pricing-UPC": None,
        "Pricing-MyPrice": None,
        "Pricing-MAP": None,
    }
    data.update(overrides)
    return pd.Series(data)


@pytest.fixture()
def mapper(monkeypatch) -> LutronMapper:
    """LutronMapper with images and categories stubbed out."""
    m = LutronMapper()
    monkeypatch.setattr(m, "_build_images", lambda row: _DUMMY_IMAGE)
    monkeypatch.setattr(m, "_build_categories", lambda row: _DUMMY_CATEGORIES)
    monkeypatch.setattr(m, "_get_category_map", lambda: _DUMMY_CATEGORY_MAP)
    return m


# ---------------------------------------------------------------------------
# map_row: pricing
# ---------------------------------------------------------------------------

class TestMapRowPricing:
    def test_map_present_sets_price_and_map_price(self, mapper):
        row = _base_row(**{"Pricing-MyPrice": "100.00", "Pricing-MAP": "150.00"})
        payload = mapper.map_row(row)
        assert payload["price"] == 150.0
        assert payload["cost_price"] == 100.0
        assert "map_price" not in payload

    def test_map_absent_falls_back_to_markup(self, mapper):
        row = _base_row(**{"Pricing-MyPrice": "100.00", "Pricing-MAP": None})
        payload = mapper.map_row(row)
        expected_price = round(100.0 * lutron_cfg.PRICE_MARKUP, 2)
        assert payload["price"] == expected_price
        assert "map_price" not in payload

    def test_map_zero_treated_as_absent(self, mapper):
        """MAP=0 means no restriction; fall back to My Price * markup."""
        row = _base_row(**{"Pricing-MyPrice": "100.00", "Pricing-MAP": "0"})
        payload = mapper.map_row(row)
        expected_price = round(100.0 * lutron_cfg.PRICE_MARKUP, 2)
        assert payload["price"] == expected_price
        assert "map_price" not in payload

    def test_no_pricing_at_all_raises(self, mapper):
        row = _base_row(**{"Pricing-MyPrice": None, "Pricing-MAP": None})
        with pytest.raises(ValueError, match="No pricing data"):
            mapper.map_row(row)


# ---------------------------------------------------------------------------
# build_price_patch: pricing for human-created products
# ---------------------------------------------------------------------------

class TestBuildPricePatch:
    def _mapper(self) -> LutronMapper:
        return LutronMapper()

    def test_map_present_sets_price_and_map_price(self):
        row = _base_row(**{"Pricing-MyPrice": "200.00", "Pricing-MAP": "250.00"})
        patch = self._mapper().build_price_patch(row)
        assert patch == {"price": 250.0, "cost_price": 200.0}

    def test_map_absent_falls_back_to_markup(self):
        row = _base_row(**{"Pricing-MyPrice": "200.00", "Pricing-MAP": None})
        patch = self._mapper().build_price_patch(row)
        expected = round(200.0 * lutron_cfg.PRICE_MARKUP, 2)
        assert patch == {"price": expected, "cost_price": 200.0}
        assert "map_price" not in patch

    def test_no_my_price_returns_none(self):
        row = _base_row(**{"Pricing-MyPrice": None, "Pricing-MAP": "250.00"})
        assert self._mapper().build_price_patch(row) is None
