"""Tests for base_mapper helpers: build_sku, _str, _num, _add_field, channel_ids."""
import os

import pandas as pd
import pytest

from mappers.base_mapper import (
    MAX_SKU_LENGTH,
    _add_field,
    _num,
    _str,
    build_sku,
)


# ------------------------------------------------------------------
# build_sku
# ------------------------------------------------------------------

class TestBuildSku:
    def test_simple(self):
        assert build_sku("LU-", "MA-AS-WH") == "LU-MA-AS-WH"

    def test_at_exact_limit(self):
        # prefix + item number == MAX_SKU_LENGTH exactly
        item = "X" * (MAX_SKU_LENGTH - len("LU-"))
        assert len(build_sku("LU-", item)) == MAX_SKU_LENGTH

    def test_strips_hyphens_when_too_long(self):
        # 15 letters + 14 hyphens = 29 chars; "LU-" + 29 = 32 > 30 → triggers stripping
        # Without hyphens: 15 chars → "LU-" + 15 = 18 ≤ 30
        item = "A-B-C-D-E-F-G-H-I-J-K-L-M-N-O"
        result = build_sku("LU-", item)
        assert len(result) <= MAX_SKU_LENGTH
        assert "-" not in result[len("LU-"):]  # hyphens stripped from item part

    def test_raises_when_still_too_long_after_stripping(self):
        item = "X" * (MAX_SKU_LENGTH + 5)  # way too long even without hyphens
        with pytest.raises(ValueError, match="SKU too long"):
            build_sku("LU-", item)

    def test_no_hyphens_needed(self):
        assert build_sku("LU-", "DITT300BL") == "LU-DITT300BL"

    def test_empty_item_number(self):
        assert build_sku("LU-", "") == "LU-"


# ------------------------------------------------------------------
# _str
# ------------------------------------------------------------------

class TestStr:
    def test_normal_string(self):
        assert _str("hello") == "hello"

    def test_strips_whitespace(self):
        assert _str("  hello  ") == "hello"

    def test_empty_string_returns_none(self):
        assert _str("") is None

    def test_whitespace_only_returns_none(self):
        assert _str("   ") is None

    def test_pandas_na_returns_none(self):
        assert _str(pd.NA) is None

    def test_numpy_nan_returns_none(self):
        import numpy as np
        assert _str(np.nan) is None

    def test_numeric_converts_to_string(self):
        assert _str(42) == "42"

    def test_none_returns_none(self):
        assert _str(None) is None


# ------------------------------------------------------------------
# _num
# ------------------------------------------------------------------

class TestNum:
    def test_plain_float(self):
        assert _num("1.5") == 1.5

    def test_integer_string(self):
        assert _num("10") == 10.0

    def test_number_with_unit(self):
        assert _num("0.300 L") == 0.3

    def test_number_with_inches(self):
        assert _num("2.3000 IN") == 2.3

    def test_pandas_na_returns_none(self):
        assert _num(pd.NA) is None

    def test_none_returns_none(self):
        assert _num(None) is None

    def test_non_numeric_string_returns_none(self):
        assert _num("abc") is None

    def test_empty_string_returns_none(self):
        assert _num("") is None

    def test_actual_float(self):
        assert _num(3.14) == 3.14


# ------------------------------------------------------------------
# _add_field
# ------------------------------------------------------------------

class TestAddField:
    def test_adds_when_value_present(self):
        fields = []
        _add_field(fields, "Color/ Finish", "Brushed Nickel")
        assert fields == [{"name": "Color/ Finish", "value": "Brushed Nickel"}]

    def test_no_op_when_none(self):
        fields = []
        _add_field(fields, "Color/ Finish", None)
        assert fields == []

    def test_no_op_when_empty_string(self):
        fields = []
        _add_field(fields, "Style", "")
        assert fields == []

    def test_appends_multiple(self):
        fields = []
        _add_field(fields, "Color/ Finish", "White")
        _add_field(fields, "Style", "Modern")
        assert len(fields) == 2


# ------------------------------------------------------------------
# channel_ids property
# ------------------------------------------------------------------

class TestChannelIds:
    """Test the channel_ids property via a minimal BaseMapper subclass."""

    def _make_mapper(self):
        from mappers.base_mapper import BaseMapper

        class _TestMapper(BaseMapper):
            VENDOR_ID = 1
            PRODUCT_TYPES = {0}
            SKU_PREFIX = "T-"
            CATEGORY_MAP_FILE = "category_map.json"
            ROOT_CATEGORY = "Root"
            VENDOR_CATEGORY = "TestVendor"

        return _TestMapper()

    def test_single_channel_from_env(self, monkeypatch):
        monkeypatch.setenv("CHANNEL_IDS", "42")
        mapper = self._make_mapper()
        assert mapper.channel_ids == [42]

    def test_multiple_channels_from_env(self, monkeypatch):
        monkeypatch.setenv("CHANNEL_IDS", "1,1837773,1837778")
        mapper = self._make_mapper()
        assert mapper.channel_ids == [1, 1837773, 1837778]

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("CHANNEL_IDS", " 1 , 2 , 3 ")
        mapper = self._make_mapper()
        assert mapper.channel_ids == [1, 2, 3]

    def test_fallback_to_channel_id(self, monkeypatch):
        monkeypatch.delenv("CHANNEL_IDS", raising=False)
        monkeypatch.setenv("CHANNEL_ID", "99")
        mapper = self._make_mapper()
        assert mapper.channel_ids == [99]

    def test_default_when_nothing_set(self, monkeypatch):
        monkeypatch.delenv("CHANNEL_IDS", raising=False)
        monkeypatch.delenv("CHANNEL_ID", raising=False)
        mapper = self._make_mapper()
        assert mapper.channel_ids == [1]


class TestTruncation:
    def test_truncate_to_word_basic(self):
        from mappers.base_mapper import _truncate_to_word

        s = "short name"
        assert _truncate_to_word(s, 250) == s

    def test_truncate_to_word_trims_on_space(self):
        from mappers.base_mapper import _truncate_to_word

        # build a string that would cut a word at the 20-char boundary
        s = "word1 word2 word3 word4 word5"
        # choose limit that falls inside 'word4'
        limit = s.index("word4") + 2
        t = _truncate_to_word(s, limit)
        # truncated should end before 'word4' (so ends with 'word3')
        assert t is not None
        assert "word4" not in t

    def test_truncate_to_word_none(self):
        from mappers.base_mapper import _truncate_to_word

        assert _truncate_to_word(None, 250) is None

    def test_base_mapper_map_row_truncates_name_and_sets_image_description(self):
        """Ensure BaseMapper.map_row truncates long names and writes image descriptions."""
        from mappers.base_mapper import BaseMapper

        class _TestMapper(BaseMapper):
            VENDOR_ID = 1
            PRODUCT_TYPES = {0}
            SKU_PREFIX = "T-"
            CATEGORY_MAP_FILE = "category_map.json"
            ROOT_CATEGORY = "Root"
            VENDOR_CATEGORY = "TestVendor"

            def _build_images(self, row):
                return [{"image_url": "http://example.com/x.jpg", "is_thumbnail": True}]

            def _build_categories(self, row):
                return []

        mapper = _TestMapper()

        long_words = ["lorem"] * 80
        long_name = " ".join(long_words)

        import pandas as pd

        row = pd.Series({"Item Number": "SKU1", "Item Name": long_name})

        payload = mapper.map_row(row)
        assert len(payload["name"]) <= 250
        assert payload["images"][0]["description"] == payload["name"]
