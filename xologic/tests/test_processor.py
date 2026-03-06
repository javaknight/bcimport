"""Tests for processor helpers: find_feed, _reconcile_custom_fields."""
import fnmatch
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from processor import _reconcile_custom_fields, find_feed


# ------------------------------------------------------------------
# _reconcile_custom_fields
# ------------------------------------------------------------------

class TestReconcileCustomFields:
    def test_injects_id_for_existing_field(self):
        payload = [{"name": "Color/ Finish", "value": "White"}]
        existing = [{"id": 101, "name": "Color/ Finish", "value": "Black"}]
        result = _reconcile_custom_fields(payload, existing)
        assert result == [{"name": "Color/ Finish", "value": "White", "id": 101}]

    def test_new_field_has_no_id(self):
        payload = [{"name": "New Field", "value": "val"}]
        existing = []
        result = _reconcile_custom_fields(payload, existing)
        assert result == [{"name": "New Field", "value": "val"}]
        assert "id" not in result[0]

    def test_mixed_existing_and_new(self):
        payload = [
            {"name": "Color/ Finish", "value": "White"},
            {"name": "Style", "value": "Modern"},
        ]
        existing = [{"id": 55, "name": "Color/ Finish", "value": "Black"}]
        result = _reconcile_custom_fields(payload, existing)
        assert result[0]["id"] == 55
        assert "id" not in result[1]

    def test_does_not_mutate_payload_dicts(self):
        original = {"name": "Color/ Finish", "value": "White"}
        payload = [original]
        existing = [{"id": 99, "name": "Color/ Finish", "value": "Black"}]
        _reconcile_custom_fields(payload, existing)
        assert "id" not in original  # original dict unchanged

    def test_empty_payload(self):
        assert _reconcile_custom_fields([], [{"id": 1, "name": "X", "value": "y"}]) == []


# ------------------------------------------------------------------
# find_feed
# ------------------------------------------------------------------

class TestFindFeed:
    def _mock_glob(self, names: list[str], tmp_path: Path):
        """Create real empty xlsx files in tmp_path and return the dir."""
        for name in names:
            (tmp_path / name).touch()
        return str(tmp_path)

    def test_single_xlsx_found(self, tmp_path):
        d = self._mock_glob(["lutron-data.xlsx"], tmp_path)
        assert find_feed(d).endswith("lutron-data.xlsx")

    def test_enricher_file_excluded(self, tmp_path):
        d = self._mock_glob(["lutron-data.xlsx", "lutron-pricing-2026.xlsx"], tmp_path)

        class FakeEnricher:
            FILENAME_PATTERN = "lutron-pricing*.xlsx"

        result = find_feed(d, enricher_classes=[FakeEnricher])
        assert result.endswith("lutron-data.xlsx")

    def test_raises_when_no_files(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No .xlsx files"):
            find_feed(str(tmp_path))

    def test_raises_on_ambiguous_files(self, tmp_path):
        d = self._mock_glob(["lutron-data.xlsx", "lutron-data-backup.xlsx"], tmp_path)
        with pytest.raises(FileNotFoundError, match="Ambiguous"):
            find_feed(d)

    def test_raises_when_only_enricher_files(self, tmp_path):
        d = self._mock_glob(["lutron-pricing-2026.xlsx"], tmp_path)

        class FakeEnricher:
            FILENAME_PATTERN = "lutron-pricing*.xlsx"

        with pytest.raises(FileNotFoundError, match="enricher inputs"):
            find_feed(d, enricher_classes=[FakeEnricher])

    def test_no_enricher_classes(self, tmp_path):
        d = self._mock_glob(["feed.xlsx"], tmp_path)
        assert find_feed(d, enricher_classes=None).endswith("feed.xlsx")
