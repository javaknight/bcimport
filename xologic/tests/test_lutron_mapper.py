"""Tests for LutronMapper-specific behavior."""

from mappers.lutron_mapper import LutronMapper


class TestResolveDepth:
    def test_uses_length_custom_field_when_present(self):
        mapper = LutronMapper()
        payload = {
            "custom_fields": [{"name": "Length", "value": "2.3000 IN"}],
            "width": 5.0,
            "height": 4.0,
        }

        assert mapper._resolve_depth(payload) == 2.3

    def test_falls_back_to_smallest_width_height_without_length(self):
        mapper = LutronMapper()
        payload = {
            "custom_fields": [{"name": "Style", "value": "Modern"}],
            "width": 8.5,
            "height": 6.25,
        }

        assert mapper._resolve_depth(payload) == 6.25

    def test_falls_back_when_length_is_blank(self):
        mapper = LutronMapper()
        payload = {
            "custom_fields": [{"name": "Length", "value": "   "}],
            "width": 2.0,
            "height": 9.0,
        }

        assert mapper._resolve_depth(payload) == 2.0

    def test_returns_none_when_no_usable_dimensions(self):
        mapper = LutronMapper()
        payload = {
            "custom_fields": [{"name": "Style", "value": "Classic"}],
        }

        assert mapper._resolve_depth(payload) is None
