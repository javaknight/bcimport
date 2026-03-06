"""Tests for pdf_links helpers."""
import pytest

from mappers.pdf_links import (
    _humanize_label,
    build_description_link_or_none,
    extract_anchor_text,
    extract_href,
    is_bc_content_url,
    is_pdf_url,
)

CONTENT_BASE = "https://grandbrass.com/content"
DAV_SUBDIR = "catalog/documents/lutron/pdfs"


# ------------------------------------------------------------------
# is_pdf_url
# ------------------------------------------------------------------

class TestIsPdfUrl:
    def test_pdf_url(self):
        assert is_pdf_url("https://example.com/docs/spec.pdf")

    def test_pdf_url_uppercase(self):
        assert is_pdf_url("https://example.com/docs/spec.PDF")

    def test_non_pdf_url(self):
        assert not is_pdf_url("https://example.com/image.png")

    def test_empty(self):
        assert not is_pdf_url("")


# ------------------------------------------------------------------
# is_bc_content_url
# ------------------------------------------------------------------

class TestIsBcContentUrl:
    def test_content_url(self):
        assert is_bc_content_url("https://grandbrass.com/content/catalog/docs/file.pdf")

    def test_non_content_url(self):
        assert not is_bc_content_url("https://lutron.com/docs/spec.pdf")


# ------------------------------------------------------------------
# extract_href / extract_anchor_text
# ------------------------------------------------------------------

class TestExtractHref:
    def test_anchor_tag(self):
        assert extract_href('<a href="https://example.com/doc.pdf">Doc</a>') == "https://example.com/doc.pdf"

    def test_raw_url_returns_none(self):
        assert extract_href("https://example.com/doc.pdf") is None

    def test_empty_returns_none(self):
        assert extract_href("") is None

    def test_anchor_text(self):
        assert extract_anchor_text('<a href="https://example.com/doc.pdf">Spec Sheet</a>') == "Spec Sheet"


# ------------------------------------------------------------------
# _humanize_label
# ------------------------------------------------------------------

class TestHumanizeLabel:
    def test_strips_extra_prefix_and_link_suffix(self):
        assert _humanize_label("Extra-Installation Link") == "Installation"

    def test_strips_extra_prefix_no_link_suffix(self):
        assert _humanize_label("Extra-Brochure") == "Brochure"

    def test_replaces_hyphens_with_spaces(self):
        assert _humanize_label("Extra-Tech Drawing Link") == "Tech Drawing"

    def test_no_extra_prefix(self):
        assert _humanize_label("Spec Sheet") == "Spec Sheet"

    def test_empty_falls_back(self):
        assert _humanize_label("Extra-") == "Document"


# ------------------------------------------------------------------
# build_description_link_or_none
# ------------------------------------------------------------------

class TestBuildDescriptionLinkOrNone:
    def test_raw_pdf_url_becomes_bc_hosted(self):
        result = build_description_link_or_none(
            "https://lutron.com/docs/spec.pdf",
            "Extra-Spec Sheet",
            DAV_SUBDIR,
            CONTENT_BASE,
        )
        assert result == '<a href="https://grandbrass.com/content/catalog/documents/lutron/pdfs/spec.pdf">Spec Sheet</a>'

    def test_anchor_pdf_href_is_rewritten(self):
        cell = '<a href="https://lutron.com/docs/install.pdf">Install Guide</a>'
        result = build_description_link_or_none(cell, "Extra-Installation Link", DAV_SUBDIR, CONTENT_BASE)
        assert "grandbrass.com/content" in result
        assert "Install Guide" in result

    def test_already_bc_content_url_unchanged(self):
        cell = '<a href="https://grandbrass.com/content/catalog/documents/lutron/pdfs/spec.pdf">Spec</a>'
        result = build_description_link_or_none(cell, "Extra-Spec Sheet", DAV_SUBDIR, CONTENT_BASE)
        assert result == cell  # returned as-is

    def test_plus_in_filename_replaced_with_hyphen(self):
        result = build_description_link_or_none(
            "https://lutron.com/docs/spec+sheet.pdf",
            "Extra-Spec Sheet",
            DAV_SUBDIR,
            CONTENT_BASE,
        )
        assert "spec-sheet.pdf" in result
        assert "spec+sheet.pdf" not in result

    def test_space_in_filename_replaced_with_hyphen(self):
        result = build_description_link_or_none(
            "https://lutron.com/docs/spec sheet.pdf",
            "Extra-Spec Sheet",
            DAV_SUBDIR,
            CONTENT_BASE,
        )
        assert "spec-sheet.pdf" in result

    def test_none_on_empty_value(self):
        assert build_description_link_or_none("", "Extra-Spec Sheet", DAV_SUBDIR, CONTENT_BASE) is None

    def test_non_pdf_url_preserved(self):
        cell = '<a href="https://www.youtube.com/watch?v=abc">Video</a>'
        result = build_description_link_or_none(cell, "Extra-Video Clip", DAV_SUBDIR, CONTENT_BASE)
        assert result == cell  # non-PDF anchors returned as-is

    def test_none_on_non_url_value(self):
        assert build_description_link_or_none("not a url", "Extra-Spec Sheet", DAV_SUBDIR, CONTENT_BASE) is None
