"""Shared helpers for mirrored PDF asset link handling in mapper descriptions."""
import html.parser
import logging
import re
import re
from urllib.parse import urlparse

log = logging.getLogger(__name__)


def is_pdf_url(url: str) -> bool:
    """True when URL path ends in .pdf."""
    return urlparse(url).path.lower().endswith(".pdf")


def is_bc_content_url(url: str) -> bool:
    """True when URL already points at /content/ on the store domain."""
    return urlparse(url).path.startswith("/content/")


class _HrefExtractor(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.href: str | None = None
        self.text_parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if self.href is None and tag == "a":
            self.href = dict(attrs).get("href")

    def handle_data(self, data):
        self.text_parts.append(data)


def extract_href(html_fragment: str) -> str | None:
    """Extract first href from an HTML fragment."""
    if not html_fragment:
        return None
    parser = _HrefExtractor()
    parser.feed(html_fragment)
    return parser.href


def extract_anchor_text(html_fragment: str) -> str | None:
    """Extract text content from the first anchor fragment."""
    if not html_fragment:
        return None
    parser = _HrefExtractor()
    parser.feed(html_fragment)
    text = "".join(parser.text_parts).strip()
    return text or None


def extract_link_url(value: str) -> str | None:
    """Extract URL from a cell value that may be an anchor or a raw URL."""
    if not value:
        return None
    href = extract_href(value)
    if href:
        return href
    candidate = value.strip()
    parsed = urlparse(candidate)
    if parsed.scheme in ("http", "https") and parsed.netloc:
        return candidate
    return None


class _HrefRewriter(html.parser.HTMLParser):
    def __init__(self, replacements: dict[str, str]):
        super().__init__()
        self._replacements = replacements
        self._parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            new_attrs = []
            for name, val in attrs:
                if name == "href" and val:
                    val = self._replacements.get(val, val)
                new_attrs.append(f'{name}="{val}"' if val is not None else name)
            self._parts.append(f'<a {" ".join(new_attrs)}>')
        else:
            self._parts.append(self.get_starttag_text() or "")

    def handle_endtag(self, tag):
        self._parts.append(f"</{tag}>")

    def handle_data(self, data):
        self._parts.append(data)

    @property
    def result(self) -> str:
        return "".join(self._parts)


def _rewrite_anchor_href(html_fragment: str, old_href: str, new_href: str) -> str:
    parser = _HrefRewriter({old_href: new_href})
    parser.feed(html_fragment)
    return parser.result


def _humanize_label(column_name: str) -> str:
    label = column_name
    label = re.sub(r"^Extra[-_\s]*", "", label, flags=re.IGNORECASE)
    label = re.sub(r"[-_]", " ", label).strip()
    if label.lower().endswith(" link"):
        label = label[:-5].strip()
    return label or "Document"


def build_description_link_or_none(
    cell_value: str,
    column_name: str,
    dav_subdir: str,
    content_base_url: str,
) -> str | None:
    """Build one description line from a feed cell that may be anchor or raw URL.

    PDF URLs are rewritten deterministically to the BC-hosted /content/ path.
    Non-PDF URLs are preserved as-is.
    """
    if not cell_value:
        return None
    raw = cell_value.strip()
    if not raw:
        return None

    href = extract_link_url(raw)
    if not href:
        return None

    final_href = href
    if is_pdf_url(href) and not is_bc_content_url(href):
        raw_filename = urlparse(href).path.rsplit("/", 1)[-1]
        filename = re.sub(r'[+ ]+', '-', raw_filename)
        final_href = f"{content_base_url.rstrip('/')}/{dav_subdir}/{filename}"

    if extract_href(raw):
        if final_href == href:
            return raw
        return _rewrite_anchor_href(raw, href, final_href)

    label = _humanize_label(column_name)
    return f'<a href="{final_href}">{label}</a>'
