"""Shared PDF mirroring utilities for import pipeline and CLI tools."""
import logging
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.auth import HTTPDigestAuth

from mappers.pdf_links import extract_link_url

log = logging.getLogger(__name__)


def load_filtered_feed(path: str, vendor_id: int, product_types: set[int]) -> pd.DataFrame:
    """Read XLSX and apply the standard vendor/product-type filter."""
    df = pd.read_excel(path, dtype_backend="numpy_nullable")
    return df[
        (df["VendorID"] == vendor_id) &
        (df["Product Type"].isin(product_types))
    ].reset_index(drop=True)


def _is_pdf(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".pdf")


def _pdf_filename(url: str) -> str:
    raw = Path(urlparse(url).path).name
    return re.sub(r'[+ ]+', '-', raw)


def collect_pdf_urls(feed_df, link_columns: list[str]) -> dict[str, str]:
    """Return {source_pdf_url: filename} for all unique PDF links in configured columns."""
    pdf_map: dict[str, str] = {}
    for _, row in feed_df.iterrows():
        for col in link_columns:
            raw_value = row.get(col, "")
            if pd.isna(raw_value):
                value = ""
            else:
                value = str(raw_value).strip()
            if not value:
                continue
            url = extract_link_url(value)
            if url and _is_pdf(url) and url not in pdf_map:
                pdf_map[url] = _pdf_filename(url)
    return pdf_map


def _webdav_url(base_url: str, dav_subdir: str, filename: str) -> str:
    return f"{base_url.rstrip('/')}/content/{dav_subdir}/{filename}"


def _head_exists(webdav_url: str, auth: tuple[str, str]) -> bool:
    try:
        response = requests.head(webdav_url, auth=auth, timeout=15)
        return response.status_code == 200
    except requests.RequestException:
        return False


def _download(url: str, session: requests.Session) -> bytes | None:
    try:
        response = session.get(url, timeout=30)
        if response.status_code == 200:
            return response.content
        log.warning("GET %s -> HTTP %s", url, response.status_code)
    except requests.RequestException as exc:
        log.warning("GET %s failed: %s", url, exc)
    return None


def _upload_pdf(webdav_url: str, data: bytes, auth: tuple[str, str]) -> bool:
    try:
        response = requests.put(
            webdav_url,
            data=data,
            auth=auth,
            headers={"Content-Type": "application/pdf"},
            timeout=60,
        )
        if response.status_code in (200, 201, 204):
            return True
        log.error("PUT %s -> HTTP %s", webdav_url, response.status_code)
    except requests.RequestException as exc:
        log.error("PUT %s failed: %s", webdav_url, exc)
    return False


def _ensure_webdav_dir(base_url: str, auth: tuple[str, str], dav_subdir: str) -> None:
    base = base_url.rstrip("/")
    path = f"{base}/content"
    for part in dav_subdir.split("/"):
        path = f"{path}/{part}"
        try:
            response = requests.request("MKCOL", path, auth=auth, timeout=15)
            if response.status_code not in (201, 301, 302, 405):
                log.debug("MKCOL %s -> %s", path, response.status_code)
        except requests.RequestException as exc:
            log.debug("MKCOL %s failed: %s", path, exc)


def mirror_feed_pdfs(
    feed_df,
    link_columns: list[str],
    dav_subdir: str,
    dry_run: bool = False,
) -> None:
    """Mirror PDF assets from feed to BC WebDAV. Skips files already present (via HEAD)."""
    webdav_url = os.environ["BC_WEBDAV_URL"].rstrip("/")
    webdav_user = os.environ["BC_WEBDAV_USER"]
    webdav_pass = os.environ["BC_WEBDAV_PASS"]
    content_base_url = os.environ["BC_CONTENT_BASE_URL"].rstrip("/")
    auth = HTTPDigestAuth(webdav_user, webdav_pass)

    pdf_map = collect_pdf_urls(feed_df, link_columns)
    log.info("Unique PDF URLs found: %d", len(pdf_map))
    if not pdf_map:
        return

    if dry_run:
        for source_url, filename in sorted(pdf_map.items()):
            dav_dest = _webdav_url(webdav_url, dav_subdir, filename)
            public_url = f"{content_base_url}/{dav_subdir}/{filename}"
            log.info("[DRY-RUN] %s -> %s (public %s)", source_url, dav_dest, public_url)
        return

    _ensure_webdav_dir(webdav_url, auth, dav_subdir)
    session = requests.Session()

    uploaded = skipped = failed = 0
    for source_url, filename in sorted(pdf_map.items()):
        dav_dest = _webdav_url(webdav_url, dav_subdir, filename)

        if _head_exists(dav_dest, auth):
            skipped += 1
            continue

        content = _download(source_url, session)
        if content is None:
            failed += 1
            continue

        if _upload_pdf(dav_dest, content, auth):
            uploaded += 1
        else:
            failed += 1

        time.sleep(0.1)

    log.info(
        "Mirror complete: %d uploaded, %d skipped, %d failed (total %d unique URLs)",
        uploaded,
        skipped,
        failed,
        len(pdf_map),
    )
