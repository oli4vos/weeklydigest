"""Extractor for Instagram posts and reels."""
from __future__ import annotations

from typing import Dict
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.models import RawMessage, Resource

from .base_extractor import BaseExtractor, ExtractionResult

REQUEST_TIMEOUT = 10.0
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


class InstagramExtractor(BaseExtractor):
    supported_formats = ("instagram_reel", "instagram_post")

    def extract(self, resource: Resource, raw_message: RawMessage) -> ExtractionResult:
        url = resource.final_url or resource.url
        headers = {"User-Agent": USER_AGENT}
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=headers) as client:
                response = client.get(url)
                response.raise_for_status()
                html = response.text
        except Exception as exc:  # noqa: BLE001
            return ExtractionResult(
                platform="instagram",
                content_format=resource.content_format or "instagram_post",
                extraction_method="httpx_meta",
                raw_metadata={"error": str(exc)},
                status="failed",
                error=str(exc),
            )

        soup = BeautifulSoup(html, "html.parser")
        metadata = _collect_instagram_meta(soup)

        extracted_text = metadata.get("og:description")
        author = metadata.get("og:title")
        description = metadata.get("og:description") or metadata.get("description")

        status = "success" if metadata else "partial"
        error = None if metadata else "Limited metadata due to login wall"

        return ExtractionResult(
            platform="instagram",
            content_format=resource.content_format or "instagram_post",
            extraction_method="httpx_meta",
            title=metadata.get("og:title"),
            description=description,
            canonical_url=metadata.get("canonical") or (resource.final_url or resource.url),
            author=author,
            extracted_text=extracted_text,
            raw_metadata=metadata,
            status=status,
            error=error,
        )


def _collect_instagram_meta(soup: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for meta in soup.find_all("meta"):
        name = meta.get("name") or meta.get("property")
        if not name:
            continue
        content = meta.get("content")
        if content:
            data[name] = content.strip()
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        data["canonical"] = canonical["href"].strip()
    return data
