"""Extractor for YouTube videos."""
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


class YouTubeExtractor(BaseExtractor):
    supported_formats = ("youtube_video", "youtube_short")

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
                platform="youtube",
                content_format="youtube_video",
                extraction_method="httpx_meta",
                raw_metadata={"error": str(exc)},
                status="failed",
                error=str(exc),
            )

        soup = BeautifulSoup(html, "html.parser")
        metadata = _collect_youtube_meta(soup)

        title = metadata.get("og:title") or metadata.get("title")
        description = metadata.get("og:description") or metadata.get("description")
        author = metadata.get("og:site_name") or metadata.get("author")

        return ExtractionResult(
            platform="youtube",
            content_format=resource.content_format or "youtube_video",
            extraction_method="httpx_meta",
            title=title,
            description=description,
            canonical_url=metadata.get("canonical") or (resource.final_url or resource.url),
            author=author,
            extracted_text=description,
            raw_metadata=metadata,
            status="success" if metadata else "partial",
            error=None if metadata else "Metadata missing",
        )


def _collect_youtube_meta(soup: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for meta in soup.find_all("meta"):
        name = meta.get("name") or meta.get("property")
        if not name:
            continue
        content = meta.get("content")
        if content:
            data[name] = content.strip()
    channel = soup.find("link", itemprop="name")
    if channel and channel.get("content"):
        data["channel_name"] = channel["content"].strip()
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        data["canonical"] = canonical["href"].strip()
    return data
