"""Extractor for generic web pages and articles."""
from __future__ import annotations

from textwrap import dedent
from typing import Dict, Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from readability import Document

from app.models import RawMessage, Resource

from .base_extractor import BaseExtractor, ExtractionResult

REQUEST_TIMEOUT = 10.0
USER_AGENT = "KnowledgeInboxExtractor/0.1 (+https://example.com)"
MAX_TEXT_LENGTH = 10000
ARTICLE_DOMAINS = {
    "medium.com",
    "substack.com",
    "nytimes.com",
    "washingtonpost.com",
    "theguardian.com",
    "blogspot.com",
}


class GenericWebExtractor(BaseExtractor):
    supported_formats = ("generic_webpage", "web_article", "unknown_url", "linkedin_post", "youtube_short")

    def extract(self, resource: Resource, raw_message: RawMessage) -> ExtractionResult:
        url = resource.final_url or resource.url
        headers = {"User-Agent": USER_AGENT}
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=headers) as client:
            response = client.get(url)
            response.raise_for_status()
            final_url = str(response.url)
            html = response.text

        doc = Document(html)
        summary_html = doc.summary()
        title = doc.short_title()
        text = BeautifulSoup(summary_html, "html.parser").get_text(separator="\n", strip=True) if summary_html else None
        if not text:
            text = BeautifulSoup(html, "html.parser").get_text(separator="\n", strip=True)
        if text and len(text) > MAX_TEXT_LENGTH:
            text = text[:MAX_TEXT_LENGTH]

        soup = BeautifulSoup(html, "html.parser")
        metadata = _collect_meta(soup)
        description = metadata.get("og:description") or metadata.get("description")
        canonical_url = metadata.get("canonical") or final_url
        domain = resource.domain or urlparse(final_url).netloc.lower()
        author = metadata.get("author") or metadata.get("og:site_name")

        content_format = resource.content_format
        if content_format in (None, "unknown_url"):
            content_format = self._infer_format(domain, text)

        return ExtractionResult(
            platform="web",
            content_format=content_format,
            extraction_method="httpx_readability",
            title=metadata.get("og:title") or title or resource.title,
            description=description,
            canonical_url=canonical_url,
            author=author,
            extracted_text=text,
            raw_metadata=metadata,
            status="success",
            error=None,
        )

    def _infer_format(self, domain: Optional[str], text: Optional[str]) -> str:
        if domain:
            domain = domain.lower()
            if any(domain.endswith(d) for d in ARTICLE_DOMAINS):
                return "web_article"
        if text and len(text) > 1500:
            return "web_article"
        return "generic_webpage"


def _collect_meta(soup: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        data["title"] = title_tag.string.strip()

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
