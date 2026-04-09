"""Service to extract links from raw messages and persist resources."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from readability import Document

from app.db import get_session
from app.models import RawMessage, Resource
from app.repositories.raw_message_repository import RawMessageRepository
from app.repositories.resource_repository import ResourceRepository

URL_PATTERN = re.compile(r"((?:https?://|www\.)[^\s<>()]+)", re.IGNORECASE)
REQUEST_TIMEOUT = 10.0
MAX_TEXT_LENGTH = 10_000
USER_AGENT = "KnowledgeInboxBot/0.1 (+https://example.com)"


@dataclass
class LinkProcessingStats:
    messages: int = 0
    links_seen: int = 0
    links_success: int = 0
    links_failed: int = 0
    links_skipped: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "messages": self.messages,
            "links_seen": self.links_seen,
            "links_success": self.links_success,
            "links_failed": self.links_failed,
            "links_skipped": self.links_skipped,
        }


class LinkService:
    """Coordinates link extraction and storage for raw messages."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def process_backlog(self, limit: Optional[int] = None) -> Dict[str, int]:
        stats = LinkProcessingStats()
        with get_session() as session:
            messages = RawMessageRepository.get_messages_with_links(session, limit)

        if not messages:
            self.logger.info("No messages with links found for processing")
            return stats.to_dict()

        for message in messages:
            stats.messages += 1
            urls = self._extract_urls(message.text or "")
            stats.links_seen += len(urls)
            for url in urls:
                outcome = self._process_single_link(message, url)
                if outcome == "success":
                    stats.links_success += 1
                elif outcome == "failed":
                    stats.links_failed += 1
                else:
                    stats.links_skipped += 1
        return stats.to_dict()

    def _extract_urls(self, text: str) -> List[str]:
        candidates = URL_PATTERN.findall(text)
        normalized: List[str] = []
        seen = set()
        for candidate in candidates:
            url = self._normalize_url(candidate)
            if not url or url in seen:
                continue
            seen.add(url)
            normalized.append(url)
        return normalized

    def _normalize_url(self, url: str) -> Optional[str]:
        cleaned = url.strip().strip("\n\t\r").strip("\"'<>).,;")
        if not cleaned:
            return None
        if not cleaned.lower().startswith(("http://", "https://")):
            cleaned = f"http://{cleaned}"
        parsed = urlparse(cleaned)
        if not parsed.scheme or not parsed.netloc:
            return None
        return parsed.geturl()

    def _process_single_link(self, message: RawMessage, url: str) -> str:
        with get_session() as session:
            if ResourceRepository.exists_for_url(session, message.id, url):
                self.logger.debug("Skipping duplicate url %s for message %s", url, message.id)
                return "skipped"

            resource = ResourceRepository.add_pending(session, message.id, url)
            resource_id = resource.id

        try:
            result = self._fetch_and_extract(url)
        except Exception as exc:  # pragma: no cover - defensive path
            self._mark_resource_failed(resource_id=resource_id, raw_message_id=message.id, error=exc)
            return "failed"

        self._mark_resource_success(resource_id=resource_id, message=message, processed_url=url, result=result)
        return "success"

    def _fetch_and_extract(self, url: str) -> "FetchResult":
        headers = {"User-Agent": USER_AGENT}
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=headers) as client:
            response = client.get(url)
            response.raise_for_status()
            final_url = str(response.url)
            html = response.text

        title, extracted_text = self._extract_content(html)
        domain = self._extract_domain(final_url)
        return FetchResult(
            final_url=final_url,
            title=title,
            domain=domain,
            extracted_text=extracted_text,
        )

    def _mark_resource_failed(self, resource_id: int, raw_message_id: int, error: Exception) -> None:
        with get_session() as session:
            resource = session.get(Resource, resource_id)
            if not resource:
                return
            resource.status = "failed"
            resource.fetched_at = datetime.now(timezone.utc)
            session.commit()
        self.logger.warning("Failed to process url for message %s: %s", raw_message_id, error)

    def _mark_resource_success(
        self, resource_id: int, message: RawMessage, processed_url: str, result: "FetchResult"
    ) -> None:
        with get_session() as session:
            resource = session.get(Resource, resource_id)
            if not resource:
                return
            resource.final_url = result.final_url
            resource.title = result.title
            resource.domain = result.domain or self._extract_domain(processed_url)
            resource.extracted_text = result.extracted_text
            resource.fetched_at = datetime.now(timezone.utc)
            resource.status = "success"
            session.commit()
        self.logger.info(
            "Stored resource for message=%s url=%s final_url=%s",
            message.id,
            processed_url,
            result.final_url,
        )

    def _extract_content(self, html: str) -> tuple[Optional[str], Optional[str]]:
        title: Optional[str] = None
        extracted_text: Optional[str] = None
        try:
            doc = Document(html)
            title = doc.short_title() or title
            summary_html = doc.summary()
            extracted_text = self._html_to_text(summary_html)
        except Exception:
            self.logger.debug("Readability extraction failed", exc_info=True)

        if not extracted_text:
            extracted_text = self._html_to_text(html)
        if not title:
            soup = BeautifulSoup(html, "html.parser")
            if soup.title and soup.title.string:
                title = soup.title.string.strip()

        if extracted_text and len(extracted_text) > MAX_TEXT_LENGTH:
            extracted_text = extracted_text[:MAX_TEXT_LENGTH]
        return title, extracted_text

    @staticmethod
    def _html_to_text(html: str) -> Optional[str]:
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        return text or None

    @staticmethod
    def _extract_domain(url: str) -> Optional[str]:
        parsed = urlparse(url)
        return parsed.netloc or None


@dataclass
class FetchResult:
    final_url: str
    title: Optional[str]
    domain: Optional[str]
    extracted_text: Optional[str]
