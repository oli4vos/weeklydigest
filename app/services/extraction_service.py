"""Deterministic extraction pipeline."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from app.db import get_session
from app.models import RawMessage, Resource
from app.repositories.raw_message_repository import RawMessageRepository
from app.repositories.resource_repository import ResourceRepository
from app.services.extractors import (
    GenericWebExtractor,
    InstagramExtractor,
    PlainTextExtractor,
    YouTubeExtractor,
)
from app.services.format_detection_service import DetectionResult, FormatDetectionService

ExtractorType = PlainTextExtractor | GenericWebExtractor | InstagramExtractor | YouTubeExtractor
LINK_PATTERN = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
DEFAULT_LINK_SCAN_LIMIT = 100


@dataclass
class ExtractionStats:
    processed: int = 0
    success: int = 0
    partial: int = 0
    failed: int = 0
    skipped: int = 0
    messages_scanned: int = 0
    link_resources_created: int = 0
    plain_resources_created: int = 0
    reclassified: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "processed": self.processed,
            "success": self.success,
            "partial": self.partial,
            "failed": self.failed,
            "skipped": self.skipped,
            "messages_scanned": self.messages_scanned,
            "link_resources_created": self.link_resources_created,
            "plain_resources_created": self.plain_resources_created,
            "reclassified": self.reclassified,
        }


class ExtractionService:
    """Coordinates format detection and extractor execution."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(__name__)
        self.detector = FormatDetectionService()
        self.extractors: List[ExtractorType] = [
            PlainTextExtractor(),
            InstagramExtractor(),
            YouTubeExtractor(),
            GenericWebExtractor(),
        ]

    def process(self, limit: int = 20) -> Dict[str, int]:
        stats = ExtractionStats()
        stats.reclassified = self._reclassify_special_cases()
        link_scanned, link_created = self._bootstrap_link_resources(DEFAULT_LINK_SCAN_LIMIT)
        stats.messages_scanned = link_scanned
        stats.link_resources_created = link_created
        stats.plain_resources_created = self._bootstrap_plain_text_resources(DEFAULT_LINK_SCAN_LIMIT)

        with get_session() as session:
            resources = ResourceRepository.get_resources_needing_extraction(session, limit)

        if not resources:
            self.logger.info("No resources require extraction")
            return stats.as_dict()

        for resource in resources:
            if not resource.raw_message:
                stats.skipped += 1
                continue

            stats.processed += 1
            detection = self.detector.detect(resource)
            extractor = self._select_extractor(detection.content_format)
            if not extractor:
                self.logger.warning("No extractor for format %s", detection.content_format)
                stats.skipped += 1
                continue

            try:
                result = extractor.extract(resource, resource.raw_message)
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("Extraction error for resource %s: %s", resource.id, exc)
                result = None

            if result is None:
                self._persist_status(resource.id, detection, status="failed", error="Extractor crashed")
                stats.failed += 1
                continue

            if result.status == "success":
                stats.success += 1
            elif result.status == "partial":
                stats.partial += 1
            elif result.status == "failed":
                stats.failed += 1

            self._persist_result(resource.id, detection, result)

        return stats.as_dict()

    def _select_extractor(self, content_format: str) -> Optional[ExtractorType]:
        for extractor in self.extractors:
            if extractor.supports(content_format):
                return extractor
        return None

    def _bootstrap_plain_text_resources(self, limit: int) -> int:
        created = 0
        with get_session() as session:
            messages = RawMessageRepository.get_plain_text_without_resource(session, limit)
            for message in messages:
                ResourceRepository.ensure_plain_text_resource(session, message)
                created += 1
            if created:
                session.commit()
        if created:
            self.logger.info("Created %s plain-text resources", created)
        return created

    def _bootstrap_link_resources(self, limit: int) -> Tuple[int, int]:
        scanned = 0
        created = 0
        with get_session() as session:
            messages = RawMessageRepository.get_messages_with_links(session, limit=limit)
            scanned = len(messages)
            for message in messages:
                urls = self._extract_urls(message.text or "")
                for url in urls:
                    if ResourceRepository.exists_for_url(session, message.id, url):
                        continue
                    ResourceRepository.add_pending(session, message.id, url)
                    created += 1
            if created:
                session.commit()
        if scanned:
            self.logger.info("Scanned %s messages with links, created %s resources", scanned, created)
        return scanned, created

    def _persist_status(self, resource_id: int, detection: DetectionResult, status: str, error: Optional[str]) -> None:
        with get_session() as session:
            resource = session.get(Resource, resource_id)
            if not resource:
                return
            resource.platform = detection.platform
            resource.content_format = detection.content_format
            resource.extraction_status = status
            resource.extraction_error = error

    def _persist_result(self, resource_id: int, detection: DetectionResult, result) -> None:
        with get_session() as session:
            resource = session.get(Resource, resource_id)
            if not resource:
                return
            resource.platform = result.platform or detection.platform
            resource.content_format = result.content_format or detection.content_format
            resource.extraction_method = result.extraction_method
            if result.title:
                resource.title = result.title
            if result.description:
                resource.description = result.description
            if result.canonical_url:
                resource.canonical_url = result.canonical_url
            if result.author:
                resource.author = result.author
            if result.extracted_text:
                resource.extracted_text = result.extracted_text
            resource.raw_metadata_json = result.raw_metadata
            resource.extraction_status = result.status
            resource.extraction_error = result.error

    def _extract_urls(self, text: str) -> List[str]:
        candidates = LINK_PATTERN.findall(text)
        urls: List[str] = []
        seen = set()
        for candidate in candidates:
            normalized = self._normalize_url(candidate)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            urls.append(normalized)
        return urls

    @staticmethod
    def _normalize_url(url: str) -> Optional[str]:
        cleaned = url.strip().strip("()[]{}<>\"'.,")
        if not cleaned:
            return None
        if not cleaned.lower().startswith(("http://", "https://")):
            cleaned = f"http://{cleaned}"
        return cleaned

    def _reclassify_special_cases(self) -> int:
        patterns = ["linkedin.com", "youtube.com", "youtu.be"]
        updated = 0
        with get_session() as session:
            resources = ResourceRepository.get_resources_by_url_patterns(session, patterns)
            for resource in resources:
                detection = self.detector.detect(resource)
                if (
                    resource.platform != detection.platform
                    or resource.content_format != detection.content_format
                ):
                    resource.platform = detection.platform
                    resource.content_format = detection.content_format
                    resource.extraction_status = "pending"
                    updated += 1
            if updated:
                session.commit()
        if updated:
            self.logger.info("Reclassified %s existing resources", updated)
        return updated
