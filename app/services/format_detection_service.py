"""Detect platform/content format for resources."""
from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

from app.models import Resource


@dataclass
class DetectionResult:
    platform: str
    content_format: str


class FormatDetectionService:
    """Lightweight heuristics to decide which extractor to use."""

    def detect(self, resource: Resource) -> DetectionResult:
        url = resource.final_url or resource.url or ""
        if url.startswith("telegram://"):
            return DetectionResult(platform="telegram", content_format="plain_text")

        parsed = urlparse(url)
        domain = (parsed.netloc or "").lower()
        path = parsed.path or ""
        path_lower = path.lower()

        if any(x in domain for x in ("instagram.com", "instagr.am")):
            if "/reel/" in path_lower:
                return DetectionResult(platform="instagram", content_format="instagram_reel")
            return DetectionResult(platform="instagram", content_format="instagram_post")

        if "linkedin.com" in domain:
            if any(segment in path_lower for segment in ("/posts/", "/feed/update/", "/pulse/")):
                return DetectionResult(platform="linkedin", content_format="linkedin_post")

        if "youtube.com" in domain or "youtu.be" in domain:
            if "/shorts/" in path_lower:
                return DetectionResult(platform="youtube", content_format="youtube_short")
            return DetectionResult(platform="youtube", content_format="youtube_video")

        if domain:
            if path_lower.endswith(".html") or any(domain.endswith(d) for d in ARTICLE_DOMAINS):
                return DetectionResult(platform="web", content_format="web_article")
            return DetectionResult(platform="web", content_format="generic_webpage")

        return DetectionResult(platform="web", content_format="unknown_url")


ARTICLE_DOMAINS = (
    "medium.com",
    "substack.com",
    "nytimes.com",
    "theguardian.com",
    "washingtonpost.com",
    "blogspot.com",
)
