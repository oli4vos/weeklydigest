"""Base extractor interfaces and models."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.models import RawMessage, Resource


@dataclass
class ExtractionResult:
    platform: str
    content_format: str
    extraction_method: str
    title: Optional[str] = None
    description: Optional[str] = None
    canonical_url: Optional[str] = None
    author: Optional[str] = None
    extracted_text: Optional[str] = None
    raw_metadata: Optional[dict] = None
    status: str = "success"
    error: Optional[str] = None


class BaseExtractor:
    """Strategy base-class for deterministic extraction."""

    supported_formats: tuple[str, ...] = ()

    def supports(self, content_format: str) -> bool:
        return content_format in self.supported_formats

    def extract(self, resource: Resource, raw_message: RawMessage) -> ExtractionResult:
        raise NotImplementedError
