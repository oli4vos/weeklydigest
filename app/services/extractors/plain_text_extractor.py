"""Extractor for plain text Telegram messages without links."""
from __future__ import annotations

from textwrap import shorten

from app.models import RawMessage, Resource

from .base_extractor import BaseExtractor, ExtractionResult

MAX_TEXT_LENGTH = 5000


class PlainTextExtractor(BaseExtractor):
    supported_formats = ("plain_text",)

    def extract(self, resource: Resource, raw_message: RawMessage) -> ExtractionResult:
        text = (raw_message.text or "").strip()
        if len(text) > MAX_TEXT_LENGTH:
            text = text[:MAX_TEXT_LENGTH]

        description = shorten(text, width=240, placeholder="…") if text else None

        return ExtractionResult(
            platform="telegram",
            content_format="plain_text",
            extraction_method="direct_text",
            title=f"Bericht vanaf Telegram #{raw_message.id}",
            description=description,
            canonical_url=resource.final_url or resource.url,
            author=raw_message.sender_name,
            extracted_text=text,
            raw_metadata={
                "source": raw_message.source,
                "chat_id": raw_message.chat_id,
            },
            status="success" if text else "partial",
            error=None if text else "No text content available",
        )
