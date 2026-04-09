"""Enrichment service powering AI-based knowledge extraction.

Note: this service is only used when `scripts/process_enrichment.py` runs;
it is not part of the daily/weekly automation yet, but kept for future expansion.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from textwrap import dedent
from typing import Dict, List, Optional

from openai import OpenAI

from app.config import get_settings
from app.db import get_session
from app.models import RawMessage, Resource
from app.repositories.knowledge_repository import KnowledgeRepository
from app.repositories.raw_message_repository import RawMessageRepository
from app.schemas import EnrichmentPayload

settings = get_settings()
MAX_MESSAGE_CHARS = 2000
MAX_RESOURCE_CHARS = 1200
MAX_RESOURCES = 3


@dataclass
class EnrichmentStats:
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0

    def as_dict(self) -> Dict[str, int]:
        return {
            "processed": self.processed,
            "success": self.success,
            "failed": self.failed,
            "skipped": self.skipped,
        }


class EnrichmentService:
    """Runs OpenAI-based enrichment for raw messages."""

    def __init__(self) -> None:
        if not settings.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY is not set")
        self.logger = logging.getLogger(__name__)
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model

    def process_batch(self, limit: int = 20) -> Dict[str, int]:
        stats = EnrichmentStats()
        with get_session() as session:
            messages = RawMessageRepository.get_messages_without_knowledge(session, limit=limit)

        if not messages:
            self.logger.info("No raw messages need enrichment")
            return stats.as_dict()

        for message in messages:
            stats.processed += 1
            try:
                payload = self._enrich_message(message)
                if payload is None:
                    stats.skipped += 1
                    continue
                with get_session() as session:
                    managed_message = session.get(RawMessage, message.id)
                    if not managed_message:
                        continue
                    KnowledgeRepository.create_from_enrichment(
                        session, raw_message=managed_message, payload=payload.model_dump()
                    )
                stats.success += 1
            except Exception as exc:
                stats.failed += 1
                self.logger.exception("Failed to enrich raw_message_id=%s: %s", message.id, exc)

        return stats.as_dict()

    def _enrich_message(self, message: RawMessage) -> Optional[EnrichmentPayload]:
        prompt = self._build_prompt(message)
        if prompt is None:
            self.logger.warning("Skipping message %s due to missing context", message.id)
            return None
        response = self.client.responses.create(
            model=self.model,
            input=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        try:
            content = response.output[0].content[0].text  # type: ignore[index]
            data = json.loads(content)
            payload = EnrichmentPayload(**data)
            return payload
        except Exception as exc:  # noqa: BLE001
            self.logger.exception("Invalid enrichment output for message %s: %s", message.id, exc)
            return None

    def _build_prompt(self, message: RawMessage) -> Optional[str]:
        raw_text = (message.text or "").strip()
        trimmed_text = raw_text[:MAX_MESSAGE_CHARS]
        resource_context = self._format_resources(message.resources)

        if not trimmed_text and not resource_context:
            return None

        prompt = dedent(
            f"""
            Bron: {message.source}
            Ontvangen op: {message.received_at.isoformat()}
            Aangeleverd door: {message.sender_name or 'onbekend'}

            Originele tekst:
            {trimmed_text or '[geen platte tekst beschikbaar]'}
            """
        ).strip()

        if resource_context:
            prompt += "\n\nTechnische context:\n" + resource_context

        prompt += dedent(
            """

            Maak een inhoudelijke analyse en geef output in strikt JSON-formaat met de velden:
            {
              "category": "...",
              "summary": "...",
              "insights": ["..."],
              "tags": ["..."],
              "priority": "...",
              "action_required": true/false,
              "action_suggestion": "...",
              "relevance_reason": "..."
            }
            Gebruik alleen de toegestane categorieën (Idee, Taak, Inspiratie, Link/Resource, Reflectie, Overig)
            en prioriteiten (Hoog, Medium, Laag). Schrijf alles in het Nederlands.
            """
        )
        return prompt

    def _format_resources(self, resources: List[Resource]) -> str:
        parts: List[str] = []
        for idx, resource in enumerate(sorted(resources, key=lambda r: r.id)[:MAX_RESOURCES], start=1):
            parts.append(self._summarize_resource(idx, resource))
        return "\n\n".join(part for part in parts if part)

    def _summarize_resource(self, idx: int, resource: Resource) -> str:
        platform = resource.platform or "onbekend"
        content_format = resource.content_format or "unknown"
        domain = resource.domain or "onbekend"
        status = resource.extraction_status or "unknown"

        desc = resource.description or ""
        extracted = resource.extracted_text or ""

        if desc:
            desc = self._excerpt(desc, MAX_RESOURCE_CHARS // 4)
        if extracted:
            extracted = self._excerpt(extracted, MAX_RESOURCE_CHARS)

        summary_lines = [
            f"{idx}) Platform: {platform} | Format: {content_format} | Domein: {domain} | Status: {status}",
            f"URL: {resource.canonical_url or resource.final_url or resource.url}",
        ]
        if resource.title:
            summary_lines.append(f"Titel: {resource.title}")
        if desc:
            summary_lines.append(f"Korte beschrijving: {desc}")
        if extracted:
            summary_lines.append(f"Tekstfragment: {extracted}")
        if status != "success" and resource.extraction_error:
            summary_lines.append(f"Opmerking: {resource.extraction_error}")

        return "\n".join(summary_lines)

    @staticmethod
    def _excerpt(text: str, limit: int) -> str:
        if len(text) <= limit:
            return text.strip()
        return text[:limit].strip() + "…"


SYSTEM_PROMPT = dedent(
    """
    Jij bent een AI-assistent die aantekeningen verrijkt voor een persoonlijk kennisarchief.
    Classificeer het bericht in één van: Idee, Taak, Inspiratie, Link/Resource, Reflectie, Overig.
    Vat het bericht in maximaal 2-3 zinnen samen.
    Lever 3-5 kerninzichten/opportuniteiten en 3-6 tags.
    Kies prioriteit uit Hoog, Medium, Laag.
    Geef aan of een actie vereist is, met een concrete suggestie als dat zo is.
    Leg kort uit waarom dit bericht relevant is voor de gebruiker.
    Schrijf alles in het Nederlands.
    Output moet strikt JSON zijn.
    """
).strip()
