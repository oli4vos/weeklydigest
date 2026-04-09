"""Optional AI enhancement layer for digest quality improvements."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from textwrap import dedent
from typing import Any, Dict, List, Optional

from app.config import get_settings
from app.db import get_session
from app.models import Resource
from app.repositories.weekly_report_repository import WeeklyReportRepository
from app.schemas import AIDigestPayload
from app.utils.datetime_utils import to_app_timezone


DIGEST_JSON_SCHEMA: Dict[str, Any] = {
    "name": "digest_enhancement",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary": {"type": "string"},
            "key_insight": {"type": "string"},
            "highlights": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "title": {"type": "string"},
                        "why_relevant": {"type": "string"},
                    },
                    "required": ["title", "why_relevant"],
                },
            },
            "themes": {"type": "array", "items": {"type": "string"}},
            "ideas": {"type": "array", "items": {"type": "string"}},
            "actions": {"type": "array", "items": {"type": "string"}},
            "reflection": {"type": "string"},
            "meta_analysis": {"type": "string"},
        },
        "required": [
            "summary",
            "key_insight",
            "highlights",
            "themes",
            "ideas",
            "actions",
            "reflection",
            "meta_analysis",
        ],
    },
}

SYSTEM_PROMPT = dedent(
    """
    Je verbetert een bestaande daily/weekly digest op basis van deterministische data.
    Vervang de pipeline niet; verbeter formulering, scherpte en actiegerichtheid.
    Schrijf in het Nederlands, concreet en coachend.
    Houd output compact, scanbaar en consistent.
    Highlights: max 5, acties: max 3, thema's: max 5.
    """
).strip()


@dataclass
class AIDigestResult:
    payload: Optional[AIDigestPayload]
    meta: Dict[str, Any]


class AIDigestService:
    """Runs a single low-cost AI enhancement call per digest."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)
        self.enabled = bool(self.settings.openai_digest_enabled and self.settings.openai_api_key)
        self.model = self.settings.openai_digest_model
        self.client: Any = None
        if self.enabled:
            try:
                from openai import OpenAI  # imported lazily so non-AI mode does not depend on SDK importability

                self.client = OpenAI(api_key=self.settings.openai_api_key)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("OpenAI SDK unavailable; AI digest enhancement disabled: %s", exc)
                self.enabled = False

    def enhance(
        self,
        *,
        user_id: int,
        start: date,
        end: date,
        message_count: int,
        resource_count: int,
        duplicates_filtered_count: int,
        notes: List[Dict[str, Any]],
        resources: List[Resource],
        highlights: List[Dict[str, Any]],
        themes: Dict[str, Any],
        actions: List[str],
        reflection: str,
        meta_analysis: str,
    ) -> AIDigestResult:
        if not is_ai_enabled(self.settings) or not self.enabled or not self.client:
            self.logger.info("AI digest enhancement skipped: disabled or missing OPENAI_API_KEY")
            return AIDigestResult(
                payload=None,
                meta={
                    "enabled": False,
                    "status": "disabled",
                    "reason": "OPENAI_DIGEST_ENABLED is false or OPENAI_API_KEY missing",
                    "model": self.model,
                    "call_made": False,
                    "tokens_total": 0,
                },
            )

        daily_usage = self._daily_usage(user_id=user_id)
        if daily_usage["tokens_total"] > self.settings.openai_daily_token_limit:
            self.logger.info("Skipping AI digest enhancement: token limit reached")
            return self._skip_meta("token limit reached", daily_usage)
        if daily_usage["call_count"] > self.settings.openai_daily_call_limit:
            self.logger.info("Skipping AI digest enhancement: call limit reached")
            return self._skip_meta("call limit reached", daily_usage)

        prompt = self._build_prompt(
            start=start,
            end=end,
            message_count=message_count,
            resource_count=resource_count,
            duplicates_filtered_count=duplicates_filtered_count,
            notes=notes,
            resources=resources,
            highlights=highlights,
            themes=themes,
            actions=actions,
            reflection=reflection,
            meta_analysis=meta_analysis,
        )
        try:
            response = self.client.responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                max_output_tokens=self.settings.openai_digest_max_output_tokens,
                text={"format": {"type": "json_schema", **DIGEST_JSON_SCHEMA}},
            )
            raw_text = getattr(response, "output_text", "") or self._extract_output_text(response)
            payload = AIDigestPayload(**json.loads(raw_text))
            if not self._has_meaningful_content(payload):
                self.logger.warning("AI digest enhancement returned empty payload; falling back to heuristic digest")
                return AIDigestResult(
                    payload=None,
                    meta={
                        "enabled": True,
                        "status": "failed_fallback",
                        "reason": "empty_or_unusable_payload",
                        "model": self.model,
                        "call_made": True,
                        "tokens_total": 0,
                        "daily_tokens_before": daily_usage["tokens_total"],
                        "daily_calls_before": daily_usage["call_count"],
                    },
                )
            usage = self._usage_from_response(response)
            meta = {
                "enabled": True,
                "status": "enhanced",
                "model": self.model,
                "call_made": True,
                "tokens_input": usage["input_tokens"],
                "tokens_output": usage["output_tokens"],
                "tokens_total": usage["total_tokens"],
                "daily_tokens_before": daily_usage["tokens_total"],
                "daily_calls_before": daily_usage["call_count"],
            }
            self.logger.info(
                "AI digest enhancement success (model=%s input_tokens=%s output_tokens=%s)",
                self.model,
                usage["input_tokens"],
                usage["output_tokens"],
            )
            return AIDigestResult(payload=payload, meta=meta)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("AI digest enhancement failed, using heuristic fallback: %s", exc)
            return AIDigestResult(
                payload=None,
                meta={
                    "enabled": True,
                    "status": "failed_fallback",
                    "reason": str(exc),
                    "model": self.model,
                    "call_made": True,
                    "tokens_total": 0,
                    "daily_tokens_before": daily_usage["tokens_total"],
                    "daily_calls_before": daily_usage["call_count"],
                },
            )

    def _skip_meta(self, reason: str, usage: Dict[str, int]) -> AIDigestResult:
        return AIDigestResult(
            payload=None,
            meta={
                "enabled": True,
                "status": "skipped_limit",
                "reason": reason,
                "model": self.model,
                "call_made": False,
                "tokens_total": 0,
                "daily_tokens_before": usage["tokens_total"],
                "daily_calls_before": usage["call_count"],
            },
        )

    def _daily_usage(self, *, user_id: int) -> Dict[str, int]:
        today_local = to_app_timezone(datetime.now(timezone.utc)).date()
        tokens_total = 0
        call_count = 0
        with get_session() as session:
            reports = WeeklyReportRepository.list_recent(session, limit=250)
        for report in reports:
            if report.user_id != user_id or not report.generated_at:
                continue
            report_day = to_app_timezone(report.generated_at).date()
            if report_day != today_local:
                continue
            themes_json = report.themes_json or {}
            if not isinstance(themes_json, dict):
                continue
            ai_meta = themes_json.get("ai_enhancement")
            if not isinstance(ai_meta, dict):
                continue
            tokens_total += int(ai_meta.get("tokens_total") or 0)
            if ai_meta.get("call_made"):
                call_count += 1
        return {"tokens_total": tokens_total, "call_count": call_count}

    def _build_prompt(
        self,
        *,
        start: date,
        end: date,
        message_count: int,
        resource_count: int,
        duplicates_filtered_count: int,
        notes: List[Dict[str, Any]],
        resources: List[Resource],
        highlights: List[Dict[str, Any]],
        themes: Dict[str, Any],
        actions: List[str],
        reflection: str,
        meta_analysis: str,
    ) -> str:
        context = {
            "period": {"start": str(start), "end": str(end)},
            "counts": {
                "source_message_count": message_count,
                "source_resource_count": resource_count,
                "duplicates_filtered_count": duplicates_filtered_count,
            },
            "top_notes": [
                {"text": (note.get("text") or "")[:240], "received_at": note.get("received_at")}
                for note in notes[:6]
            ],
            "top_resources": [self._resource_context(resource) for resource in resources[:6]],
            "heuristic_digest": {
                "highlights": highlights[:5],
                "themes_story": themes.get("story"),
                "themes_platform_story": themes.get("platform_story"),
                "actions": actions[:3],
                "reflection": reflection,
                "meta_analysis": meta_analysis,
            },
        }
        context_json = json.dumps(context, ensure_ascii=False)
        max_chars = self.settings.openai_digest_max_input_tokens * 4
        if len(context_json) > max_chars:
            context_json = context_json[:max_chars] + "... [truncated]"
        return dedent(
            f"""
            Verbeter de digestinhoud op basis van onderstaande JSON-context.
            Houd het compact, actiegericht en niet-generiek.
            Context:
            {context_json}
            """
        ).strip()

    @staticmethod
    def _resource_context(resource: Resource) -> Dict[str, Any]:
        return {
            "title": (resource.title or "")[:160],
            "description": (resource.description or "")[:220],
            "extracted_text_snippet": (resource.extracted_text or "")[:300],
            "platform": resource.platform,
            "content_format": resource.content_format,
            "domain": resource.domain,
            "url": resource.canonical_url or resource.final_url or resource.url,
            "status": resource.extraction_status,
        }

    @staticmethod
    def _extract_output_text(response: Any) -> str:
        output = getattr(response, "output", None) or []
        for item in output:
            for content in getattr(item, "content", []) or []:
                text = getattr(content, "text", None)
                if text:
                    return text
        raise ValueError("No text output returned by OpenAI response")

    @staticmethod
    def _usage_from_response(response: Any) -> Dict[str, int]:
        usage = getattr(response, "usage", None)
        if usage is None:
            return {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
        if isinstance(usage, dict):
            input_tokens = int(usage.get("input_tokens") or 0)
            output_tokens = int(usage.get("output_tokens") or 0)
            total_tokens = int(usage.get("total_tokens") or (input_tokens + output_tokens))
            return {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "total_tokens": total_tokens,
            }
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        total_tokens = int(getattr(usage, "total_tokens", 0) or (input_tokens + output_tokens))
        return {"input_tokens": input_tokens, "output_tokens": output_tokens, "total_tokens": total_tokens}

    @staticmethod
    def _has_meaningful_content(payload: AIDigestPayload) -> bool:
        return bool(
            payload.summary.strip()
            or payload.reflection.strip()
            or payload.actions
            or payload.highlights
            or payload.themes
        )


def is_ai_enabled(settings: Any) -> bool:
    """Return True when digest AI enhancement may run."""
    return bool(getattr(settings, "openai_digest_enabled", False) and getattr(settings, "openai_api_key", None))
