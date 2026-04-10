"""Per-user data export service for Telegram knowledge inbox."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from app.models import KnowledgeItem, RawMessage, Resource, User, WeeklyReport
from app.utils.datetime_utils import ensure_utc


@dataclass
class ExportBundle:
    """Serializable export payload + file metadata."""

    filename: str
    payload: dict[str, Any]

    def as_json_bytes(self) -> bytes:
        return json.dumps(self.payload, ensure_ascii=False, indent=2).encode("utf-8")


class ExportService:
    """Collects all historical user-scoped data into a JSON export bundle."""

    def build_user_export(self, session: Session, *, user_id: int) -> ExportBundle:
        user = session.get(User, user_id)
        if not user:
            raise ValueError(f"User {user_id} not found")

        raw_messages = (
            session.query(RawMessage)
            .filter(RawMessage.user_id == user_id)
            .order_by(RawMessage.received_at.asc(), RawMessage.id.asc())
            .all()
        )

        resources = (
            session.query(Resource)
            .join(RawMessage, Resource.raw_message_id == RawMessage.id)
            .filter(RawMessage.user_id == user_id)
            .order_by(Resource.id.asc())
            .all()
        )

        weekly_reports = (
            session.query(WeeklyReport)
            .filter(WeeklyReport.user_id == user_id)
            .order_by(WeeklyReport.week_start.asc(), WeeklyReport.id.asc())
            .all()
        )

        knowledge_items = (
            session.query(KnowledgeItem)
            .filter(KnowledgeItem.user_id == user_id)
            .order_by(KnowledgeItem.created_at.asc(), KnowledgeItem.id.asc())
            .all()
        )

        payload: dict[str, Any] = {
            "exported_at": self._serialize_datetime(datetime.now(timezone.utc)),
            "app_version": os.getenv("APP_VERSION", "unknown"),
            "user": self._serialize_user(user),
            "raw_messages": [self._serialize_raw_message(item) for item in raw_messages],
            "resources": [self._serialize_resource(item) for item in resources],
            "weekly_reports": [self._serialize_weekly_report(item) for item in weekly_reports],
            "knowledge_items": [self._serialize_knowledge_item(item) for item in knowledge_items],
            "counts": {
                "raw_messages": len(raw_messages),
                "resources": len(resources),
                "weekly_reports": len(weekly_reports),
                "knowledge_items": len(knowledge_items),
            },
        }

        filename = self._build_filename(user)
        return ExportBundle(filename=filename, payload=payload)

    def _build_filename(self, user: User) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
        raw_telegram_id = (user.telegram_user_id or "unknown").strip()
        safe_telegram_id = "".join(ch for ch in raw_telegram_id if ch.isalnum() or ch in {"-", "_"}) or "unknown"
        return f"knowledge_export_telegram_{safe_telegram_id}_{timestamp}.json"

    def _serialize_user(self, user: User) -> dict[str, Any]:
        return {
            "id": user.id,
            "telegram_user_id": user.telegram_user_id,
            "telegram_chat_id": user.telegram_chat_id,
            "telegram_username": user.telegram_username,
            "display_name": user.display_name,
            "email": user.email,
            "email_verified": bool(user.email_verified),
            "phone_number": user.phone_number,
            "phone_verified": bool(user.phone_verified),
            "onboarding_status": user.onboarding_status,
            "timezone": user.timezone,
            "is_active": bool(user.is_active),
            "created_at": self._serialize_datetime(user.created_at),
            "updated_at": self._serialize_datetime(user.updated_at),
            "last_seen_at": self._serialize_datetime(user.last_seen_at),
        }

    def _serialize_raw_message(self, item: RawMessage) -> dict[str, Any]:
        return {
            "id": item.id,
            "user_id": item.user_id,
            "source": item.source,
            "external_message_id": item.external_message_id,
            "chat_id": item.chat_id,
            "sender_name": item.sender_name,
            "text": item.text,
            "contains_link": bool(item.contains_link),
            "received_at": self._serialize_datetime(item.received_at),
            "created_at": self._serialize_datetime(item.created_at),
        }

    def _serialize_resource(self, item: Resource) -> dict[str, Any]:
        return {
            "id": item.id,
            "raw_message_id": item.raw_message_id,
            "url": item.url,
            "final_url": item.final_url,
            "title": item.title,
            "domain": item.domain,
            "fetched_at": self._serialize_datetime(item.fetched_at),
            "status": item.status,
            "raw_html_path": item.raw_html_path,
            "extracted_text": item.extracted_text,
            "platform": item.platform,
            "content_format": item.content_format,
            "extraction_method": item.extraction_method,
            "author": item.author,
            "description": item.description,
            "canonical_url": item.canonical_url,
            "raw_metadata_json": item.raw_metadata_json,
            "extraction_status": item.extraction_status,
            "extraction_error": item.extraction_error,
        }

    def _serialize_weekly_report(self, item: WeeklyReport) -> dict[str, Any]:
        return {
            "id": item.id,
            "week_start": item.week_start.isoformat() if item.week_start else None,
            "week_end": item.week_end.isoformat() if item.week_end else None,
            "source_message_count": item.source_message_count,
            "source_resource_count": item.source_resource_count,
            "highlights_json": item.highlights_json,
            "themes_json": item.themes_json,
            "ideas_json": item.ideas_json,
            "actions_json": item.actions_json,
            "reflection": item.reflection,
            "meta_analysis": item.meta_analysis,
            "email_subject": item.email_subject,
            "email_body": item.email_body,
            "generated_at": self._serialize_datetime(item.generated_at),
            "sent_at": self._serialize_datetime(item.sent_at),
            "status": item.status,
            "created_at": self._serialize_datetime(item.created_at),
            "updated_at": self._serialize_datetime(item.updated_at),
        }

    def _serialize_knowledge_item(self, item: KnowledgeItem) -> dict[str, Any]:
        return {
            "id": item.id,
            "user_id": item.user_id,
            "raw_message_id": item.raw_message_id,
            "date": item.date.isoformat() if item.date else None,
            "source": item.source,
            "category": item.category,
            "summary": item.summary,
            "insights_json": item.insights_json,
            "tags_json": item.tags_json,
            "priority": item.priority,
            "action_required": bool(item.action_required),
            "action_suggestion": item.action_suggestion,
            "relevance_reason": item.relevance_reason,
            "created_at": self._serialize_datetime(item.created_at),
            "updated_at": self._serialize_datetime(item.updated_at),
        }

    @staticmethod
    def _serialize_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        return ensure_utc(value).isoformat()
