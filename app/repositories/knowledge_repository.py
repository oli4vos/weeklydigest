"""Helpers for knowledge item persistence."""
from __future__ import annotations

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from app.models import KnowledgeItem, RawMessage


class KnowledgeRepository:
    """Query helpers around knowledge items."""

    @staticmethod
    def raw_message_has_item(session: Session, raw_message_id: int) -> bool:
        stmt = select(exists().where(KnowledgeItem.raw_message_id == raw_message_id))
        return session.execute(stmt).scalar() or False

    @staticmethod
    def create_from_enrichment(
        session: Session,
        *,
        raw_message: RawMessage,
        payload: dict,
    ) -> KnowledgeItem:
        item = KnowledgeItem(
            user_id=raw_message.user_id,
            raw_message_id=raw_message.id,
            date=raw_message.received_at.date(),
            source=raw_message.source,
            category=payload["category"],
            summary=payload["summary"],
            insights_json=payload.get("insights"),
            tags_json=payload.get("tags"),
            priority=_priority_to_int(payload.get("priority")),
            action_required=payload.get("action_required", False),
            action_suggestion=payload.get("action_suggestion"),
            relevance_reason=payload.get("relevance_reason"),
        )
        session.add(item)
        session.flush()
        return item


def _priority_to_int(priority: str | None) -> int | None:
    mapping = {"Hoog": 3, "Medium": 2, "Laag": 1}
    return mapping.get(priority or "")
