"""Repository helpers for raw messages."""
from __future__ import annotations

from typing import List, Optional

from sqlalchemy import Select, select
from sqlalchemy.orm import Session, joinedload

from app.models import RawMessage


class RawMessageRepository:
    """Read-only helpers for retrieving raw messages."""

    @staticmethod
    def get_messages_with_links(session: Session, limit: Optional[int] = None) -> List[RawMessage]:
        stmt: Select[RawMessage] = select(RawMessage).where(RawMessage.contains_link.is_(True)).order_by(
            RawMessage.received_at.asc()
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        result = session.scalars(stmt).all()
        return result

    @staticmethod
    def get_messages_without_knowledge(session: Session, limit: int = 20) -> List[RawMessage]:
        stmt: Select[RawMessage] = (
            select(RawMessage)
            .options(joinedload(RawMessage.resources))
            .where(~RawMessage.knowledge_items.any())
            .order_by(RawMessage.received_at.asc())
            .limit(limit)
        )
        return session.execute(stmt).unique().scalars().all()

    @staticmethod
    def get_plain_text_without_resource(session: Session, limit: int = 20) -> List[RawMessage]:
        stmt: Select[RawMessage] = (
            select(RawMessage)
            .where(RawMessage.contains_link.is_(False))
            .where(~RawMessage.resources.any())
            .order_by(RawMessage.received_at.asc())
            .limit(limit)
        )
        return session.scalars(stmt).all()
