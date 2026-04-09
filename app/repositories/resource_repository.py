"""Repository helpers for resources."""
from __future__ import annotations

from typing import Iterable, List

from sqlalchemy import Select, or_, select
from sqlalchemy.orm import Session, joinedload

from app.models import RawMessage, Resource


class ResourceRepository:
    """Persistence helpers for Resource records."""

    @staticmethod
    def exists_for_url(session: Session, raw_message_id: int, url: str) -> bool:
        stmt: Select[int] = select(Resource.id).where(
            Resource.raw_message_id == raw_message_id,
            Resource.url == url,
        )
        return session.execute(stmt).first() is not None

    @staticmethod
    def add_pending(session: Session, raw_message_id: int, url: str) -> Resource:
        resource = Resource(
            raw_message_id=raw_message_id,
            url=url,
            status="pending",
            extraction_status="pending",
        )
        session.add(resource)
        session.flush()  # assign primary key before returning
        return resource

    @staticmethod
    def ensure_plain_text_resource(session: Session, raw_message: RawMessage) -> Resource:
        url = ResourceRepository._plain_text_url(raw_message.id)
        stmt = select(Resource).where(Resource.raw_message_id == raw_message.id, Resource.url == url)
        existing = session.execute(stmt).scalar_one_or_none()
        if existing:
            return existing
        resource = Resource(
            raw_message_id=raw_message.id,
            url=url,
            final_url=url,
            title=f"Bericht #{raw_message.id}",
            domain="telegram",
            status="plain_text",
            platform="telegram",
            content_format="plain_text",
            extraction_method="direct_text",
            extraction_status="pending",
        )
        session.add(resource)
        session.flush()
        return resource

    @staticmethod
    def get_resources_needing_extraction(session: Session, limit: int) -> List[Resource]:
        stmt = (
            select(Resource)
            .options(joinedload(Resource.raw_message))
            .where(
                or_(
                    Resource.extraction_status.is_(None),
                    Resource.extraction_status.in_(("pending", "partial")),
                )
            )
            .order_by(Resource.id.asc())
            .limit(limit)
        )
        return session.scalars(stmt).all()

    @staticmethod
    def _plain_text_url(raw_message_id: int) -> str:
        return f"telegram://message/{raw_message_id}"

    @staticmethod
    def get_resources_by_url_patterns(session: Session, patterns: Iterable[str]) -> List[Resource]:
        filters = []
        for pattern in patterns:
            like_pattern = f"%{pattern}%"
            filters.append(Resource.url.ilike(like_pattern))
            filters.append(Resource.final_url.ilike(like_pattern))
        if not filters:
            return []
        stmt = select(Resource).where(or_(*filters))
        return session.scalars(stmt).all()
