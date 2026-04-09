"""Repository helpers for user records."""
from __future__ import annotations

from typing import Optional

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.models import User


class UserRepository:
    """Persistence utilities for user lookups."""

    @staticmethod
    def find_by_telegram_user_id(session: Session, telegram_user_id: str) -> Optional[User]:
        stmt: Select[User] = select(User).where(User.telegram_user_id == telegram_user_id)
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def find_or_create_from_telegram(
        session: Session,
        telegram_user_id: str,
        chat_id: str,
        username: Optional[str],
        display_name: Optional[str],
    ) -> User:
        user = UserRepository.find_by_telegram_user_id(session, telegram_user_id)
        if user:
            updated = False
            if chat_id and user.telegram_chat_id != chat_id:
                user.telegram_chat_id = chat_id
                updated = True
            if username and user.telegram_username != username:
                user.telegram_username = username
                updated = True
            if display_name and user.display_name != display_name:
                user.display_name = display_name
                updated = True
            if updated:
                session.flush()
            return user

        user = User(
            telegram_user_id=telegram_user_id,
            telegram_chat_id=chat_id,
            telegram_username=username,
            display_name=display_name,
        )
        session.add(user)
        session.flush()
        return user
