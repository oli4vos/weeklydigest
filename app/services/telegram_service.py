"""Telegram intake service that persists incoming text messages."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Optional

from telegram import Message, Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

from app.db import get_session
from app.models import RawMessage
from app.repositories.user_repository import UserRepository

LINK_PATTERN = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)


class TelegramService:
    """Encapsulates polling logic and persistence for a Telegram bot."""

    def __init__(self, bot_token: str) -> None:
        if not bot_token:
            raise ValueError("Telegram bot token must be provided")
        self.bot_token = bot_token
        self.logger = logging.getLogger(__name__)

    def run(self) -> None:
        """Start the polling bot and block until it is stopped."""
        application = self._build_application()
        self.logger.info("Starting Telegram polling bot...")
        application.run_polling()

    def _build_application(self) -> Application:
        application = Application.builder().token(self.bot_token).build()
        application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self._handle_text_message))
        application.add_error_handler(self._handle_error)
        return application

    async def _handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.message
        if not message or not message.text:
            return

        text = message.text.strip()
        if not text:
            return

        try:
            self._store_message(message, text)
        except Exception:  # pragma: no cover - defensive logging
            self.logger.exception("Failed to persist Telegram message %s", getattr(message, "message_id", "?"))

    def _store_message(self, message: Message, text: str) -> None:
        telegram_user = message.from_user
        if telegram_user is None:
            self.logger.warning("Skipping message %s without Telegram user", getattr(message, "message_id", "?"))
            return

        received_at = self._normalize_datetime(message.date)
        contains_link = bool(LINK_PATTERN.search(text))
        sender_name = self._format_sender_name(message)
        telegram_user_id = str(telegram_user.id)
        chat_id = str(message.chat_id)
        username = telegram_user.username

        self._persist_record(
            telegram_user_id=telegram_user_id,
            chat_id=chat_id,
            username=username,
            display_name=sender_name,
            external_message_id=str(message.message_id),
            text=text,
            received_at=received_at,
            contains_link=contains_link,
        )
        self.logger.info(
            "Stored Telegram message id=%s chat=%s contains_link=%s",
            message.message_id,
            message.chat_id,
            contains_link,
        )

    async def _handle_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        self.logger.exception("Telegram polling error: %s", context.error)

    @staticmethod
    def _normalize_datetime(value: Optional[datetime]) -> datetime:
        if value is None:
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _format_sender_name(message: Message) -> Optional[str]:
        user = message.from_user
        if user is None:
            return None
        full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
        if full_name:
            return full_name
        if user.username:
            return user.username
        if user.id:
            return str(user.id)
        return None

    @classmethod
    def store_from_payload(cls, payload: dict) -> bool:
        """Persist a Telegram update received via webhook."""
        message = payload.get("message") or payload.get("edited_message")
        if not message:
            return False

        text = (message.get("text") or "").strip()
        if not text:
            return False

        from_user = message.get("from") or {}
        chat = message.get("chat") or {}
        telegram_user_id = from_user.get("id")
        chat_id = chat.get("id")
        if telegram_user_id is None or chat_id is None:
            logging.getLogger(__name__).warning("Webhook payload missing user/chat identifiers")
            return False

        sender_name = cls._format_sender_name_from_dict(from_user)
        username = from_user.get("username")
        contains_link = bool(LINK_PATTERN.search(text))
        timestamp = message.get("date")
        received_at = cls._normalize_datetime(cls._from_timestamp(timestamp))
        cls._persist_record(
            telegram_user_id=str(telegram_user_id),
            chat_id=str(chat_id),
            username=username,
            display_name=sender_name,
            external_message_id=str(message.get("message_id") or ""),
            text=text,
            received_at=received_at,
            contains_link=contains_link,
        )
        return True

    @staticmethod
    def _persist_record(
        *,
        telegram_user_id: str,
        chat_id: str,
        username: Optional[str],
        display_name: Optional[str],
        external_message_id: str,
        text: str,
        received_at: datetime,
        contains_link: bool,
    ) -> None:
        """Shared persistence helper for polling + webhook flows."""
        logger = logging.getLogger(__name__)
        with get_session() as session:
            user = UserRepository.find_or_create_from_telegram(
                session=session,
                telegram_user_id=telegram_user_id,
                chat_id=chat_id,
                username=username,
                display_name=display_name,
            )
            raw_message = RawMessage(
                user_id=user.id,
                source="telegram",
                external_message_id=external_message_id or None,
                chat_id=chat_id,
                sender_name=display_name,
                text=text,
                received_at=received_at,
                contains_link=contains_link,
            )
            session.add(raw_message)
        logger.info("Stored Telegram webhook message chat=%s contains_link=%s", chat_id, contains_link)

    @staticmethod
    def _from_timestamp(timestamp: Optional[int]) -> datetime:
        if timestamp is None:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    @staticmethod
    def _format_sender_name_from_dict(user_dict: dict) -> Optional[str]:
        full_name = " ".join(
            part
            for part in [
                user_dict.get("first_name"),
                user_dict.get("last_name"),
            ]
            if part
        ).strip()
        if full_name:
            return full_name
        if user_dict.get("username"):
            return user_dict["username"]
        if user_dict.get("id"):
            return str(user_dict["id"])
        return None
