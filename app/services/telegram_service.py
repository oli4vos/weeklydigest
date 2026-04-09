"""Telegram intake and onboarding service."""
from __future__ import annotations

import hashlib
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from telegram import KeyboardButton, Message, ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from app.config import get_settings
from app.db import get_session
from app.models import RawMessage, User
from app.repositories.user_repository import UserRepository
from app.services.email_service import EmailService

LINK_PATTERN = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
CODE_PATTERN = re.compile(r"^\d{6}$")

STATUS_NEW = "new"
STATUS_AWAITING_EMAIL = "awaiting_email"
STATUS_AWAITING_EMAIL_VERIFICATION = "awaiting_email_verification"
STATUS_ACTIVE = "active"
STATUS_AWAITING_PHONE = "awaiting_phone"


@dataclass
class OutboundMessage:
    text: str
    ask_contact: bool = False


@dataclass
class ProcessingResult:
    handled: bool
    stored: bool
    responses: list[OutboundMessage]
    onboarding_status: str


class TelegramService:
    """Encapsulates polling/webhook intake, onboarding, and persistence."""

    def __init__(self, bot_token: str) -> None:
        if not bot_token:
            raise ValueError("Telegram bot token must be provided")
        self.bot_token = bot_token
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)
        self._verification_ttl = timedelta(minutes=self.settings.email_verification_code_ttl_minutes)

    def run(self) -> None:
        """Start the polling bot and block until it is stopped."""
        application = self._build_application()
        self.logger.info("Starting Telegram polling bot...")
        application.run_polling()

    def _build_application(self) -> Application:
        application = Application.builder().token(self.bot_token).build()
        application.add_handler(CommandHandler("start", self._handle_start))
        application.add_handler(CommandHandler("resend", self._handle_resend))
        application.add_handler(MessageHandler(filters.CONTACT, self._handle_contact))
        application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), self._handle_text_message))
        application.add_error_handler(self._handle_error)
        return application

    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if not message:
            return
        await self._handle_message_for_polling(message, text="/start")

    async def _handle_contact(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if not message:
            return
        await self._handle_message_for_polling(message)

    async def _handle_resend(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.effective_message
        if not message:
            return
        await self._handle_message_for_polling(message, text="/resend")

    async def _handle_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.message
        if not message or not message.text:
            return
        await self._handle_message_for_polling(message, text=message.text.strip())

    async def _handle_message_for_polling(self, message: Message, *, text: Optional[str] = None) -> None:
        telegram_user = message.from_user
        if telegram_user is None:
            return
        result = self._process_event(
            telegram_user_id=str(telegram_user.id),
            chat_id=str(message.chat_id),
            username=telegram_user.username,
            display_name=self._format_sender_name(message),
            external_message_id=str(message.message_id),
            text=text if text is not None else (message.text or "").strip() or None,
            contact_phone=(message.contact.phone_number if message.contact else None),
            contact_user_id=(str(message.contact.user_id) if message.contact and message.contact.user_id else None),
            received_at=self._normalize_datetime(message.date),
        )
        for outbound in result.responses:
            kwargs = {}
            if outbound.ask_contact:
                kwargs["reply_markup"] = ReplyKeyboardMarkup(
                    [[KeyboardButton("Deel telefoonnummer (optioneel)", request_contact=True)]],
                    resize_keyboard=True,
                    one_time_keyboard=True,
                )
            await message.reply_text(outbound.text, **kwargs)

    async def _handle_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        self.logger.exception("Telegram polling error: %s", context.error)

    def _process_event(
        self,
        *,
        telegram_user_id: str,
        chat_id: str,
        username: Optional[str],
        display_name: Optional[str],
        external_message_id: Optional[str],
        text: Optional[str],
        contact_phone: Optional[str],
        contact_user_id: Optional[str],
        received_at: datetime,
    ) -> ProcessingResult:
        responses: list[OutboundMessage] = []
        stored = False
        with get_session() as session:
            user = UserRepository.find_or_create_from_telegram(
                session=session,
                telegram_user_id=telegram_user_id,
                chat_id=chat_id,
                username=username,
                display_name=display_name,
            )
            user.last_seen_at = received_at
            if not user.onboarding_status:
                user.onboarding_status = STATUS_NEW

            if contact_phone:
                responses.extend(self._handle_contact_share(user, contact_phone, contact_user_id, telegram_user_id))
                return ProcessingResult(True, stored, responses, user.onboarding_status)

            normalized_text = (text or "").strip()
            if not normalized_text:
                return ProcessingResult(False, stored, responses, user.onboarding_status)

            lowered = normalized_text.lower()
            if lowered == "/start":
                responses.extend(self._handle_start_command(user))
                return ProcessingResult(True, stored, responses, user.onboarding_status)
            if lowered.startswith("/resend"):
                responses.extend(self._handle_resend_command(user))
                return ProcessingResult(True, stored, responses, user.onboarding_status)

            if lowered.startswith("/") and lowered != "/start":
                responses.append(
                    OutboundMessage(
                        "Onbekend commando. Gebruik /start om onboarding te zien of /resend voor een nieuwe verificatiecode."
                    )
                )
                return ProcessingResult(True, stored, responses, user.onboarding_status)

            if not user.email_verified:
                handled, pre_verified_stored, onboarding_responses = self._handle_pre_verification_text(
                    session=session,
                    user=user,
                    text=normalized_text,
                    chat_id=chat_id,
                    display_name=display_name,
                    external_message_id=external_message_id,
                    received_at=received_at,
                )
                responses.extend(onboarding_responses)
                if handled:
                    return ProcessingResult(True, pre_verified_stored, responses, user.onboarding_status)
                return ProcessingResult(False, pre_verified_stored, responses, user.onboarding_status)

            user.onboarding_status = STATUS_ACTIVE
            stored = self._store_raw_message(
                session=session,
                user=user,
                chat_id=chat_id,
                display_name=display_name,
                external_message_id=external_message_id,
                text=normalized_text,
                received_at=received_at,
            )
        return ProcessingResult(True, stored, responses, STATUS_ACTIVE)

    def _handle_start_command(self, user: User) -> list[OutboundMessage]:
        responses: list[OutboundMessage] = []
        first_name = (user.display_name or "daar").split(" ")[0]
        if user.email_verified:
            user.onboarding_status = STATUS_ACTIVE
            responses.append(
                OutboundMessage(
                    f"Welkom terug {first_name}! Je account is actief. Stuur gerust berichten; ik neem ze mee in je digest."
                )
            )
            if not user.phone_verified:
                responses.append(
                    OutboundMessage(
                        "Wil je optioneel je telefoonnummer delen voor je profiel?",
                        ask_contact=True,
                    )
                )
            return responses

        if user.onboarding_status in {STATUS_NEW, STATUS_AWAITING_EMAIL} or not user.email:
            user.onboarding_status = STATUS_AWAITING_EMAIL
            responses.append(
                OutboundMessage(
                    "Welkom! 👋\nIk help je met een persoonlijke digest.\nStuur eerst je e-mailadres, dan activeer ik je account."
                )
            )
            responses.append(
                OutboundMessage(
                    "Na verificatie ontvang je je eigen daily/weekly digest. Je berichten worden nu al opgeslagen."
                )
            )
        elif user.onboarding_status in {STATUS_AWAITING_EMAIL_VERIFICATION, STATUS_AWAITING_PHONE}:
            user.onboarding_status = STATUS_AWAITING_EMAIL_VERIFICATION
            responses.append(
                OutboundMessage(
                    f"Je e-mailadres ({self._mask_email(user.email)}) staat al klaar.\n"
                    "Stuur de 6-cijferige verificatiecode uit je mail, of gebruik /resend."
                )
            )
        else:
            user.onboarding_status = STATUS_AWAITING_EMAIL
            responses.append(OutboundMessage("Stuur je e-mailadres om onboarding af te ronden."))
        responses.append(
            OutboundMessage(
                "Optioneel: deel je telefoonnummer via de knop hieronder.",
                ask_contact=True,
            )
        )
        return responses

    def _handle_resend_command(self, user: User) -> list[OutboundMessage]:
        responses: list[OutboundMessage] = []
        if not user.email:
            user.onboarding_status = STATUS_AWAITING_EMAIL
            responses.append(
                OutboundMessage("Ik heb nog geen e-mailadres. Stuur eerst je e-mailadres.")
            )
            return responses
        user.email_verified = False
        user.onboarding_status = STATUS_AWAITING_EMAIL_VERIFICATION
        self._issue_and_send_verification_code(user, responses)
        return responses

    def _handle_pre_verification_text(
        self,
        *,
        session,
        user: User,
        text: str,
        chat_id: str,
        display_name: Optional[str],
        external_message_id: Optional[str],
        received_at: datetime,
    ) -> tuple[bool, bool, list[OutboundMessage]]:
        responses: list[OutboundMessage] = []
        lowered = text.lower()
        if EMAIL_PATTERN.match(text):
            user.email = text.strip().lower()
            user.email_verified = False
            user.onboarding_status = STATUS_AWAITING_EMAIL_VERIFICATION
            responses.append(
                OutboundMessage(
                    f"Top, e-mailadres ontvangen: {self._mask_email(user.email)}."
                )
            )
            self._issue_and_send_verification_code(user, responses)
            return True, False, responses

        if CODE_PATTERN.match(text) and user.onboarding_status == STATUS_AWAITING_EMAIL_VERIFICATION:
            verification_state = self._verify_code(user, text)
            if verification_state == "ok":
                user.email_verified = True
                user.onboarding_status = STATUS_ACTIVE
                user.email_verification_token_hash = None
                user.email_verification_sent_at = None
                responses.append(
                    OutboundMessage("✅ Code klopt. Je e-mailadres is geverifieerd.")
                )
                responses.append(
                    OutboundMessage("Je account is nu actief. Nieuwe berichten worden automatisch verwerkt voor je digest.")
                )
                if not user.phone_verified:
                    responses.append(
                        OutboundMessage("Optioneel: deel je telefoonnummer via de knop.", ask_contact=True)
                    )
            elif verification_state == "expired":
                responses.append(
                    OutboundMessage(
                        "⏳ Deze code is verlopen. Gebruik /resend voor een nieuwe code."
                    )
                )
            else:
                responses.append(
                    OutboundMessage(
                        "❌ Die code klopt niet. Controleer de 6 cijfers of gebruik /resend."
                    )
                )
            return True, False, responses

        if lowered in {"resend", "opnieuw", "stuur code", "nieuw code", "/resend"}:
            responses.extend(self._handle_resend_command(user))
            return True, False, responses

        if user.onboarding_status == STATUS_AWAITING_EMAIL and ("@" in text or "mail" in lowered):
            responses.append(
                OutboundMessage(
                    "Dat lijkt geen geldig e-mailadres. Gebruik bijvoorbeeld naam@domein.nl."
                )
            )
            return True, False, responses

        if CODE_PATTERN.match(text) and user.onboarding_status != STATUS_AWAITING_EMAIL_VERIFICATION:
            responses.append(
                OutboundMessage(
                    "Ik verwacht nu eerst je e-mailadres. Daarna kun je de verificatiecode invoeren."
                )
            )
            return True, False, responses

        # Store regular content messages even before verification.
        stored = self._store_raw_message(
            session=session,
            user=user,
            chat_id=chat_id,
            display_name=display_name,
            external_message_id=external_message_id,
            text=text,
            received_at=received_at,
        )
        if not user.email:
            user.onboarding_status = STATUS_AWAITING_EMAIL
            responses.append(
                OutboundMessage(
                    "Bericht opgeslagen. Rond onboarding af door je e-mailadres te sturen."
                )
            )
        else:
            user.onboarding_status = STATUS_AWAITING_EMAIL_VERIFICATION
            responses.append(
                OutboundMessage(
                    "Bericht opgeslagen. Verifieer je e-mail met de 6-cijferige code of gebruik /resend."
                )
            )
        return True, stored, responses

    def _handle_contact_share(
        self,
        user: User,
        contact_phone: str,
        contact_user_id: Optional[str],
        telegram_user_id: str,
    ) -> list[OutboundMessage]:
        responses: list[OutboundMessage] = []
        if contact_user_id and contact_user_id != telegram_user_id:
            responses.append(
                OutboundMessage("Gebruik je eigen contactkaart; dit nummer kan ik nu niet verifiëren.")
            )
            return responses
        user.phone_number = contact_phone
        user.phone_verified = True
        if user.email_verified:
            user.onboarding_status = STATUS_ACTIVE
        elif user.onboarding_status == STATUS_NEW:
            user.onboarding_status = STATUS_AWAITING_EMAIL
        responses.append(OutboundMessage("Dank! Je telefoonnummer is opgeslagen als optioneel profielveld."))
        return responses

    def _issue_and_send_verification_code(self, user: User, responses: list[OutboundMessage]) -> None:
        code = f"{secrets.randbelow(1_000_000):06d}"
        user.email_verification_token_hash = self._hash_code(user.id, code)
        user.email_verification_sent_at = datetime.now(timezone.utc)
        try:
            EmailService(settings=self.settings).send_verification_code(
                email_address=user.email or "",
                code=code,
                display_name=user.display_name,
            )
            responses.append(
                OutboundMessage(
                    "📩 Verificatiecode verstuurd. Stuur hier de 6 cijfers terug. Oude codes zijn nu ongeldig."
                )
            )
        except Exception as exc:  # pragma: no cover
            self.logger.exception("Failed to send verification email for user %s: %s", user.id, exc)
            responses.append(
                OutboundMessage(
                    "Ik kon nu geen verificatiecode mailen. Probeer straks opnieuw met /resend."
                )
            )

    def _verify_code(self, user: User, provided_code: str) -> str:
        if not user.email_verification_token_hash or not user.email_verification_sent_at:
            return "missing"
        sent_at = self._normalize_datetime(user.email_verification_sent_at)
        if datetime.now(timezone.utc) > sent_at + self._verification_ttl:
            return "expired"
        expected = self._hash_code(user.id, provided_code)
        if secrets.compare_digest(expected, user.email_verification_token_hash):
            return "ok"
        return "invalid"

    @staticmethod
    def _hash_code(user_id: int, code: str) -> str:
        digest = hashlib.sha256(f"{user_id}:{code}".encode("utf-8"))
        return digest.hexdigest()

    @staticmethod
    def _mask_email(email: Optional[str]) -> str:
        value = (email or "").strip()
        if not value or "@" not in value:
            return "onbekend e-mailadres"
        local, domain = value.split("@", 1)
        if len(local) <= 2:
            local_masked = f"{local[0]}*" if local else "*"
        else:
            local_masked = f"{local[0]}{'*' * (len(local) - 2)}{local[-1]}"
        return f"{local_masked}@{domain}"

    @staticmethod
    def _store_raw_message(
        *,
        session,
        user: User,
        chat_id: str,
        display_name: Optional[str],
        external_message_id: Optional[str],
        text: str,
        received_at: datetime,
    ) -> bool:
        clean_text = (text or "").strip()
        if not clean_text:
            return False
        contains_link = bool(LINK_PATTERN.search(clean_text))
        raw_message = RawMessage(
            user_id=user.id,
            source="telegram",
            external_message_id=external_message_id or None,
            chat_id=chat_id,
            sender_name=display_name,
            text=clean_text,
            received_at=received_at,
            contains_link=contains_link,
        )
        session.add(raw_message)
        return True

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
        """Persist and respond to a Telegram update received via webhook."""
        settings = get_settings()
        message = payload.get("message") or payload.get("edited_message")
        if not message:
            return False

        from_user = message.get("from") or {}
        chat = message.get("chat") or {}
        telegram_user_id = from_user.get("id")
        chat_id = chat.get("id")
        if telegram_user_id is None or chat_id is None:
            logging.getLogger(__name__).warning("Webhook payload missing user/chat identifiers")
            return False

        text = (message.get("text") or "").strip() or None
        contact = message.get("contact") or {}
        contact_phone = contact.get("phone_number")
        contact_user_id = str(contact.get("user_id")) if contact.get("user_id") is not None else None
        service = cls(bot_token=settings.telegram_bot_token or "")
        result = service._process_event(
            telegram_user_id=str(telegram_user_id),
            chat_id=str(chat_id),
            username=from_user.get("username"),
            display_name=cls._format_sender_name_from_dict(from_user),
            external_message_id=str(message.get("message_id") or ""),
            text=text,
            contact_phone=contact_phone,
            contact_user_id=contact_user_id,
            received_at=cls._normalize_datetime(cls._from_timestamp(message.get("date"))),
        )
        if result.responses:
            service._send_webhook_responses(chat_id=str(chat_id), responses=result.responses)
        return result.handled

    def _send_webhook_responses(self, *, chat_id: str, responses: list[OutboundMessage]) -> None:
        if not self.bot_token:
            return
        api_base = f"https://api.telegram.org/bot{self.bot_token}"
        timeout = httpx.Timeout(10.0)
        with httpx.Client(timeout=timeout) as client:
            for outbound in responses:
                payload: dict[str, object] = {
                    "chat_id": chat_id,
                    "text": outbound.text,
                }
                if outbound.ask_contact:
                    payload["reply_markup"] = {
                        "keyboard": [[{"text": "Deel telefoonnummer (optioneel)", "request_contact": True}]],
                        "resize_keyboard": True,
                        "one_time_keyboard": True,
                    }
                try:
                    client.post(f"{api_base}/sendMessage", json=payload)
                except Exception:  # pragma: no cover
                    self.logger.exception("Failed to send Telegram response to chat %s", chat_id)

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
