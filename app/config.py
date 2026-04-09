"""Application configuration helpers."""
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)

DEFAULT_DB_PATH = BASE_DIR / "knowledge.db"


def _default_database_url() -> str:
    """Return the default SQLite URL within the project root."""
    return f"sqlite:///{DEFAULT_DB_PATH}"  # absolute path ensures predictable location


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse common truthy env values."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """Typed view over the runtime configuration."""

    database_url: str = os.getenv("DATABASE_URL", _default_database_url())
    telegram_bot_token: Optional[str] = os.getenv("TELEGRAM_BOT_TOKEN")
    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    openai_digest_enabled: bool = _env_bool("OPENAI_DIGEST_ENABLED", False)
    openai_digest_model: str = os.getenv("OPENAI_DIGEST_MODEL", "gpt-4o-mini")
    openai_digest_max_input_tokens: int = int(os.getenv("OPENAI_DIGEST_MAX_INPUT_TOKENS", "6000"))
    openai_digest_max_output_tokens: int = int(os.getenv("OPENAI_DIGEST_MAX_OUTPUT_TOKENS", "1000"))
    openai_daily_token_limit: int = int(os.getenv("OPENAI_DAILY_TOKEN_LIMIT", "50000"))
    openai_daily_call_limit: int = int(os.getenv("OPENAI_DAILY_CALL_LIMIT", "5"))
    smtp_host: Optional[str] = os.getenv("SMTP_HOST")
    smtp_port: int = int(os.getenv("SMTP_PORT", "587"))
    smtp_username: Optional[str] = os.getenv("SMTP_USERNAME")
    smtp_password: Optional[str] = os.getenv("SMTP_PASSWORD")
    email_from: Optional[str] = os.getenv("EMAIL_FROM")
    email_to: Optional[str] = os.getenv("EMAIL_TO")
    app_timezone: str = os.getenv("APP_TIMEZONE", "UTC")
    default_user_id: int = int(os.getenv("DEFAULT_USER_ID", "1"))
    dashboard_username: str = os.getenv("DASHBOARD_USERNAME", "admin")
    dashboard_password: str = os.getenv("DASHBOARD_PASSWORD", "admin")
    internal_trigger_token: Optional[str] = os.getenv("INTERNAL_TRIGGER_TOKEN")

    def sqlite_path(self) -> Optional[Path]:
        """Return the SQLite file path if the URL targets a local SQLite database."""
        if not self.database_url.startswith("sqlite:///"):
            return None

        relative = self.database_url.replace("sqlite:///", "", 1)
        candidate = Path(relative).expanduser()
        if candidate.is_absolute():
            return candidate.resolve()
        return (BASE_DIR / candidate).resolve()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""
    settings = Settings()
    _validate_settings(settings)
    return settings


def _validate_settings(settings: Settings) -> None:
    """Fail fast when mandatory env vars are missing."""
    missing = []
    required_fields = {
        "DATABASE_URL": settings.database_url,
        "TELEGRAM_BOT_TOKEN": settings.telegram_bot_token,
        "DASHBOARD_USERNAME": settings.dashboard_username,
        "DASHBOARD_PASSWORD": settings.dashboard_password,
        "INTERNAL_TRIGGER_TOKEN": settings.internal_trigger_token,
        "SMTP_HOST": settings.smtp_host,
        "SMTP_PORT": settings.smtp_port,
        "SMTP_USERNAME": settings.smtp_username,
        "SMTP_PASSWORD": settings.smtp_password,
        "EMAIL_FROM": settings.email_from,
        "EMAIL_TO": settings.email_to,
    }
    for key, value in required_fields.items():
        if value in (None, "", []):
            missing.append(key)
    if missing:
        msg = f"Missing required environment variables: {', '.join(missing)}"
        logging.getLogger(__name__).error(msg)
        raise RuntimeError(msg)
