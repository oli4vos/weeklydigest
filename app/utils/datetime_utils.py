"""Datetime conversion helpers for UTC storage and local display."""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.config import get_settings


def ensure_utc(dt: datetime) -> datetime:
    """Return a timezone-aware UTC datetime, assuming naive values are UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_app_timezone(dt: datetime, tz_name: str | None = None) -> datetime:
    """Convert a datetime to the configured app timezone for display."""
    settings = get_settings()
    timezone_name = tz_name or settings.app_timezone or "UTC"
    try:
        target_tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        target_tz = timezone.utc
    return ensure_utc(dt).astimezone(target_tz)


def format_datetime_for_display(
    dt: datetime | None,
    *,
    tz_name: str | None = None,
    pattern: str = "%Y-%m-%d %H:%M",
) -> str:
    """Format datetime in app timezone for user-facing UI/email output."""
    if not dt:
        return ""
    return to_app_timezone(dt, tz_name=tz_name).strftime(pattern)

