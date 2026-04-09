"""SQLAlchemy ORM models for the knowledge inbox."""
from __future__ import annotations

from datetime import datetime, date

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, JSON, String, Text, func
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class User(Base):
    """Telegram user representation with contact preferences."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    telegram_user_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    telegram_chat_id: Mapped[str] = mapped_column(String(255), nullable=False)
    telegram_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)
    email_verification_token_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email_verification_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, server_default="Europe/Amsterdam")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="1", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    raw_messages: Mapped[list["RawMessage"]] = relationship("RawMessage", back_populates="user")
    knowledge_items: Mapped[list["KnowledgeItem"]] = relationship("KnowledgeItem", back_populates="user")
    weekly_reports: Mapped[list["WeeklyReport"]] = relationship("WeeklyReport", back_populates="user")


class RawMessage(Base):
    """Incoming raw messages captured from Telegram or other sources."""

    __tablename__ = "raw_messages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    external_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    chat_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    sender_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    contains_link: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    resources: Mapped[list["Resource"]] = relationship(
        "Resource", back_populates="raw_message", cascade="all, delete-orphan"
    )
    knowledge_items: Mapped[list["KnowledgeItem"]] = relationship(
        "KnowledgeItem", back_populates="raw_message", cascade="all, delete-orphan"
    )
    user: Mapped["User"] = relationship("User", back_populates="raw_messages")


class Resource(Base):
    """Metadata about URLs discovered in raw messages."""

    __tablename__ = "resources"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    raw_message_id: Mapped[int] = mapped_column(ForeignKey("raw_messages.id"), nullable=False, index=True)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    final_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    raw_html_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    extracted_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    platform: Mapped[str | None] = mapped_column(String(100), nullable=True)
    content_format: Mapped[str | None] = mapped_column(String(100), nullable=True)
    extraction_method: Mapped[str | None] = mapped_column(String(100), nullable=True)
    author: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    canonical_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    raw_metadata_json: Mapped[dict | None] = mapped_column(MutableDict.as_mutable(JSON), nullable=True)
    extraction_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    extraction_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    raw_message: Mapped[RawMessage] = relationship("RawMessage", back_populates="resources")


class KnowledgeItem(Base):
    """AI-ready structured knowledge extracted from incoming messages."""

    __tablename__ = "knowledge_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    raw_message_id: Mapped[int | None] = mapped_column(ForeignKey("raw_messages.id"), nullable=True, index=True)
    date: Mapped[date | None] = mapped_column(Date, nullable=True)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    insights_json: Mapped[list[str] | None] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=True)
    tags_json: Mapped[list[str] | None] = mapped_column(MutableList.as_mutable(JSON), default=list, nullable=True)
    priority: Mapped[int | None] = mapped_column(Integer, nullable=True)
    action_required: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)
    action_suggestion: Mapped[str | None] = mapped_column(Text, nullable=True)
    relevance_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    raw_message: Mapped[RawMessage | None] = relationship("RawMessage", back_populates="knowledge_items")
    user: Mapped["User"] = relationship("User", back_populates="knowledge_items")


class WeeklyReport(Base):
    """Aggregated weekly digest derived from processed knowledge."""

    __tablename__ = "weekly_reports"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    week_start: Mapped[date] = mapped_column(Date, nullable=False)
    week_end: Mapped[date] = mapped_column(Date, nullable=False)
    source_message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    source_resource_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    highlights_json: Mapped[dict | None] = mapped_column(JSON, default=dict, nullable=True)
    themes_json: Mapped[dict | None] = mapped_column(JSON, default=dict, nullable=True)
    ideas_json: Mapped[dict | None] = mapped_column(JSON, default=dict, nullable=True)
    actions_json: Mapped[dict | None] = mapped_column(JSON, default=dict, nullable=True)
    reflection: Mapped[str | None] = mapped_column(Text, nullable=True)
    meta_analysis: Mapped[str | None] = mapped_column(Text, nullable=True)
    email_subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    user: Mapped["User"] = relationship("User", back_populates="weekly_reports")
