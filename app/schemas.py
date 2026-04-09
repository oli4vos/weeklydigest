"""Pydantic schemas for future service and API boundaries."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

try:  # Pydantic v2
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - fallback for Pydantic v1
    ConfigDict = None  # type: ignore


class ORMModel(BaseModel):
    """Compatibility base-class that works with both Pydantic v1 and v2."""

    if ConfigDict is not None:  # Pydantic v2 style
        model_config = ConfigDict(from_attributes=True)  # type: ignore[assignment]
    else:  # pragma: no cover
        class Config:
            orm_mode = True


class UserBase(BaseModel):
    telegram_user_id: str
    telegram_chat_id: str
    telegram_username: Optional[str] = None
    display_name: Optional[str] = None
    email: Optional[str] = None
    email_verified: bool = False
    email_verification_token_hash: Optional[str] = None
    email_verification_sent_at: Optional[datetime] = None
    timezone: str = "Europe/Amsterdam"
    is_active: bool = True


class User(ORMModel, UserBase):
    id: int
    created_at: datetime
    updated_at: datetime


class RawMessageBase(BaseModel):
    source: str
    external_message_id: Optional[str] = None
    chat_id: Optional[str] = None
    sender_name: Optional[str] = None
    text: str
    received_at: Optional[datetime] = None
    contains_link: bool = False


class RawMessageCreate(RawMessageBase):
    pass


class RawMessage(ORMModel, RawMessageBase):
    id: int
    user_id: int
    created_at: datetime


class ResourceBase(BaseModel):
    raw_message_id: int
    url: str
    final_url: Optional[str] = None
    title: Optional[str] = None
    domain: Optional[str] = None
    fetched_at: Optional[datetime] = None
    status: Optional[str] = None
    raw_html_path: Optional[str] = None
    extracted_text: Optional[str] = None
    platform: Optional[str] = None
    content_format: Optional[str] = None
    extraction_method: Optional[str] = None
    author: Optional[str] = None
    description: Optional[str] = None
    canonical_url: Optional[str] = None
    raw_metadata_json: Optional[Dict[str, Any]] = None
    extraction_status: Optional[str] = None
    extraction_error: Optional[str] = None


class ResourceCreate(ResourceBase):
    pass


class Resource(ORMModel, ResourceBase):
    id: int


class KnowledgeItemBase(BaseModel):
    user_id: int
    raw_message_id: Optional[int] = None
    date: Optional[date] = None
    source: Optional[str] = None
    category: Optional[str] = None
    summary: Optional[str] = None
    insights_json: Optional[Dict[str, Any]] = None
    tags_json: Optional[List[str]] = None
    priority: Optional[int] = None
    action_required: bool = False
    action_suggestion: Optional[str] = None
    relevance_reason: Optional[str] = None


class KnowledgeItemCreate(KnowledgeItemBase):
    pass


class KnowledgeItem(ORMModel, KnowledgeItemBase):
    id: int
    created_at: datetime
    updated_at: datetime


class WeeklyReportBase(BaseModel):
    user_id: int
    week_start: date
    week_end: date
    highlights_json: Optional[Dict[str, Any]] = None
    themes_json: Optional[Dict[str, Any]] = None
    ideas_json: Optional[Dict[str, Any]] = None
    actions_json: Optional[Dict[str, Any]] = None
    reflection: Optional[str] = None
    meta_analysis: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None
    sent_at: Optional[datetime] = None
    status: Optional[str] = None


class WeeklyReportCreate(WeeklyReportBase):
    pass


class WeeklyReport(ORMModel, WeeklyReportBase):
    id: int


ALLOWED_CATEGORIES = ["Idee", "Taak", "Inspiratie", "Link/Resource", "Reflectie", "Overig"]
ALLOWED_PRIORITIES = ["Hoog", "Medium", "Laag"]


class EnrichmentPayload(BaseModel):
    """Validated response from the OpenAI enrichment call."""

    category: str = Field(..., description="One of the allowed categories")
    summary: str
    insights: List[str]
    tags: List[str]
    priority: str = Field(..., description="Hoog, Medium of Laag")
    action_required: bool
    action_suggestion: Optional[str] = None
    relevance_reason: str

    def model_post_init(self, __context: Any) -> None:  # type: ignore[override]
        if self.category not in ALLOWED_CATEGORIES:
            raise ValueError(f"category must be one of {ALLOWED_CATEGORIES}")
        if self.priority not in ALLOWED_PRIORITIES:
            raise ValueError(f"priority must be one of {ALLOWED_PRIORITIES}")
        if len(self.insights) > 5:
            self.insights = self.insights[:5]
        if len(self.tags) > 6:
            self.tags = self.tags[:6]
