"""Repository helpers around weekly reports."""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlalchemy import Select, and_, select
from sqlalchemy.orm import Session

from app.models import WeeklyReport


class WeeklyReportRepository:
    """Data access helpers for weekly reports."""

    @staticmethod
    def find_in_range(session: Session, user_id: int, start: date, end: date) -> Optional[WeeklyReport]:
        stmt: Select[WeeklyReport] = (
            select(WeeklyReport)
            .where(
                WeeklyReport.user_id == user_id,
                WeeklyReport.week_start == start,
                WeeklyReport.week_end == end,
            )
            .order_by(WeeklyReport.created_at.desc(), WeeklyReport.id.desc())
            .limit(1)
        )
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def latest_for_user(session: Session, user_id: int) -> Optional[WeeklyReport]:
        stmt = (
            select(WeeklyReport)
            .where(WeeklyReport.user_id == user_id)
            .order_by(WeeklyReport.week_end.desc())
            .limit(1)
        )
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def list_recent(session: Session, limit: int = 10) -> list[WeeklyReport]:
        stmt = (
            select(WeeklyReport)
            .order_by(WeeklyReport.week_end.desc(), WeeklyReport.id.desc())
            .limit(limit)
        )
        return session.execute(stmt).scalars().all()

    @staticmethod
    def get_by_id(session: Session, report_id: int) -> Optional[WeeklyReport]:
        return session.get(WeeklyReport, report_id)

    @staticmethod
    def create(
        session: Session,
        *,
        user_id: int,
        week_start: date,
        week_end: date,
        data: dict,
    ) -> WeeklyReport:
        report = WeeklyReport(
            user_id=user_id,
            week_start=week_start,
            week_end=week_end,
            **data,
        )
        session.add(report)
        session.flush()
        return report

    @staticmethod
    def mark_sent(session: Session, report: WeeklyReport, sent_at: datetime) -> None:
        report.sent_at = sent_at
        report.status = "sent"
        session.flush()
