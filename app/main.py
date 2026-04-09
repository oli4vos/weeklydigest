"""FastAPI entrypoint providing a simple dashboard + webhook endpoints."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
import secrets
from typing import Iterator
from urllib.parse import quote_plus, unquote_plus

from fastapi import Body, Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.config import BASE_DIR, get_settings
from app.db import get_session
from app.models import RawMessage, Resource
from app.repositories.user_repository import UserRepository
from app.repositories.weekly_report_repository import WeeklyReportRepository
from app.services.digest_service import DigestService
from app.services.email_service import EmailService
from app.services.extraction_service import ExtractionService
from app.services.telegram_service import TelegramService
from app.utils.datetime_utils import format_datetime_for_display

settings = get_settings()
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
app = FastAPI(title="Knowledge Inbox")
security = HTTPBasic()
logger = logging.getLogger(__name__)


def _format_datetime_for_template(value: object) -> str:
    """Format datetime values for UI display in APP_TIMEZONE."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return format_datetime_for_display(value)
    return str(value)


templates.env.filters["dt_local"] = _format_datetime_for_template


def get_db_session() -> Iterator[Session]:
    with get_session() as session:
        yield session


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    expected_user = settings.dashboard_username
    expected_pass = settings.dashboard_password
    if not (
        secrets.compare_digest(credentials.username, expected_user)
        and secrets.compare_digest(credentials.password, expected_pass)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


@app.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    db: Session = Depends(get_db_session),
    _: None = Depends(require_auth),
) -> HTMLResponse:
    reports = WeeklyReportRepository.list_recent(db, limit=10)
    recent_messages = (
        db.query(RawMessage)
        .order_by(RawMessage.received_at.desc(), RawMessage.id.desc())
        .limit(10)
        .all()
    )
    recent_resources = (
        db.query(Resource)
        .order_by(Resource.id.desc())
        .limit(10)
        .all()
    )
    recent_users = UserRepository.list_recent(db, limit=10)
    user_count = UserRepository.count_all(db)
    digest_recipients = UserRepository.list_digest_recipients(db)
    status_rows = (
        db.query(Resource.extraction_status)
        .filter(Resource.extraction_status.isnot(None))
        .order_by(Resource.id.desc())
        .limit(50)
        .all()
    )
    extraction_counts = {"success": 0, "partial": 0, "failed": 0}
    for row in status_rows:
        key = (row[0] or "").lower()
        if key in extraction_counts:
            extraction_counts[key] += 1
    latest_extraction = (
        db.query(Resource)
        .filter(Resource.extraction_status.isnot(None))
        .order_by(Resource.id.desc())
        .first()
    )
    latest_digest = reports[0] if reports else None
    message = request.query_params.get("message")
    if message:
        message = unquote_plus(message)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "reports": reports,
            "recent_messages": recent_messages,
            "recent_resources": recent_resources,
            "latest_digest": latest_digest,
            "user_count": user_count,
            "digest_recipient_count": len(digest_recipients),
            "recent_users": recent_users,
            "extraction_counts": extraction_counts,
            "extraction_total": len(status_rows),
            "latest_extraction": latest_extraction,
            "message": message,
        },
    )


@app.get("/reports", response_class=HTMLResponse)
def reports(
    request: Request,
    db: Session = Depends(get_db_session),
    _: None = Depends(require_auth),
) -> HTMLResponse:
    reports = WeeklyReportRepository.list_recent(db, limit=50)
    return templates.TemplateResponse(
        "reports.html",
        {
            "request": request,
            "reports": reports,
        },
    )


@app.get("/reports/{report_id}", response_class=HTMLResponse)
def report_detail(
    report_id: int,
    request: Request,
    db: Session = Depends(get_db_session),
    _: None = Depends(require_auth),
) -> HTMLResponse:
    report = WeeklyReportRepository.get_by_id(db, report_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return templates.TemplateResponse(
        "report_detail.html",
        {
            "request": request,
            "report": report,
        },
    )


@app.post("/run/daily")
def run_daily(_: None = Depends(require_auth)) -> RedirectResponse:
    result = _run_digest(days=1)
    return RedirectResponse(url=f"/?message={quote_plus(result['message'])}", status_code=303)


@app.post("/run/weekly")
def run_weekly(_: None = Depends(require_auth)) -> RedirectResponse:
    result = _run_digest(days=7)
    return RedirectResponse(url=f"/?message={quote_plus(result['message'])}", status_code=303)


@app.post("/run/extraction")
def run_extraction(_: None = Depends(require_auth)) -> RedirectResponse:
    service = ExtractionService()
    stats = service.process()
    summary = (
        f"Extraction klaar: {stats['processed']} resources verwerkt "
        f"({stats['success']} success, {stats['failed']} failed)."
    )
    return RedirectResponse(url=f"/?message={quote_plus(summary)}", status_code=303)


@app.post("/telegram/webhook")
def telegram_webhook(payload: dict = Body(...)) -> dict:
    try:
        handled = TelegramService.store_from_payload(payload)
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {"ok": True, "handled": handled}


@app.get("/health")
def health() -> dict:
    return {"ok": True}


def require_internal_token(request: Request) -> None:
    token = settings.internal_trigger_token
    presented = request.headers.get("X-Internal-Token")
    if not token or not presented or not secrets.compare_digest(token, presented):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid internal token")


@app.post("/internal/run/daily")
def internal_run_daily(request: Request) -> dict:
    require_internal_token(request)
    result = _run_digest(days=1)
    return result


@app.post("/internal/run/weekly")
def internal_run_weekly(request: Request) -> dict:
    require_internal_token(request)
    result = _run_digest(days=7)
    return result


@app.post("/internal/run/extraction")
def internal_run_extraction(request: Request) -> dict:
    require_internal_token(request)
    service = ExtractionService()
    stats = service.process()
    return {"status": "processed", "message": f"{stats['processed']} resources verwerkt", "stats": stats}


def _run_digest(days: int) -> dict:
    if days < 1:
        raise ValueError("days must be >= 1")
    end = date.today()
    start = end - timedelta(days=days - 1)
    digest_service = DigestService()
    label = "daily" if days == 1 else f"{days}-day"
    base_info: dict[str, object] = {
        "ok": True,
        "status": "processed",
        "start": str(start),
        "end": str(end),
        "processed_users": 0,
        "sent": 0,
        "skipped_empty": 0,
        "failed": 0,
        "reports": [],
    }

    with get_session() as session:
        recipients = UserRepository.list_digest_recipients(session)
    if not recipients:
        base_info["status"] = "skipped_no_recipients"
        base_info["message"] = "Geen geverifieerde gebruikers met e-mailadres gevonden."
        return base_info

    email_service = EmailService()
    for user in recipients:
        base_info["processed_users"] = int(base_info["processed_users"]) + 1
        user_result: dict[str, object] = {"user_id": user.id}
        try:
            report = digest_service.generate_report(
                user_id=user.id,
                start=start,
                end=end,
                force=True,
            )
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to generate digest for user %s: %s", user.id, exc)
            base_info["failed"] = int(base_info["failed"]) + 1
            user_result["status"] = "failed_generate"
            user_result["error"] = str(exc)
            cast_reports = base_info["reports"]
            if isinstance(cast_reports, list):
                cast_reports.append(user_result)
            continue

        user_result["report_id"] = report.id
        if (report.source_message_count or 0) == 0 and (report.source_resource_count or 0) == 0:
            with get_session() as session:
                stored = session.get(type(report), report.id)
                if stored:
                    stored.status = "skipped_empty"
                    stored.sent_at = None
            base_info["skipped_empty"] = int(base_info["skipped_empty"]) + 1
            user_result["status"] = "skipped_empty"
            cast_reports = base_info["reports"]
            if isinstance(cast_reports, list):
                cast_reports.append(user_result)
            continue

        try:
            email_service.send_report(report, to_address=user.email)
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to send digest report=%s user=%s: %s", report.id, user.id, exc)
            with get_session() as session:
                stored = session.get(type(report), report.id)
                if stored:
                    stored.status = "failed"
                    stored.sent_at = None
            base_info["failed"] = int(base_info["failed"]) + 1
            user_result["status"] = "failed_send"
            user_result["error"] = str(exc)
            cast_reports = base_info["reports"]
            if isinstance(cast_reports, list):
                cast_reports.append(user_result)
            continue

        with get_session() as session:
            refreshed = session.get(type(report), report.id)
            if refreshed:
                WeeklyReportRepository.mark_sent(session, refreshed, datetime.now(timezone.utc))
        base_info["sent"] = int(base_info["sent"]) + 1
        user_result["status"] = "sent"
        cast_reports = base_info["reports"]
        if isinstance(cast_reports, list):
            cast_reports.append(user_result)

    if int(base_info["failed"]) > 0:
        base_info["ok"] = False
        base_info["status"] = "failed"
    elif int(base_info["sent"]) == 0 and int(base_info["skipped_empty"]) > 0:
        base_info["status"] = "skipped_empty"
    else:
        base_info["status"] = "sent"
    base_info["message"] = (
        f"{label.capitalize()} digest: {base_info['processed_users']} users, "
        f"{base_info['sent']} sent, {base_info['skipped_empty']} skipped_empty, {base_info['failed']} failed."
    )
    return base_info
