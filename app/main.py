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
from app.repositories.weekly_report_repository import WeeklyReportRepository
from app.services.digest_service import DigestService
from app.services.email_service import EmailService
from app.services.extraction_service import ExtractionService
from app.services.telegram_service import TelegramService

settings = get_settings()
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))
app = FastAPI(title="Knowledge Inbox")
security = HTTPBasic()
logger = logging.getLogger(__name__)


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
    report = digest_service.generate_report(
        user_id=settings.default_user_id,
        start=start,
        end=end,
        force=True,
    )
    label = "daily" if days == 1 else f"{days}-day"
    base_info = {
        "ok": True,
        "status": "sent",
        "report_id": report.id,
        "start": str(start),
        "end": str(end),
    }

    if (report.source_message_count or 0) == 0 and (report.source_resource_count or 0) == 0:
        with get_session() as session:
            stored = session.get(type(report), report.id)
            if stored:
                stored.status = "skipped_empty"
                stored.sent_at = None
        base_info["status"] = "skipped_empty"
        base_info["message"] = (
            f"{label.capitalize()} digest {report.id} over {start} → {end} overgeslagen (geen activity)"
        )
        return base_info

    try:
        EmailService().send_report(report)
    except Exception as exc:  # pragma: no cover
        logger.exception("Failed to send digest %s: %s", report.id, exc)
        base_info["status"] = "failed"
        base_info["ok"] = False
        base_info["message"] = f"{label.capitalize()} digest {report.id} kon niet gestuurd worden: {exc}"
        return base_info

    with get_session() as session:
        refreshed = session.get(type(report), report.id)
        if refreshed:
            WeeklyReportRepository.mark_sent(session, refreshed, datetime.now(timezone.utc))
    base_info["message"] = (
        f"{label.capitalize()} digest {report.id}: mail verstuurd ({start} → {end})"
    )
    return base_info
