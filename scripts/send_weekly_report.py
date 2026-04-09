"""Send an existing weekly digest via SMTP."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402
from app.db import get_session  # noqa: E402
from app.repositories.user_repository import UserRepository  # noqa: E402
from app.repositories.weekly_report_repository import WeeklyReportRepository  # noqa: E402
from app.services.email_service import EmailService  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send a weekly digest email.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--report-id", type=int, help="Specific report ID to send.")
    group.add_argument("--latest", action="store_true", help="Send the latest report for a user.")
    parser.add_argument("--user-id", type=int, help="User ID (required when using --latest).")
    parser.add_argument("--force", action="store_true", help="Send even if the report is already marked as sent.")
    parser.add_argument("--to", type=str, help="Override destination email address.")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    settings = get_settings()
    args = parse_args()
    email_service = EmailService(settings=settings)

    with get_session() as session:
        if args.report_id:
            report = WeeklyReportRepository.get_by_id(session, args.report_id)
        else:
            if not args.user_id:
                raise ValueError("--user-id is required when using --latest")
            report = WeeklyReportRepository.latest_for_user(session, args.user_id)
        if not report:
            raise ValueError("No report found for the provided criteria.")
        if (report.status or "").lower() == "sent" and not args.force:
            raise ValueError("Report already sent. Use --force to resend.")
        user_email = None
        user = UserRepository.find_by_id(session, report.user_id)
        if user and user.email_verified and user.email:
            user_email = user.email
        email_service.send_report(report, to_address=args.to or user_email)
        WeeklyReportRepository.mark_sent(session, report, datetime.now(timezone.utc))
        session.commit()
    logging.info("Report %s sent successfully.", report.id)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        logging.getLogger(__name__).exception("Failed to send report: %s", exc)
        sys.exit(1)
