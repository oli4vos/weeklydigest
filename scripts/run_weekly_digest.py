"""Convenience script to generate and send a weekly digest in one go."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402
from app.db import get_session  # noqa: E402
from app.repositories.user_repository import UserRepository  # noqa: E402
from app.repositories.weekly_report_repository import WeeklyReportRepository  # noqa: E402
from app.services.digest_service import DigestService  # noqa: E402
from app.services.email_service import EmailService  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and send a weekly digest end-to-end.")
    parser.add_argument("--user-id", type=int, help="Single user ID. Defaults to DEFAULT_USER_ID when omitted.")
    parser.add_argument(
        "--all-verified",
        action="store_true",
        help="Send digests for all users with email_verified=true and a valid email.",
    )
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)
    parser.add_argument("--force", action="store_true", help="Regenerate even if a report exists.")
    parser.add_argument("--dry-run", action="store_true", help="Generate but do not send email.")
    return parser.parse_args()


def parse_range(args: argparse.Namespace) -> tuple[date, date]:
    if args.start_date and args.end_date:
        return date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    end = date.today()
    start = end - timedelta(days=args.days - 1)
    return start, end


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    settings = get_settings()
    args = parse_args()
    start, end = parse_range(args)
    digest_service = DigestService()
    if args.all_verified:
        with get_session() as session:
            target_user_ids = [user.id for user in UserRepository.list_digest_recipients(session)]
    else:
        target_user_ids = [args.user_id or settings.default_user_id]

    if not target_user_ids:
        logging.info("No verified users with email found; nothing to send.")
        return

    email_service = EmailService(settings=settings)
    for user_id in target_user_ids:
        try:
            report = digest_service.generate_report(user_id=user_id, start=start, end=end, force=args.force)
        except ValueError as err:
            logging.info("Digest skipped for user %s: %s", user_id, err)
            continue

        if (report.source_message_count or 0) == 0 and (report.source_resource_count or 0) == 0:
            with get_session() as session:
                refreshed = session.get(type(report), report.id)
                if refreshed:
                    refreshed.status = "skipped_empty"
                    refreshed.sent_at = None
            logging.info("Skipped empty digest for user %s report=%s", user_id, report.id)
            continue

        if args.dry_run:
            print(f"\n--- user_id={user_id} report_id={report.id} ---\n")
            print(report.email_body or "")
            continue

        with get_session() as session:
            user = UserRepository.find_by_id(session, user_id)
            to_address = user.email if user and user.email_verified else None
        if not to_address:
            logging.info("Skipping send for user %s; no verified email.", user_id)
            continue

        email_service.send_report(report, to_address=to_address)
        with get_session() as session:
            refreshed = session.get(type(report), report.id)
            if refreshed:
                WeeklyReportRepository.mark_sent(session, refreshed, datetime.now(timezone.utc))
                session.commit()
        logging.info("Weekly digest generated and sent for user %s.", user_id)

    if args.dry_run:
        logging.info("Dry run complete; reports generated but not sent.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        logging.getLogger(__name__).exception("Weekly digest run failed: %s", exc)
        sys.exit(1)
