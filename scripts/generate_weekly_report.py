"""Generate a weekly digest report without sending it."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402
from app.services.digest_service import DigestService  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a weekly digest report.")
    parser.add_argument("--user-id", type=int, required=True, help="User ID to build the report for.")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD). Defaults to today-6 days.")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--days", type=int, default=7, help="Span in days if start/end not provided.")
    parser.add_argument("--force", action="store_true", help="Regenerate even if a report already exists.")
    parser.add_argument("--preview-file", type=str, help="Optional file to write the email body to.")
    return parser.parse_args()


def parse_range(args: argparse.Namespace) -> tuple[date, date]:
    if args.start_date and args.end_date:
        start = date.fromisoformat(args.start_date)
        end = date.fromisoformat(args.end_date)
        return start, end
    end = date.today()
    start = end - timedelta(days=args.days - 1)
    return start, end


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    get_settings()
    args = parse_args()
    start, end = parse_range(args)
    service = DigestService()
    report = service.generate_report(user_id=args.user_id, start=start, end=end, force=args.force)
    logging.info(
        "Report %s generated for user %s covering %s - %s (messages=%s resources=%s)",
        report.id,
        args.user_id,
        start,
        end,
        report.source_message_count,
        report.source_resource_count,
    )
    if args.preview_file:
        Path(args.preview_file).write_text(report.email_body or "", encoding="utf-8")
        logging.info("Email preview written to %s", args.preview_file)
    else:
        print("\n" + (report.email_body or ""))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        logging.getLogger(__name__).exception("Failed to generate report: %s", exc)
        sys.exit(1)
