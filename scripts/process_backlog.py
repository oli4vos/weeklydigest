"""Process stored messages with links and populate resources.

The deterministic extraction pipeline already creates link resources automatically;
keep this script around for manual reprocessing/backfill runs.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402
from app.services.link_service import LinkService  # noqa: E402


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    get_settings()  # ensure .env is loaded before processing
    service = LinkService()
    stats = service.process_backlog()
    logging.getLogger(__name__).info(
        "Processed %(messages)s messages, links success=%(links_success)s failed=%(links_failed)s skipped=%(links_skipped)s",
        stats,
    )
    print(
        "Messages={messages} | Links success={links_success} failed={links_failed} skipped={links_skipped}".format(
            **stats
        )
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Link processing interrupted by user")
        sys.exit(0)
