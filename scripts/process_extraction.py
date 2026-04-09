"""Run deterministic extraction over pending resources."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402
from app.services.extraction_service import ExtractionService  # noqa: E402


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    get_settings()
    service = ExtractionService()
    stats = service.process(limit=25)
    logging.getLogger(__name__).info("Extraction stats: %s", stats)
    print(
        (
            "Processed={processed} success={success} partial={partial} failed={failed} skipped={skipped} "
            "messages_scanned={messages_scanned} new_link_resources={link_resources_created} "
            "new_plain_resources={plain_resources_created} reclassified={reclassified}"
        ).format(**stats)
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Extraction interrupted by user")
        sys.exit(0)
