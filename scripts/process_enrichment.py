"""Process raw messages lacking knowledge items and create enrichment."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402
from app.services.enrichment_service import EnrichmentService  # noqa: E402


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def main() -> None:
    configure_logging()
    get_settings()  # ensure env vars are loaded
    service = EnrichmentService()
    stats = service.process_batch(limit=20)
    logging.getLogger(__name__).info("Enrichment stats: %s", stats)
    print(
        "Processed={processed} success={success} failed={failed} skipped={skipped}".format(
            **stats
        )
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Enrichment interrupted by user")
        sys.exit(0)
