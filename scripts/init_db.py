"""Initialize the local SQLite database and create all tables."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import Base, engine  # noqa: E402 - imported after sys.path manipulation
from app import models  # noqa: F401,E402 - ensure metadata registration


def main() -> None:
    """Create all database tables defined in SQLAlchemy models."""
    Base.metadata.create_all(bind=engine)
    db_url = engine.url.render_as_string(hide_password=False)
    print(f"Database initialized at {db_url}")


if __name__ == "__main__":
    main()
