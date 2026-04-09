"""Additive migration for Telegram onboarding user fields."""
from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import inspect, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import engine  # noqa: E402


def main() -> None:
    inspector = inspect(engine)
    if "users" not in inspector.get_table_names():
        print("users table not found; run init_db first.")
        return

    existing = {col["name"] for col in inspector.get_columns("users")}
    dialect = engine.dialect.name
    if dialect == "sqlite":
        additions = {
            "phone_number": "ALTER TABLE users ADD COLUMN phone_number VARCHAR(64)",
            "phone_verified": "ALTER TABLE users ADD COLUMN phone_verified BOOLEAN NOT NULL DEFAULT 0",
            "onboarding_status": "ALTER TABLE users ADD COLUMN onboarding_status VARCHAR(64) NOT NULL DEFAULT 'new'",
            "last_seen_at": "ALTER TABLE users ADD COLUMN last_seen_at DATETIME",
        }
        backfills = [
            "UPDATE users SET phone_verified = 0 WHERE phone_verified IS NULL",
            "UPDATE users SET onboarding_status = 'new' WHERE onboarding_status IS NULL OR onboarding_status = ''",
        ]
    else:
        additions = {
            "phone_number": "ALTER TABLE users ADD COLUMN phone_number VARCHAR(64)",
            "phone_verified": "ALTER TABLE users ADD COLUMN phone_verified BOOLEAN NOT NULL DEFAULT FALSE",
            "onboarding_status": "ALTER TABLE users ADD COLUMN onboarding_status VARCHAR(64) NOT NULL DEFAULT 'new'",
            "last_seen_at": "ALTER TABLE users ADD COLUMN last_seen_at TIMESTAMPTZ",
        }
        backfills = [
            "UPDATE users SET phone_verified = FALSE WHERE phone_verified IS NULL",
            "UPDATE users SET onboarding_status = 'new' WHERE onboarding_status IS NULL OR onboarding_status = ''",
        ]

    added: list[str] = []
    with engine.begin() as connection:
        for column_name, statement in additions.items():
            if column_name not in existing:
                connection.execute(text(statement))
                added.append(column_name)
        for statement in backfills:
            connection.execute(text(statement))
    if added:
        print(f"Added user columns: {', '.join(added)}")
    else:
        print("No schema changes needed; onboarding columns already present.")


if __name__ == "__main__":
    main()
