"""Database session and metadata helpers."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .config import get_settings

Base = declarative_base()

settings = get_settings()
engine = create_engine(settings.database_url, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)

# Import models so SQLAlchemy registers all tables on Base.metadata.
from . import models as _models  # noqa: F401

# Create any missing tables on app startup. No drop/reset.
Base.metadata.create_all(bind=engine)


@contextmanager
def get_session() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
