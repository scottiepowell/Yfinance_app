"""Database connection helpers for MongoEngine models."""
from __future__ import annotations

from functools import lru_cache

from mongoengine import connect

MONGO_URI = "mongodb://appuser:appuser@localhost:27017/yfinance"
DB_NAME = "yfinance"


@lru_cache(maxsize=1)
def init_db_connection() -> None:
    """Initialise the MongoEngine connection if it has not been created."""

    connect(db=DB_NAME, host=MONGO_URI)
