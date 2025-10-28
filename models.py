"""MongoEngine document definitions used by the application."""
from __future__ import annotations

from mongoengine import DateTimeField, Document, FloatField, IntField, StringField


class MinuteBar(Document):
    """MongoEngine document storing 1-minute OHLCV bars for a ticker."""

    meta = {
        "collection": "minute_bars",
        "indexes": [
            {"fields": ("ticker", "date", "minute_index"), "unique": True},
        ],
    }

    ticker = StringField(required=True)
    date = StringField(required=True)
    minute_index = IntField(required=True)
    timestamp = DateTimeField(required=True)
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = IntField()
