import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
from mongoengine import Document, StringField, IntField, DateTimeField, FloatField, connect, IndexModel, signals

# --- Configuration ---
# MongoDB connection for the dockerised MongoDB instance running on localhost.
# The container exposes port 27017 and a user `appuser` with password `appuser`
# on the `yfinance` database.
MONGO_URI = "mongodb://appuser:appuser@localhost:27017/yfinance"
DB_NAME = "yfinance"
COLLECTION = "minute_bars"

# Market timezone
ET = pytz.timezone("US/Eastern")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Number of minutes in regular session: 6.5 hours * 60 = 390
MAX_MINUTES = 390

# Example ticker list (replace with full QQQ constituents)
TICKERS = ["AAPL", "MSFT", "GOOGL", "..."]  # ~100 tickers

# --- MongoEngine model ---
class MinuteBar(Document):
    meta = {
        'collection': COLLECTION,
        'indexes': [
            {'fields': ('ticker', 'date', 'minute_index'), 'unique': True}
        ]
    }
    ticker = StringField(required=True)
    date = StringField(required=True)              # e.g., '2025-10-24'
    minute_index = IntField(required=True)         # 1 = 9:30, etc.
    timestamp = DateTimeField(required=True)       # actual datetime
    open = FloatField()
    high = FloatField()
    low = FloatField()
    close = FloatField()
    volume = IntField()

# --- Connect to DB ---
connect(db=DB_NAME, host=MONGO_URI)

# --- Utility to compute minute_index ---
def compute_minute_index(ts: datetime) -> int:
    # ts should be timezone‐aware in ET
    dt = ts.astimezone(ET)
    market_start_dt = dt.replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute,
                                 second=0, microsecond=0)
    delta = dt - market_start_dt
    minute_index = int(delta.total_seconds() // 60) + 1
    return minute_index

# --- Function to fetch and store one ticker for a given period ---
def fetch_and_store_ticker(ticker: str, period: str = "7d", interval: str = "1m"):
    print(f"Fetching {ticker} period={period}, interval={interval}")
    df = yf.download(tickers=ticker, period=period, interval=interval, progress=False)
    if df.empty:
        print(f"No data for {ticker}")
        return
    # df index: datetime (timezone aware)
    df = df.tz_convert(ET)  # ensure timezone is ET if needed
    date_str = df.index[0].date().isoformat()
    docs = []
    for idx, row in df.iterrows():
        # Only include rows between market open/close (optional)
        if idx.time() < MARKET_OPEN or idx.time() > MARKET_CLOSE:
            continue
        try:
            minute_idx = compute_minute_index(idx)
            if minute_idx < 1 or minute_idx > MAX_MINUTES:
                continue
        except Exception as ex:
            continue
        doc = MinuteBar(
            ticker=ticker,
            date=date_str,
            minute_index=minute_idx,
            timestamp=idx.to_pydatetime(),
            open=row['Open'],
            high=row['High'],
            low=row['Low'],
            close=row['Close'],
            volume=int(row['Volume'])
        )
        docs.append(doc)
    if docs:
        # Bulk insert (with error handling for duplicates)
        try:
            MinuteBar.objects.insert(docs, load_bulk=False)
            print(f"Inserted {len(docs)} bars for {ticker}")
        except Exception as ex:
            print(f"Insert error for {ticker}: {ex}")

# --- Backfill all tickers ---
def backfill_all():
    for ticker in TICKERS:
        fetch_and_store_ticker(ticker, period="7d", interval="1m")

# --- Live capture (for current trading day) ---
def capture_live_minute(tickers):
    # This would be run every minute (via scheduler/cron/while loop + sleep)
    for ticker in tickers:
        fetch_and_store_ticker(ticker, period="1d", interval="1m")

if __name__ == "__main__":
    # Example usage: backfill once
    backfill_all()
    # Then schedule live minute capture (outside this script or extend while loop)
