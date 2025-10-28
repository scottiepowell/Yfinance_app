import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
from mongoengine import signals

from db import DB_NAME, MONGO_URI, init_db_connection
from models import MinuteBar

# --- Configuration ---
# MongoDB connection constants are defined in db.py and imported above.
COLLECTION = "minute_bars"

# Market timezone
ET = pytz.timezone("US/Eastern")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Number of minutes in regular session: 6.5 hours * 60 = 390
MAX_MINUTES = 390

# Load tickers from tickers.csv
import csv
with open('tickers.csv', 'r') as f:
    csv_reader = csv.DictReader(f)
    TICKERS = [row['ticker'] for row in csv_reader]
print(f"Loaded {len(TICKERS)} tickers from tickers.csv")

# --- Connect to DB ---
init_db_connection()

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

# --- Check if ticker has data ---
def ticker_has_data(ticker: str, date_str: str) -> bool:
    return MinuteBar.objects(ticker=ticker, date=date_str).count() > 0

# --- Backfill all tickers ---
def backfill_all(force_update=False):
    # Get today's date in ET
    et_now = datetime.now(ET)
    today_str = et_now.date().isoformat()
    
    # Load tickers from CSV with pandas to handle the QQQ_holding column
    df = pd.read_csv('tickers.csv')
    tickers_to_process = df['ticker'].tolist()
    
    print(f"Processing {len(tickers_to_process)} tickers...")
    
    for ticker in tickers_to_process:
        # Skip if data exists and force_update is False
        if not force_update and ticker_has_data(ticker, today_str):
            print(f"Skipping {ticker} - data already exists for {today_str}")
            continue
            
        # Special handling for VIX (use ^VIX)
        if ticker == "VIX":
            actual_ticker = "^VIX"
        else:
            actual_ticker = ticker
            
        print(f"Fetching {ticker} data...")
        fetch_and_store_ticker(actual_ticker, period="7d", interval="1m")

# --- Live capture (for current trading day) ---
def capture_live_minute(tickers):
    # This would be run every minute (via scheduler/cron/while loop + sleep)
    for ticker in tickers:
        fetch_and_store_ticker(ticker, period="1d", interval="1m")

if __name__ == "__main__":
    # Example usage: backfill once
    backfill_all()
    # Then schedule live minute capture (outside this script or extend while loop)
