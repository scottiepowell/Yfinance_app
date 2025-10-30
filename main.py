#!/usr/bin/env python3
"""
main.py - YFinance 1m bar downloader + MongoDB storage

Features:
- Handles tz-aware or tz-naive DataFrames safely
- Backfill specific dates (via run_dates.py)
- Automatically includes SPY and VIX (^VIX)
- Skips already-stored tickers for efficiency
"""

import yfinance as yf
import pandas as pd
import pytz
from datetime import datetime, time
from models import MinuteBar
from db import init_db_connection

# --- Constants ---
ET = pytz.timezone("US/Eastern")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)
MAX_MINUTES = 390  # 6.5 trading hours * 60
TICKER_CSV = "tickers.csv"

# --- Initialize DB connection ---
init_db_connection()

# --- Compute minute index helper ---
def compute_minute_index(ts: datetime) -> int:
    """Return 1-based minute index since market open (Eastern Time)."""
    dt = ts.astimezone(ET)
    start_dt = dt.replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute, second=0, microsecond=0)
    delta = dt - start_dt
    return int(delta.total_seconds() // 60) + 1

# --- Fetch 1m bars for a specific date ---
def fetch_and_store_for_date(ticker: str, date: str):
    """
    Fetch 1-minute bars for a single date (YYYY-MM-DD) and store in MongoDB.
    """
    print(f"Fetching {ticker} for {date}...")

    start_date = pd.to_datetime(date)
    end_date = start_date + pd.Timedelta(days=1)

    df = yf.download(
        tickers=ticker,
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        interval="1m",
        progress=False,
        ignore_tz=True  # ensures tz-naive index from yfinance
    )

    if df.empty:
        print(f"⚠️ No 1m data found for {ticker} on {date}")
        return

    # Make timezone-safe: convert to Eastern Time
    if df.index.tz is None:
        df = df.tz_localize("UTC").tz_convert(ET)
    else:
        df = df.tz_convert(ET)

    docs = []

    for idx, row in df.iterrows():
        # Skip outside normal session
        if idx.time() < MARKET_OPEN or idx.time() > MARKET_CLOSE:
            continue

        try:
            minute_idx = compute_minute_index(idx)
        except Exception:
            continue

        if minute_idx < 1 or minute_idx > MAX_MINUTES:
            continue

        docs.append(
            MinuteBar(
                ticker=ticker.replace("^", ""),  # e.g., store VIX not ^VIX
                date=date,
                minute_index=minute_idx,
                timestamp=idx.to_pydatetime(),
                open=float(row["Open"]),
                high=float(row["High"]),
                low=float(row["Low"]),
                close=float(row["Close"]),
                volume=int(row["Volume"]),
            )
        )

    if not docs:
        print(f"⚠️ No valid trading minutes for {ticker} on {date}")
        return

    # Bulk insert
    try:
        MinuteBar.objects.insert(docs, load_bulk=False)
        print(f"✅ Inserted {len(docs)} bars for {ticker} on {date}")
    except Exception as ex:
        print(f"❌ Insert error for {ticker} on {date}: {ex}")

# --- Backfill multiple dates ---
def backfill_for_dates(dates, tickers=None):
    """Backfill multiple trading dates for all tickers in tickers.csv (plus SPY, VIX)."""
    df = pd.read_csv(TICKER_CSV)
    ticker_list = tickers or df["ticker"].tolist()

    # Always include SPY and VIX
    for extra in ("SPY", "VIX"):
        if extra not in ticker_list:
            ticker_list.append(extra)

    for date in dates:
        print(f"\n=== DATE: {date} ===")
        for t in ticker_list:
            yf_symbol = "^VIX" if t == "VIX" else t
            # Skip already existing data
            if MinuteBar.objects(ticker=t, date=date).count() > 0:
                print(f"⏭️  {t} already loaded for {date}, skipping.")
                continue
            fetch_and_store_for_date(yf_symbol, date)

# --- Convenience: daily update (for today) ---
def backfill_today(force=False):
    today = datetime.now(ET).date().isoformat()
    backfill_for_dates([today])

if __name__ == "__main__":
    # Example: backfill today's data
    backfill_today()