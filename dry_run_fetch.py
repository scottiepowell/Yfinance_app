#!/usr/bin/env python3
import yfinance as yf
import pytz
from datetime import datetime, time
import pandas as pd

# Market timezone (same as main.py)
ET = pytz.timezone("US/Eastern")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

def test_fetch_ticker(ticker="AAPL", period="1d", interval="1m"):
    """Test yfinance data fetching for a single ticker"""
    print(f"\nFetching {ticker} data (period={period}, interval={interval})...")
    
    # Fetch data
    df = yf.download(tickers=ticker, period=period, interval=interval, progress=False)
    
    if df.empty:
        print(f"Error: No data received for {ticker}")
        return False
        
    # Print basic info
    print(f"\nDataFrame Info:")
    print(f"Shape: {df.shape}")
    print(f"Time Range: {df.index[0]} to {df.index[-1]}")
    print(f"Timezone Info: {df.index.tz}")
    
    # Show first few rows
    print(f"\nFirst 3 rows of data:")
    print(df.head(3))
    
    # Basic validation
    trading_hours_data = df[
        (df.index.time >= MARKET_OPEN) & 
        (df.index.time <= MARKET_CLOSE)
    ]
    
    print(f"\nValidation:")
    print(f"Total rows: {len(df)}")
    print(f"Rows during trading hours: {len(trading_hours_data)}")
    print(f"Data columns: {', '.join([col[0] for col in df.columns])}")
    
    return not df.empty

if __name__ == "__main__":
    # Test with AAPL for one day
    success = test_fetch_ticker("AAPL", period="1d", interval="1m")
    print(f"\nTest {'succeeded' if success else 'failed'}")