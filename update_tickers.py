#!/usr/bin/env python3
from main import backfill_all, connect, DB_NAME, MONGO_URI
from mongoengine import connect

def update_specific_tickers(tickers=None, force=False):
    """
    Update data for specific tickers or all tickers if none specified.
    
    Args:
        tickers (list): List of tickers to update. If None, updates all tickers.
        force (bool): If True, updates even if data exists for today.
    """
    # Ensure DB connection
    connect(db=DB_NAME, host=MONGO_URI)
    
    if tickers:
        print(f"Updating specific tickers: {tickers}")
    else:
        print("Updating all tickers")
    
    # Run backfill with force flag
    backfill_all(force_update=force)

if __name__ == "__main__":
    # Update SPY and VIX specifically
    update_specific_tickers(force=True)  # Will update all tickers, but skip those with existing data
    print("\nUpdate complete! Use check_db.py to verify the data.")