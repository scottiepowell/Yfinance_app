#!/usr/bin/env python3

from db import DB_NAME, MONGO_URI, init_db_connection
from main import backfill_all

def update_specific_tickers(tickers=None, force=False):
    """
    Update data for specific tickers or all tickers if none specified.
    
    Args:
        tickers (list): List of tickers to update. If None, updates all tickers.
        force (bool): If True, updates even if data exists for today.
    """
    # Ensure DB connection
    init_db_connection()

    if tickers:
        print(f"Updating specific tickers: {tickers}")
    else:
        print("Updating all tickers")

    # Run backfill with force flag
    backfill_all(force_update=force)

if __name__ == "__main__":
    # Update SPY and VIX specifically (or all if you prefer)
    update_specific_tickers(force=True)
    print("\nUpdate complete! Use check_db.py to verify the data.")