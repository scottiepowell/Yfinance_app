#!/usr/bin/env python3
import sys
from main import backfill_for_dates
from db import init_db_connection

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_dates.py YYYY-MM-DD [YYYY-MM-DD ...]")
        sys.exit(1)

    init_db_connection()
    dates = sys.argv[1:]
    backfill_for_dates(dates)
    print("\n✅ Done loading historic 1-minute bars!")