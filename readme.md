1. Key considerations & caveats

Minute-level data availability

yfinance supports intraday interval like '1m'. For example:

Important note: minute-level data via Yahoo Finance is only available for a limited recent period (≈ 7 days or less) when interval is “1m”/“2m”. 
Medium
+1

So when you say “last week” you are in the safe range; but if you meant further back (20+ days) you might not get minute data reliably from yfinance.

Also note timezone alignment: Yahoo will give timestamps likely in exchange / local timezone (often US Eastern for US stocks) — you’ll need to map them to your minute index (see below).

Mapping minute index to integer

You want: 9:30 ET -> minute index = 1; 9:31 ET -> 2; etc. You likely need to convert the timestamp to ET timezone if not already, then compute (timestamp – 9:30) / 1 minute + 1 (only for minutes during trading hours).

You’ll also need to handle possibly missing minutes (illiquid stocks) or after-hours if you choose to include them.

Database modelling

You want a database (MongoDB) collection that stores each minute bar for each ticker, along with ticker symbol, date, minute index, open, high, low, close, volume, etc.

Using an ODM/ORM (in NoSQL world more accurately an “ODM”) is good for code structure. For Python + MongoDB you have options:

MongoEngine — a synchronous ODM for MongoDB. 
MongoEngine Documentation
+1

Beanie — asynchronous ODM based on Pydantic. 
GitHub
+1

Or you could simply use PyMongo directly (lower abstraction) if you want simple.

Retrieving list of tickers for QQQ

You need ~100 tickers — presumably the constituents of the Nasdaq-100 (tracked by QQQ ETF). You’ll need a list (maybe snapshot). Your code needs to ingest that list (maybe CSV, JSON, or via API). This part isn’t covered in depth here but you should plan for a method to obtain/refresh the list of tickers.

Scheduling / real-time capture

You want to capture each minute for the full trading day in ET. So you’ll likely schedule a job to run every minute during market hours (9:30 ET to 16:00 ET), fetch data for all tickers, compute minute index, store.

For historical/backfill you’ll fetch for last week (e.g., using period=“7d”, interval=“1m”) and store.

Timezones / alignment

Ensure your system uses UTC or correct timezone conversion. Trading hours in US Eastern Standard Time (EST/EDT depending on DST) – you’ll need to adjust for DST.

Also ensure the timestamp from yfinance is aligned: the DataFrame index often will have timezone info (e.g., 2025-10-24 09:30:00-04:00 for EDT) depending on data. Example from StackOverflow. 
Stack Overflow

Data volume & performance

If you are storing 100 tickers × (6.5 hours × 60 minutes = 390 minutes) per day = ~39,000 records/day. For a week ~200k records. This is manageable in Mongo but you should index by (ticker, date, minute_index) for quick querying.

Bulk insertion might help rather than one record at a time.

Schema design

Example document structure could be: