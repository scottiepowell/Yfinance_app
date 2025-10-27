# YFinance Data Collection App

This application fetches minute-level stock data for QQQ constituents using yfinance and stores it in MongoDB. It supports both historical data backfilling and live market data capture.

## Features

- Fetches minute-level data for multiple stocks using yfinance
- Stores data in MongoDB with efficient indexing
- Handles timezone conversion (market hours in ET)
- Supports both backfill and live data capture modes
- Configurable for different time periods and stock lists
- Prevents duplicate data through unique indexing

## Prerequisites

- Python 3.x
- MongoDB (running locally or accessible remote instance)
- Virtual environment (recommended)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/scottiepowell/Yfinance_app.git
cd Yfinance_app
```

2. Create and activate a virtual environment:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install --upgrade pip
pip install yfinance pandas pytz mongoengine
```

4. Configure MongoDB:
- The application expects MongoDB running at: `mongodb://appuser:appuser@localhost:27017/yfinance`
- If using different credentials, update `MONGO_URI` in `main.py`

## Usage

### 1. Backfill Historical Data

To fetch the last 7 days of minute-level data for all tickers:
```bash
python main.py
```

### 2. Check Database Status

View statistics about collected data:
```bash
python check_db.py
```

### 3. Test Single Ticker

Test data fetching for a single ticker:
```bash
python dry_run_fetch.py
```

### 4. Live Data Capture

For live market data capture, you can:
1. Run the script manually during market hours:
```python
from main import capture_live_minute
capture_live_minute(TICKERS)
```

2. Or set up a cron job (Linux/macOS) to run every minute during market hours:
```bash
# Edit crontab
crontab -e

# Add line (runs every minute from 9:30 AM to 4:00 PM ET on weekdays)
* 9-16 * * 1-5 cd /path/to/Yfinance_app && .venv/bin/python main.py
```

## Project Structure

- `main.py`: Core application logic
- `tickers.csv`: List of stocks to track
- `check_db.py`: Database status verification
- `dry_run_fetch.py`: Single ticker test script

## Data Schema

Each minute bar is stored with:
- Ticker symbol
- Date
- Minute index (1 = 9:30 AM ET)
- Timestamp (with timezone)
- OHLCV data (Open, High, Low, Close, Volume)

## Limitations

- yfinance provides minute-level data only for the last ~7 days
- Market hours only (9:30 AM - 4:00 PM ET)
- Rate limits may apply for large numbers of requests

## MongoDB Indexes

The application maintains a unique compound index on:
- ticker
- date
- minute_index

This prevents duplicate data and enables efficient querying.

## Error Handling

- Duplicate data is prevented through MongoDB's unique index
- Network errors are caught and logged
- Invalid timestamps or data outside market hours are filtered

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

See the LICENSE file for details.