from mongoengine import connect
from main import MinuteBar, DB_NAME, MONGO_URI

def check_db_stats():
    # Connect to MongoDB
    connect(db=DB_NAME, host=MONGO_URI)
    
    # Get total number of documents
    total_docs = MinuteBar.objects.count()
    
    # Get unique tickers
    unique_tickers = MinuteBar.objects.distinct('ticker')
    
    # Get date range
    first_date = MinuteBar.objects.order_by('date').first().date
    last_date = MinuteBar.objects.order_by('-date').first().date
    
    # Sample a few records
    print("\nDatabase Statistics:")
    print(f"Total documents: {total_docs}")
    print(f"Number of unique tickers: {len(unique_tickers)}")
    print(f"Tickers: {sorted(unique_tickers)}")
    print(f"Date range: {first_date} to {last_date}")
    
    # Sample one ticker's data
    if unique_tickers:
        ticker = unique_tickers[0]
        ticker_count = MinuteBar.objects(ticker=ticker).count()
        print(f"\nSample ticker {ticker}:")
        print(f"Number of records: {ticker_count}")
        print("\nFirst record:")
        print(MinuteBar.objects(ticker=ticker).first().to_json())

if __name__ == "__main__":
    check_db_stats()