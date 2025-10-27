from mongoengine import connect, disconnect
from main import DB_NAME, MONGO_URI, MinuteBar

def test_mongodb_connection():
    print("Testing MongoDB connection...")
    try:
        # Ensure we're starting fresh
        disconnect()
        
        # Try to connect
        conn = connect(db=DB_NAME, host=MONGO_URI)
        
        # Simple write test
        test_doc = MinuteBar(
            ticker="TEST",
            date="2025-10-27",
            minute_index=1,
            timestamp=datetime.now(),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        test_doc.save()
        
        # Try to retrieve it
        retrieved = MinuteBar.objects(ticker="TEST").first()
        if retrieved:
            print("Successfully wrote and retrieved a test document")
            retrieved.delete()  # Clean up
        
        print("✓ MongoDB connection successful!")
        print(f"Connected to database: {DB_NAME}")
        print(f"Collections: {conn.get_database().list_collection_names()}")
        return True
        
    except Exception as e:
        print(f"✗ MongoDB connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    from datetime import datetime
    success = test_mongodb_connection()
    exit(0 if success else 1)