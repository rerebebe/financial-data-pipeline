import os
import time
import json
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1
import datetime
import pytz

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

BASE_URL = "https://finnhub.io/api/v1/quote"
PROJECT_ID = os.getenv("GOOGLE_BIGQUERY_PROJECT_ID")
TOPIC_ID = "stock-quotes-stream"
FINNHUB_TOKEN = os.getenv("FINNHUB_API_KEY")

SYMBOLS = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "NVDA"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def is_market_open():
    tz = pytz.timezone('US/Eastern')
    now = datetime.datetime.now(tz)
    if now.weekday() >= 5: 
        return False
    open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_time <= now <= close_time

def fetch_stock_price(symbol):
    """Fetches real-time price and formats it into TWO FIELDS"""
    url = f"{BASE_URL}?symbol={symbol}&token={FINNHUB_TOKEN}"
    response = requests.get(url)
    
    if response.status_code == 200:
        raw_data = response.json()
        raw_data['symbol'] = symbol
        raw_data['data_source'] = 'finnhub'
        # --- TWO COLUMN STRUCTURE HERE ---
        return {
            # "ingest_time": int(time.time()),
            "ingest_time": datetime.datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            "raw_payload": raw_data  # Send an empty JSON object
        }
    else:
        print(f"❌ Error fetching {symbol}: {response.status_code}")
        return None

def stream_data():
    print(f"🚀 Starting stream to {TOPIC_ID}...")
    last_seen_t = {symbol: None for symbol in SYMBOLS}
    duplicate_streak = 0
    
    try:
        while True: # while True: is_market_open()
            current_cycle_has_new_data = False

            for symbol in SYMBOLS:
                quote_packet = fetch_stock_price(symbol)

                if quote_packet:
                    payload = quote_packet["raw_payload"]
                    current_t = payload.get("t")
                    current_price = payload.get("c", 0)

                    data_bytes = json.dumps(quote_packet).encode("utf-8")
                    future = publisher.publish(topic_path, data_bytes)
                    print(f"✅ Published: {symbol} @ ${current_price} (ID: {future.result()})")

                    # Check for valid price and new timestamp
                    if current_price > 0 and current_t != last_seen_t[symbol]:
                        current_cycle_has_new_data = True
                        last_seen_t[symbol] = current_t

                        # Convert the 2-column packet to JSON bytes
                        data_bytes = json.dumps(quote_packet).encode("utf-8")
                        try:
                            future = publisher.publish(topic_path, data_bytes)
                            print(f"✅ Published: {symbol} @ ${current_price} (ID: {future.result()})")
                        except Exception as e:
                            print(f"❌ Python failed to reach Pub/Sub: {e}")
                    else:
                        print(f"No change for {symbol}, skipping.")

            if not current_cycle_has_new_data:
                duplicate_streak += 1
                print(f"⚠️ No price movement. Streak: {duplicate_streak}/10")
            else:
                duplicate_streak = 0 

            if duplicate_streak >= 10:
                print("🛑 Stagnant. Sleeping 1 hour...")
                time.sleep(3600)
                continue

            time.sleep(10)
        print("🔔 Market closed. Shutting down.")
            
    except KeyboardInterrupt:
        print("\nStopping the stream.")

if __name__ == "__main__":
    stream_data() # Removed check here so you can test even if market is closed