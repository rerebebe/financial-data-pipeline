# producer.py
import os
import time
import json
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1
import datetime
import pytz
import pandas_market_calendars as mcal
# import logging


load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

BASE_URL = "https://finnhub.io/api/v1/quote"
PROJECT_ID = os.getenv("GOOGLE_BIGQUERY_PROJECT_ID")
TOPIC_ID = "stock-quotes-stream"
FINNHUB_TOKEN = os.getenv("FINNHUB_API_KEY")

# logger = logging.getLogger(__name__)

# List of stocks to track
SYMBOLS = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "NVDA"]

# Setup the Google Cloud Publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)



def is_market_open():
    tz = pytz.timezone('US/Eastern')
    now = datetime.datetime.now(tz)
    
    # 1. Get the NYSE calendar
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty:
        return False

    # 2. Check if it's a weekend
    if now.weekday() >= 5: 
        return False
        
    # 3. Check Market Hours (9:30 AM - 4:00 PM ET)
    open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
   
    # # 4. Check if current time is same for more than 10 times pull
    return open_time <= now <= close_time


def fetch_stock_price(symbol):
    """Fetches real-time price from Finnhub"""
    url = f"{BASE_URL}?symbol={symbol}&token={FINNHUB_TOKEN}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()

        print('data:', data)
        return {'symbol': symbol, 'ingest_time': int(time.time()), 'data_source': 'finnhub', **data}
        # return {
        #     "data": {'symbol': symbol, 'data_source': 'finnhub', **data},
        #     "ingest_time": datetime.datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')    
        # }
    
    else:
        print(f"❌ Error fetching {symbol}: {response.status_code} - {response.text}")
        return None

def stream_data():
    print(f"🚀 Starting stream to {TOPIC_ID}...")
    last_seen_t = {symbol: None for symbol in SYMBOLS}
    duplicate_streak = 0
    
    try:
        while is_market_open():
        # while True:
            current_cycle_has_new_data = False

            for symbol in SYMBOLS:
                quote = fetch_stock_price(symbol)

                # Instead of publishing, we just print it!
                # if quote:  
                #  print(f"Would send to GCP: {quote}")

                if quote and quote.get("c", 0) > 0:
                # if quote and quote.get("data", {}).get("c", 0) > 0:
                    data_str = json.dumps(quote) # Convert dict to JSON string
                    data_bytes = data_str.encode("utf-8") # then to Bytes for Pub/Sub
                    current_t = quote.get("t")
                    
                    # 2. Second Gate: Check for Data Stagnation
                    if current_t != last_seen_t[symbol]:
                        current_cycle_has_new_data = True
                        last_seen_t[symbol] = current_t

                        # Publish to the pipe
                        future = publisher.publish(topic_path, data_bytes)
                        print(f"✅ Published: {symbol} @ ${quote['c']} (ID: {future.result()})")
                    else:
                        print(f"No change for {symbol}, skipping publish.")

                
            if not current_cycle_has_new_data:
                duplicate_streak += 1
                print(f"⚠️ No price movement detected for any symbol. Streak: {duplicate_streak}/10")
            else:
                duplicate_streak = 0 # Reset if even one stock moves

            if duplicate_streak >= 10:
                print("🛑 Market stagnant. Entering Deep Sleep mode for 1 hour...")
                duplicate_streak = 0 # Reset the counter so it can check again later
                time.sleep(3600)     # Sleep for 60 minutes before checking the market again
                continue             # Go back to the top of the 'while True' loop


            # Wait 10 seconds to stay safe within Finnhub's free tier (60 calls/min)
            time.sleep(10)
        print("🔔 Market is now closed. Shutting down the stream for the day.")
            
    except KeyboardInterrupt:
        print("\nStopping the stream. See you later!")

if __name__ == "__main__":
    if is_market_open():
        stream_data()
    else:
        print("Market is closed.......................")