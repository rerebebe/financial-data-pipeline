# backfill_stocks_prices.py
import yfinance as yf
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

import os
import time

load_dotenv()

# Config
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"
PROJECT_ID = os.getenv("GOOGLE_BIGQUERY_PROJECT_ID")
DATASET_ID = "stock_market_data"
TABLE_ID = "raw_5_year_stock_price"
SYMBOLS = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN", "NVDA"]


def upload_historical_to_bigquery():
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    
    # Download data from yfinance
    raw_data = yf.download(SYMBOLS, period="5y", interval="1d", auto_adjust=True)
    # raw_data.to_csv("1_wide_format_original.csv")
    # print(raw_data)

    
    # Reshape from "Wide" to "Long" format
    df = raw_data.stack(level=1).reset_index()
    # df.to_csv("2_long_format_transformed.csv", index=False)
    # print(df)
    
    # Standardize Columns to match your streaming schema
    df.columns = ['date', 'symbol', 'close', 'high', 'low', 'open', 'volume']
    
    # Add metadata for the Bronze Layer
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['ingest_time'] = pd.Timestamp.now(tz='UTC')
    df['data_source'] = 'yfinance_bulk'

    # Final cleanup: BigQuery likes specific types
    df['symbol'] = df['symbol'].astype(str)
    # df.to_csv("3_newcolumn_transformed.csv", index=False)
    
    print(f"⬆️ Uploading {len(df)} rows to {table_ref}...")
    
    # Load to BigQuery (Write-Truncate for the first clean load)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result() 

    print("✅ Historical load complete!")

if __name__ == "__main__":
    upload_historical_to_bigquery()