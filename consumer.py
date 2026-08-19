import os
import json
import csv
from kafka import KafkaConsumer
from dotenv import load_dotenv # to read .env file
import psycopg2

load_dotenv()

TOPIC = "stock_prices"

#supabase connectiopn
POSTGRES_URL = os.getenv("POSTGRESQL")
conn = psycopg2.connect(POSTGRES_URL)

cursor = conn.cursor()
cursor.execute(
    """
SELECT ticker, company, sector, exchange
FROM company_metadata

"""
)

rows = cursor.fetchall() #return data into python

# turn rows into a dict keyed by ticker, so we can look up any ticker instantly
metadata = {}
for ticker, company, sector, exchange in rows:
    metadata[ticker] = {
        "company": company,
        "sector": sector,
        "exchange": exchange,
    }

# create consumer
consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Subscribe to a topic
consumer.subscribe([TOPIC])
quotes = []

#append from kafka to quotes[]
for message in consumer:
    quotes.append(message.value)

print(quotes)

#enrichment
enriched_records = []

for quote in quotes:
    ticker = quote["ticker"]
    info = metadata.get(ticker, {}) #if not found -> empty{}

    enriched_records.append({
        "ticker": ticker,
        "price": quote["price"],
        "volume": quote["volume"],
        "change_percent": quote["change_percent"],
        "timestamp": quote["timestamp"],
        "company": info.get("company", "Unknown"),
        "sector": info.get("sector", "Unknown"),
        "exchange": info.get("exchange", "Unknown"),
    })

print(enriched_records)

# write everything to CSV for Tableau to read
OUTPUT_FILE = "stock_prices_enriched.csv"

with open(OUTPUT_FILE, "w", newline="") as f:
    fieldnames = ["ticker", "price", "volume", "change_percent", "timestamp",
                  "company", "sector", "exchange"]
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(enriched_records)

print(f"Done! Wrote {len(enriched_records)} enriched records to {OUTPUT_FILE}")

