import os
import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
 # to read .env file\


#load secret from .env into environment variable, then use os.getenv()
load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")

TICKERS = ["MU", "NVDA", "MRVL", "SNDK", "AMD", "AVGO", "TSM", "QCOM"]

#name of kafka's channel name we're publishing to (kafka creates it automatically when we send)
TOPIC = "stock_prices"

#create producer
# 8080 -> kafka UI
# 9092 -> kafka running
producer = KafkaProducer(
    bootstrap_servers="localhost:9092", #where local kafka container is listening
    value_serializer=lambda v: json.dumps(v).encode("utf-8"), #convert python's dict -> JSON -> raw bytes
)

def fetch_quote(ticker):
    """Ask Alpha Vantage for one stock's current price data at a time"""

    url = "https://www.alphavantage.co/query"

    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": ticker,
        "apikey": API_KEY,
    }

    # Send the request and get the response back
    response = requests.get(url, params=params)

    # Convert the response text (JSON) into a Python dictionary
    raw_data = response.json()

    # The actual price info is nested inside "Global Quote"
    quote_data = raw_data.get("Global Quote", {})

    if not quote_data:
        print(f"No data for {ticker}. Response was: {raw_data}")
        return None

    # Pick out just the fields we care about, with clear names
    return {
        "ticker": ticker,
        "price": float(quote_data.get("05. price", 0)),
        "volume": int(quote_data.get("06. volume", 0)),
        "change_percent": quote_data.get("10. change percent", "0%"),
        "timestamp": time.time(),
    }


# --- Main program starts here ---

# __name__ is created for every file
# If you run the file directly (python producer.py) → __name__ is set to "__main__"
if __name__ == "__main__": 

    for ticker in TICKERS:
        print(f"Fetching {ticker}...")
        quote = fetch_quote(ticker)

        if quote is not None:
            producer.send(TOPIC, value=quote)
            print(f"  Sent: {quote}")
        else:
            print(f"  Skipped {ticker}")

        # Wait 13 seconds so we stay under the free API's 5-requests-per-minute limit
        time.sleep(13)

    # Make sure all messages are actually delivered before the script ends
    producer.flush()
    print("Done! Check Kafka for your messages.")
