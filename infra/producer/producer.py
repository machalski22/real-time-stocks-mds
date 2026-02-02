import time
import json
import requests
import os
from dotenv import load_dotenv

from kafka import KafkaProducer
#print(os.environ.get("FINNHUB_API_KEY"))
#Define variables for API
load_dotenv()
API_KEY = os.environ.get("FINNHUB_API_KEY")

if not API_KEY:
    raise RuntimeError("FINNHUB_API_KEY environment variable not set")

#API_KEY="d60hl0hr01qto1rd6cmgd60hl0hr01qto1rd6cn0"
BASE_URL = "https://finnhub.io/api/v1"
SYMBOLS = ["NVDA", "GOOG", "AAPL", "MSFT", "META"]

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_data(symbol):
    try:
        resp = requests.get(f"{BASE_URL}/quote?symbol={symbol}&token={API_KEY}")
        resp.raise_for_status()
        data = resp.json()
        data["symbol"] = symbol
        data["timestamp"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

while True:
    for symbol in SYMBOLS:
        quote = fetch_data(symbol)
        if quote:
            print(f"Producing data: {quote}")
            producer.send("stock-quotes", value=quote)
    time.sleep(6)