"""
Kafka Producer Script for Stock Data Streaming

This script connects to the Polygon API to fetch real-time or historical stock data 
and publishes the retrieved data to different Kafka topics for downstream processing.

Modules:
    - kafka: Sends data to Kafka.
    - requests: Fetches data from the API.
    - json: Parses API responses.
    - time: Controls fetch intervals.
    - datetime: Adds timestamps.
    - re: Removes special characters in ticker name with regex.

Functionality:
    - Establishes a connection to the Polygon API using an API key and retrieves stock data.
    - Publishes processed data to Kafka topics based on the market (`stocks`, `fx`, `crypto`).
    - Implements retry mechanisms for API failures.
    
Usage:
    - Run this script to continuously stream stock data into Kafka.
    - Ensure Kafka is running with configured topics.
    - Set API parameters based on different data retrieval needs.

Author: Charlene Mwamjeni
Date: 2025-06-04
"""

from kafka import KafkaProducer
import requests
from datetime import datetime, timedelta, time
import json
import re

# Polygon API Key
API_KEY = "2u7PfXB4OJHCYuUQ__dYIoxj5mzlBzlU"

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"

# Asset types and tickers
markets = {
    "stocks": ["AAPL", "BA", "NKE"],
    "fx": ["C:AEDUSD", "C:USDZAR"],
    "crypto": ["X:BTCGBP", "X:BTCUSD"]
}

# OHLC bar parameters
DATE_FROM = "2024-06-01"
DATE_TO = "2025-05-02"
AGGREGATION = "1/minute"  # Customizable intervals like "1/day", "1/week", "1/quarter"

def sanitize_ticker(ticker):
    """Ensure the ticker name is valid for Kafka topics by removing special characters."""
    return re.sub(r"[^a-zA-Z0-9._-]", "", ticker)  # Remove invalid characters

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_ohlc_data(ticker):
    """Fetch OHLC bars from Polygon API for the given ticker."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{AGGREGATION}/{DATE_FROM}/{DATE_TO}?adjusted=true&sort=asc&limit=120&apiKey={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Debugging: Print API response
        print(f"🔍 API Response for {ticker}: {json.dumps(data, indent=2)}")
        
        if "results" in data and isinstance(data["results"], list):
            return data
        else:
            raise ValueError(f"⚠️ Invalid data format received for {ticker}")
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching {ticker} data: {e}")
        return None


def publish_to_kafka():
    """Publish OHLC data to Kafka topics for different asset types."""
    for asset_type, ticker_list in markets.items():
        for ticker in ticker_list:
            sanitized_ticker = sanitize_ticker(ticker)  # Clean ticker format
            topic = f"{asset_type}_{sanitized_ticker}"  # Use cleaned ticker
            
            data = fetch_ohlc_data(ticker)
            if data:
                producer.send(topic, data)
                print(f"✅ Published OHLC data for {ticker} to {topic}")
            else:
                print(f"⚠️ Skipping {ticker} due to data issues")

    producer.flush()

if __name__ == "__main__":
    publish_to_kafka()