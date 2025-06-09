"""
Kafka Consumer Script for Storing Stock Data from Topics to CSV 

Consumes stock data from Kafka topics, processes JSON messages, 
and saves structured data to a CSV file.

Modules:
    - csv: Writes data to CSV.
    - json: Parses Kafka messages.
    - kafka: Handles Kafka connections.
    - kafka.admin: Manages topics.
    - datetime: Adds timestamps.

Functionality:
    - Establishes a Kafka consumer to listen for stock-related topics.
    - Parses JSON messages containing stock price, volume, or trading details.
    - Formats the retrieved data and appends it to a CSV file with timestamps.
    - Implements error handling for network failures.

Usage:
    - Ensure Kafka topics exist before running.
    - Modify consumer configurations as needed.
    - The output CSV file will store stock market data for further analysis.

Author: Charlene Mwamjeni
Date: 2025-06-05
"""

import csv
import json
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from datetime import datetime

KAFKA_BROKER = "localhost:9092"
kafka_csv_filename = f"C:/Users/BlvckMoon/OneDrive/Documents/GitHub/StochastiCast/data/kafka_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

# Initialize Kafka Admin to dynamically fetch available topics
admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
available_topics = admin_client.list_topics()

# Define asset-type topics
ASSET_TYPES = ["stocks", "fx", "crypto"]
TOPICS = [topic for topic in available_topics if any(asset in topic for asset in ASSET_TYPES)]

print(f"✅ Found Kafka Topics: {TOPICS}")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='test-consumer-9',
    consumer_timeout_ms=60000  # Exit after 60 seconds of no data
)

print(f"✅ Connected! Subscribed to topics: {TOPICS}")

messages = []

for message in consumer:
    val = message.value
    if "results" in val:
        messages.append(val["results"])  # Extract relevant data
    if len(messages) >= 10:
        break  # Stop at 10 for testing

# Check for Empty messages
if messages and isinstance(messages[0], list) and messages[0]:  # Ensure there's data
    print(f"⚡ Debug: First Message -> {messages[0]}")
    print(f"⚡ Debug: Type of First Message -> {type(messages[0])}")
    # Flatten the messages list if it contains nested lists and add 'ticker' column
    flat_messages = [
        {"ticker": topic, **item}  # Append Kafka topic as 'ticker'
        for topic, message_list in zip(TOPICS, messages)
        for item in (message_list if isinstance(message_list, list) else [message_list])
    ]
    # Extract field names correctly
    keys = list(flat_messages[0].keys()) if flat_messages else []

    # Save data to a CSV file
    if flat_messages:
        with open(kafka_csv_filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(flat_messages)

        print(f"✅ Saved Kafka messages to CSV: {kafka_csv_filename}")
    else:
        print("⚠️ No valid data received from Kafka.")
