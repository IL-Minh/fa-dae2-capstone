from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import polars as pl
from confluent_kafka import Producer
from dotenv import load_dotenv

# Import our modular faker generator
from utils.faker_generator import get_faker_generator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [PRODUCER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_user_ids_from_csv() -> list[str]:
    """Load user IDs from the generated user registration CSV files"""
    try:
        incoming_dir = Path("data/incoming")
        if not incoming_dir.exists():
            logger.warning("data/incoming directory not found, using random user IDs")
            return []

        # Find user registration CSV files
        user_files = list(incoming_dir.glob("user_registrations_*.csv"))
        if not user_files:
            logger.warning(
                "No user registration CSV files found, using random user IDs"
            )
            return []

        # Use the most recent file
        latest_file = max(user_files, key=lambda f: f.stat().st_mtime)
        logger.info(f"Loading user IDs from: {latest_file}")

        # Read the CSV and extract user IDs
        df = pl.read_csv(latest_file)
        user_ids = df["user_id"].to_list()
        logger.info(f"Loaded {len(user_ids)} user IDs from CSV")
        return user_ids

    except Exception as e:
        logger.error(f"Failed to load user IDs from CSV: {e}")
        logger.info("Falling back to random user IDs")
        return []


def generate_streaming_transaction(faker_gen, user_ids: list[str]) -> dict:
    """Generate a transaction with different characteristics than batch data"""
    import random

    # Use real user IDs if available, otherwise generate random ones
    if user_ids:
        user_id = random.choice(user_ids)
    else:
        user_id = faker_gen.generate_transaction()["user_id"]

    # Different categories for streaming vs batch (more real-time focused)
    streaming_categories = [
        "online_shopping",
        "food_delivery",
        "ride_sharing",
        "streaming_services",
        "gaming",
        "social_media",
        "mobile_apps",
        "digital_subscriptions",
        "crypto_trading",
        "peer_to_peer",
        "micro_transactions",
        "instant_transfers",
    ]

    # Different merchant patterns for streaming (more tech/digital focused)
    streaming_merchants = [
        "Amazon",
        "Uber",
        "Netflix",
        "Spotify",
        "DoorDash",
        "Lyft",
        "Steam",
        "App Store",
        "Google Play",
        "PayPal",
        "Venmo",
        "Cash App",
        "Robinhood",
        "Coinbase",
        "Twitch",
        "Discord",
        "Zoom",
        "Slack",
        "Notion",
        "Figma",
    ]

    # Generate transaction with streaming characteristics
    transaction = faker_gen.generate_transaction()

    # Override with streaming-specific data
    transaction.update(
        {
            "user_id": user_id,
            "category": random.choice(streaming_categories),
            "merchant": random.choice(streaming_merchants),
            "source_system": "streaming_kafka",  # Mark as streaming source
            "transaction_type": "real_time",  # Different from batch
            "processing_time_ms": random.randint(
                50, 500
            ),  # Simulate real-time processing
        }
    )

    return transaction


def main() -> None:
    load_dotenv()

    # Initialize faker generator
    faker_gen = get_faker_generator()

    # Load user IDs from generated CSV files
    user_ids = load_user_ids_from_csv()
    if user_ids:
        logger.info(f"Using {len(user_ids)} real user IDs from CSV files")
    else:
        logger.info("Using randomly generated user IDs")

    # Get Kafka configuration from environment
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Starting Kafka producer with bootstrap servers: {bootstrap}")

    # Configure producer
    producer_config = {
        "bootstrap.servers": bootstrap,
        "client.id": "faker-producer",
        "acks": "all",  # Wait for all replicas to acknowledge
        "retries": 3,  # Retry failed sends
        "linger.ms": 100,  # Batch messages for better throughput
    }

    producer = Producer(producer_config)
    topic = "transactions"

    logger.info(f"Producer configured for topic: {topic}")
    logger.info(f"Available categories: {', '.join(faker_gen.get_categories())}")
    logger.info(f"Available currencies: {', '.join(faker_gen.get_currencies())}")

    def delivery_cb(err, msg):
        if err is not None:
            logger.error(f"Delivery failed for {msg.key()}: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
            )

    try:
        message_count = 0
        logger.info("Starting to produce messages...")

        while True:
            # Generate transaction using our modified generator
            event = generate_streaming_transaction(faker_gen, user_ids)

            # Produce message
            producer.produce(
                topic, key=event["tx_id"], value=json.dumps(event), callback=delivery_cb
            )
            producer.poll(0)  # Trigger delivery reports

            message_count += 1
            logger.info(
                f"Produced message #{message_count}: tx_id={event['tx_id']} | amount={event['amount']} | category={event['category']} | user={event['user_id']} | source={event.get('source_system', 'unknown')}"
            )

            # Log every 10 messages for monitoring
            if message_count % 10 == 0:
                logger.info(f"Total messages produced: {message_count}")

            time.sleep(10.0)

    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        # Ensure outstanding messages are delivered before exit
        logger.info("Flushing remaining messages...")
        producer.flush(10.0)
        logger.info(
            f"Producer shutdown complete. Total messages produced: {message_count}"
        )


if __name__ == "__main__":
    main()
