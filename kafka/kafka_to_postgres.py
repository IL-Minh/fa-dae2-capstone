from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer

# Import our modular PostgreSQL client
from utils.postgres_client import get_postgres_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [CONSUMER] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    # Initialize PostgreSQL client
    pg_client = get_postgres_client()

    # Test PostgreSQL connection and create table
    if not pg_client.test_connection():
        logger.error("Failed to connect to PostgreSQL. Exiting.")
        return

    if not pg_client.create_transactions_table():
        logger.error("Failed to create transactions table. Exiting.")
        return

    # Get initial row count
    initial_count = pg_client.get_row_count()
    logger.info(
        f"Connected to PostgreSQL successfully. Initial row count: {initial_count}"
    )

    # Configure Kafka consumer
    import os

    # Get Kafka configuration from environment
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka at: {bootstrap}")

    consumer_config = {
        "bootstrap.servers": bootstrap,
        "group.id": "transactions-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
    }

    consumer = Consumer(consumer_config)
    topic = "transactions"
    consumer.subscribe([topic])

    logger.info(f"Subscribed to topic: {topic}")
    logger.info("Starting to consume messages...")

    rows_written = 0
    messages_processed = 0
    errors_encountered = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                errors_encountered += 1
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                messages_processed += 1

                logger.debug(
                    f"Processing message #{messages_processed}: tx_id={event.get('tx_id')} | amount={event.get('amount')} | category={event.get('category')}"
                )

                # Ensure timestamp fits TIMESTAMP
                ts = event.get("timestamp")
                if isinstance(ts, str):
                    try:
                        datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except Exception:
                        ts = datetime.now(timezone.utc).isoformat()
                        logger.warning(
                            f"Invalid timestamp format, using current time for tx_id: {event.get('tx_id')}"
                        )

                # Insert into PostgreSQL using our client
                transaction_data = {
                    "tx_id": event["tx_id"],
                    "user_id": event["user_id"],
                    "amount": event["amount"],
                    "currency": event["currency"],
                    "merchant": event["merchant"],
                    "category": event["category"],
                    "timestamp": ts,
                }

                if pg_client.insert_transaction(transaction_data):
                    rows_written += 1
                    logger.info(
                        f"✅ SUCCESS: Inserted tx_id={event.get('tx_id')} (row #{rows_written})"
                    )

                    # Get current total count
                    total_count = pg_client.get_row_count()
                    logger.debug(f"📊 Total rows in table: {total_count}")
                else:
                    logger.info(
                        f"⚠️  SKIPPED: tx_id={event.get('tx_id')} (duplicate or conflict)"
                    )

                # Log progress every 10 messages
                if messages_processed % 10 == 0:
                    logger.info(
                        f"Progress: {messages_processed} messages processed, {rows_written} rows written, {errors_encountered} errors"
                    )

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for message: {e}")
                errors_encountered += 1
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                errors_encountered += 1

    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted. Final stats:")
        logger.info(f"   Messages processed: {messages_processed}")
        logger.info(f"   Rows written: {rows_written}")
        logger.info(f"   Errors encountered: {errors_encountered}")
    finally:
        consumer.close()
        logger.info(f"🚪 Consumer closed. Final row count: {rows_written}")


if __name__ == "__main__":
    main()
