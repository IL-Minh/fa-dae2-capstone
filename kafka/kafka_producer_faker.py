from __future__ import annotations

import json
import logging
import os
import time

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


def main() -> None:
    load_dotenv()

    # Initialize faker generator
    faker_gen = get_faker_generator()

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
            # Generate transaction using our modular faker generator
            event = faker_gen.generate_transaction()

            # Produce message
            producer.produce(
                topic, key=event["tx_id"], value=json.dumps(event), callback=delivery_cb
            )
            producer.poll(0)  # Trigger delivery reports

            message_count += 1
            logger.info(
                f"Produced message #{message_count}: tx_id={event['tx_id']} | amount={event['amount']} | category={event['category']} | user={event['user_id']}"
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
