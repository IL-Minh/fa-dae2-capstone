from __future__ import annotations

import json
import time
import random
from datetime import datetime, timezone

import os
from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker


def main() -> None:
    load_dotenv()
    fake = Faker()
    rng = random.Random()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"[producer] bootstrap={bootstrap}")
    producer = Producer({"bootstrap.servers": bootstrap})
    topic = "transactions"
    categories = [
        "groceries",
        "rent",
        "salary",
        "entertainment",
        "utilities",
        "transport",
        "health",
    ]

    def delivery_cb(err, msg):
        if err is not None:
            print(f"Delivery failed for {msg.key()}: {err}")
        else:
            print(f"Produced to {msg.topic()}[{msg.partition()}] @ {msg.offset()}")

    try:
        while True:
            event = {
                "tx_id": fake.uuid4(),
                "user_id": rng.randint(1, 5),
                "amount": round(rng.uniform(-200.0, 2000.0), 2),
                "currency": "USD",
                "merchant": fake.company(),
                "category": rng.choice(categories),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            producer.produce(topic, key=event["tx_id"], value=json.dumps(event), callback=delivery_cb)
            producer.poll(0)
            print(f"[producer] produced tx_id={event['tx_id']} amount={event['amount']}")
            time.sleep(10.0)
    finally:
        # Ensure outstanding messages are delivered before exit
        producer.flush(10.0)


if __name__ == "__main__":
    main()


