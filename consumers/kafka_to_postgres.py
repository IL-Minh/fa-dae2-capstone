from __future__ import annotations

import json
from datetime import datetime, timezone

import os
from confluent_kafka import Consumer
import psycopg
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS transactions_sink (
    tx_id TEXT PRIMARY KEY,
    user_id INTEGER,
    amount NUMERIC,
    currency TEXT,
    merchant TEXT,
    category TEXT,
    timestamp TIMESTAMP,
    ingested_at TIMESTAMP DEFAULT now()
)
"""

INSERT_SQL = (
    "INSERT INTO transactions_sink (tx_id, user_id, amount, currency, merchant, category, timestamp)"
    " VALUES (%(tx_id)s, %(user_id)s, %(amount)s, %(currency)s, %(merchant)s, %(category)s, %(timestamp)s)"
    " ON CONFLICT (tx_id) DO NOTHING"
)


def main() -> None:
    # Build DSN from env, allow override via PG_DSN
    dsn = os.getenv("PG_DSN")
    if not dsn:
        pg_user = os.getenv("POSTGRES_USER", "T0")
        pg_pwd = os.getenv("POSTGRES_PASSWORD", "")
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_db = os.getenv("POSTGRES_DB", "DB_T0")
        
        # Always include password in connection string
        if pg_pwd:
            dsn = f"postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}"
        else:
            dsn = f"postgresql://{pg_user}@{pg_host}:{pg_port}/{pg_db}"
        
        print(f"[consumer] Connecting to PostgreSQL at {pg_host}:{pg_port}")
    else:
        # Minimal DSN debug without leaking secrets
        print("[consumer] Using PG_DSN override")
    
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
            print("[consumer] Connected to PostgreSQL and created table successfully")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "transactions-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    topic = "transactions"
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue
            event = json.loads(msg.value().decode("utf-8"))
            print(f"[consumer] received tx_id={event.get('tx_id')} amount={event.get('amount')}")
            # ensure timestamp fits TIMESTAMP
            ts = event.get("timestamp")
            if isinstance(ts, str):
                try:
                    datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    ts = datetime.now(timezone.utc).isoformat()
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        INSERT_SQL,
                        {
                            "tx_id": event["tx_id"],
                            "user_id": event["user_id"],
                            "amount": event["amount"],
                            "currency": event["currency"],
                            "merchant": event["merchant"],
                            "category": event["category"],
                            "timestamp": ts,
                        },
                    )
            print(f"[consumer] ingested tx_id={event.get('tx_id')} into Postgres")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()


