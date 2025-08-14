"""
PostgreSQL Client Module for Kafka Consumer

This module provides a clean interface for PostgreSQL operations including:
- Connection management
- Table creation
- Data insertion
- Connection pooling and error handling
"""

import logging
import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import psycopg
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class PostgresClient:
    """PostgreSQL client for managing database connections and operations."""

    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize PostgreSQL client.

        Args:
            dsn: Optional connection string override
        """
        self.dsn = dsn or self._build_dsn()
        self._connection_pool = []

    def _build_dsn(self) -> str:
        """Build connection string from environment variables."""
        pg_user = os.getenv("POSTGRES_USER", "T0")
        pg_pwd = os.getenv("POSTGRES_PASSWORD", "")
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_db = os.getenv("POSTGRES_DB", "DB_T0")

        # For local testing, ensure we connect to Docker PostgreSQL
        if pg_host == "localhost" and pg_port == "5432":
            logger.info(
                "Local testing detected - connecting to Docker PostgreSQL container"
            )

        # Build connection string
        if pg_pwd:
            dsn = f"postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_db}"
        else:
            dsn = f"postgresql://{pg_user}@{pg_host}:{pg_port}/{pg_db}"

        logger.info(f"PostgreSQL connection configured for {pg_host}:{pg_port}")
        return dsn

    @contextmanager
    def get_connection(self):
        """
        Get a database connection with automatic cleanup.

        Yields:
            psycopg.Connection: Database connection
        """
        conn = None
        try:
            conn = psycopg.connect(self.dsn, autocommit=True)
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def test_connection(self) -> bool:
        """
        Test database connection.

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def create_transactions_table(self) -> bool:
        """
        Create the transactions_sink table if it doesn't exist.

        Returns:
            bool: True if table created/exists, False otherwise
        """
        create_sql = """
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

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(create_sql)
                    logger.info("Transactions table created/verified successfully")
                    return True
        except Exception as e:
            logger.error(f"Failed to create transactions table: {e}")
            return False

    def get_row_count(self) -> int:
        """
        Get the current row count in transactions_sink table.

        Returns:
            int: Number of rows in the table
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM transactions_sink")
                    result = cur.fetchone()
                    return result[0] if result else 0
        except Exception as e:
            logger.error(f"Failed to get row count: {e}")
            return 0

    def insert_transaction(self, transaction_data: Dict[str, Any]) -> bool:
        """
        Insert a transaction into the database.

        Args:
            transaction_data: Dictionary containing transaction fields

        Returns:
            bool: True if insert successful, False otherwise
        """
        insert_sql = """
        INSERT INTO transactions_sink (tx_id, user_id, amount, currency, merchant, category, timestamp)
        VALUES (%(tx_id)s, %(user_id)s, %(amount)s, %(currency)s, %(merchant)s, %(category)s, %(timestamp)s)
        ON CONFLICT (tx_id) DO NOTHING
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(insert_sql, transaction_data)
                    return cur.rowcount > 0
        except Exception as e:
            logger.error(
                f"Failed to insert transaction {transaction_data.get('tx_id', 'unknown')}: {e}"
            )
            return False

    def insert_transactions_batch(
        self, transactions: list[Dict[str, Any]]
    ) -> tuple[int, int]:
        """
        Insert multiple transactions in a batch.

        Args:
            transactions: List of transaction dictionaries

        Returns:
            tuple: (successful_inserts, total_attempts)
        """
        if not transactions:
            return 0, 0

        successful = 0
        total = len(transactions)

        for transaction in transactions:
            if self.insert_transaction(transaction):
                successful += 1

        logger.info(f"Batch insert completed: {successful}/{total} successful")
        return successful, total


# Convenience function for quick database operations
def get_postgres_client() -> PostgresClient:
    """Get a configured PostgreSQL client instance."""
    return PostgresClient()
