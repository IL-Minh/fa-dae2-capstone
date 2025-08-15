"""
Faker Data Generator Module for Kafka Producer

This module provides a clean interface for generating fake transaction data including:
- Transaction event generation
- Category and currency management
- Timestamp handling
- Data validation and formatting
"""

import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict

from faker import Faker

logger = logging.getLogger(__name__)


class FakerGenerator:
    """Faker-based data generator for transaction events."""

    def __init__(self):
        """Initialize the faker generator with default settings."""
        self.fake = Faker()
        self.rng = random.Random()

        # Predefined categories for consistent data
        self.categories = [
            "groceries",
            "rent",
            "salary",
            "entertainment",
            "utilities",
            "transport",
            "health",
            "shopping",
            "dining",
            "education",
        ]

        # Currency options
        self.currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]

        logger.info(
            f"Faker generator initialized with {len(self.categories)} categories"
        )

    def generate_transaction(self, user_id: str = None) -> Dict[str, Any]:
        """
        Generate a single transaction event.

        Args:
            user_id: Optional user ID override, otherwise generates random UUID

        Returns:
            Dict containing transaction data
        """
        # Generate user ID if not provided
        if user_id is None:
            user_id = self.fake.uuid4()  # Use UUID instead of integer

        # Generate transaction data
        transaction = {
            "tx_id": self.fake.uuid4(),
            "user_id": user_id,
            "amount": round(self.rng.uniform(-500.0, 3000.0), 2),
            "currency": self.rng.choice(self.currencies),
            "merchant": self.fake.company(),
            "category": self.rng.choice(self.categories),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        logger.debug(
            f"Generated transaction: tx_id={transaction['tx_id']} | amount={transaction['amount']} | category={transaction['category']}"
        )
        return transaction

    def generate_transactions_batch(
        self, count: int, user_id: str = None
    ) -> list[Dict[str, Any]]:
        """
        Generate multiple transaction events.

        Args:
            count: Number of transactions to generate
            user_id: Optional user ID override for all transactions

        Returns:
            List of transaction dictionaries
        """
        if count <= 0:
            logger.warning(f"Invalid count requested: {count}")
            return []

        transactions = []
        for i in range(count):
            transaction = self.generate_transaction(user_id)
            transactions.append(transaction)

            # Log progress for large batches
            if count > 100 and (i + 1) % 100 == 0:
                logger.info(f"Generated {i + 1}/{count} transactions")

        logger.info(f"Generated batch of {len(transactions)} transactions")
        return transactions

    def get_categories(self) -> list[str]:
        """Get available transaction categories."""
        return self.categories.copy()

    def get_currencies(self) -> list[str]:
        """Get available currencies."""
        return self.currencies.copy()

    def add_category(self, category: str) -> bool:
        """
        Add a new category to the available options.

        Args:
            category: New category to add

        Returns:
            bool: True if added successfully, False if already exists
        """
        if category not in self.categories:
            self.categories.append(category)
            logger.info(f"Added new category: {category}")
            return True
        else:
            logger.debug(f"Category already exists: {category}")
            return False

    def set_seed(self, seed: int) -> None:
        """
        Set random seed for reproducible data generation.

        Args:
            seed: Random seed value
        """
        self.rng.seed(seed)
        self.fake.seed_instance(seed)
        logger.info(f"Random seed set to: {seed}")


# Convenience function for quick data generation
def get_faker_generator() -> FakerGenerator:
    """Get a configured faker generator instance."""
    return FakerGenerator()
