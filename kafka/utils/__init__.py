"""
Kafka Utils Package

This package contains utility components for the Kafka ecosystem:
- postgres_client: PostgreSQL connection and operations
- faker_generator: Fake data generation for testing
"""

from .faker_generator import FakerGenerator, get_faker_generator
from .postgres_client import PostgresClient, get_postgres_client

__all__ = [
    "PostgresClient",
    "get_postgres_client",
    "FakerGenerator",
    "get_faker_generator",
]
