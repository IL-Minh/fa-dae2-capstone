#!/usr/bin/env python3
"""
Data Cleanup Script for Fresh Demos
Clears all data from PostgreSQL, Kafka, and Snowflake SC_RAW schema
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.append(str(Path(__file__).parent.parent))

import psycopg
import snowflake.connector as sc
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def cleanup_postgres():
    """Clean all data from PostgreSQL Kafka sink database"""
    try:
        # Get PostgreSQL connection from environment
        import os

        from dotenv import load_dotenv

        load_dotenv()

        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = os.getenv("POSTGRES_PORT", "5432")
        pg_db = os.getenv("POSTGRES_DB", "kafka_sink")
        pg_user = os.getenv("POSTGRES_USER", "postgres")
        pg_password = os.getenv("POSTGRES_PASSWORD")

        if not pg_password:
            logger.error("POSTGRES_PASSWORD not set in environment")
            return False

        # Connect to PostgreSQL
        conn_str = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                logger.info("Connected to PostgreSQL")

                # Get table names
                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                """)
                tables = [row[0] for row in cur.fetchall()]

                logger.info(f"Found tables: {tables}")

                # Clear all data from tables
                for table in tables:
                    if table != "alembic_version":  # Don't clear migration table
                        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                        logger.info(f"Cleared table: {table}")

                conn.commit()
                logger.info("✅ PostgreSQL cleanup completed")
                return True

    except Exception as e:
        logger.error(f"PostgreSQL cleanup failed: {e}")
        return False


def cleanup_kafka():
    """Clean all data from Kafka topics"""
    try:
        # Get Kafka configuration from environment
        import os

        from dotenv import load_dotenv

        load_dotenv()

        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

        # Create admin client
        admin_client = AdminClient(
            {"bootstrap.servers": kafka_bootstrap, "client.id": "cleanup_script"}
        )

        logger.info(f"Connected to Kafka at {kafka_bootstrap}")

        # Get all topics
        metadata = admin_client.list_topics(timeout=10)
        topics = list(metadata.topics.keys())

        logger.info(f"Found topics: {topics}")

        # Delete all topics (this will clear all data)
        for topic in topics:
            if topic != "__consumer_offsets":  # Don't delete system topic
                try:
                    admin_client.delete_topics([topic], timeout=10)
                    logger.info(f"Deleted topic: {topic}")
                except KafkaException as e:
                    logger.warning(f"Could not delete topic {topic}: {e}")

        logger.info("✅ Kafka cleanup completed")
        return True

    except Exception as e:
        logger.error(f"Kafka cleanup failed: {e}")
        return False


def cleanup_snowflake():
    """Clean all data from Snowflake SC_RAW schema"""
    try:
        # Get Snowflake connection parameters from environment
        import os

        from dotenv import load_dotenv

        load_dotenv()

        conn_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            "private_key_file_pwd": os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }

        # Connect to Snowflake
        conn = sc.connect(**conn_params)
        cursor = conn.cursor()

        logger.info("Connected to Snowflake")

        # Get all tables in SC_RAW schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'SC_RAW'
            AND table_type = 'BASE TABLE'
        """)
        tables = [row[0] for row in cursor.fetchall()]

        logger.info(f"Found tables in SC_RAW: {tables}")

        # Clear all data from tables
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE SC_RAW.{table}")
            logger.info(f"Cleared table: SC_RAW.{table}")

        # Get all views in SC_RAW schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = 'SC_RAW'
        """)
        views = [row[0] for row in cursor.fetchall()]

        logger.info(f"Found views in SC_RAW: {views}")

        # Clear all data from views (if they're materialized)
        for view in views:
            try:
                cursor.execute(f"TRUNCATE TABLE SC_RAW.{view}")
                logger.info(f"Cleared view: SC_RAW.{view}")
            except Exception:
                logger.info(f"View SC_RAW.{view} is not materialized, skipping")

        # Clear other schemas if they exist
        schemas_to_clear = ["SC_STG", "SC_INT", "SC_MRT"]

        for schema in schemas_to_clear:
            try:
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                logger.info(f"Dropped schema: {schema}")
            except Exception:
                logger.info(f"Schema {schema} doesn't exist or couldn't be dropped")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("✅ Snowflake cleanup completed")
        return True

    except Exception as e:
        logger.error(f"Snowflake cleanup failed: {e}")
        return False


def cleanup_local_files():
    """Clean generated CSV files"""
    try:
        incoming_dir = Path("data/incoming")

        if incoming_dir.exists():
            # Remove all CSV files except the original sample
            for csv_file in incoming_dir.glob("*.csv"):
                if csv_file.name != "transactions_2025_07.csv":  # Keep original sample
                    csv_file.unlink()
                    logger.info(f"Deleted file: {csv_file}")

        logger.info("✅ Local file cleanup completed")
        return True

    except Exception as e:
        logger.error(f"Local file cleanup failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Clean all data from PostgreSQL, Kafka, and Snowflake for fresh demos"
    )
    parser.add_argument(
        "--components",
        nargs="+",
        choices=["postgres", "kafka", "snowflake", "files", "all"],
        default=["all"],
        help="Which components to clean (default: all)",
    )
    parser.add_argument(
        "--confirm", action="store_true", help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    # Determine what to clean
    if "all" in args.components:
        components_to_clean = ["postgres", "kafka", "snowflake", "files"]
    else:
        components_to_clean = args.components

    logger.info(f"Will clean: {components_to_clean}")

    # Confirmation prompt
    if not args.confirm:
        logger.warning(
            "\n⚠️  WARNING: This will DELETE ALL DATA from the specified components!"
        )
        logger.warning("This action cannot be undone.")
        logger.info(f"\nComponents to clean: {components_to_clean}")

        response = input("\nAre you sure you want to continue? (yes/no): ")
        if response.lower() not in ["yes", "y"]:
            logger.info("Cleanup cancelled by user")
            return

    # Perform cleanup
    results = {}

    if "postgres" in components_to_clean:
        logger.info("🧹 Cleaning PostgreSQL...")
        results["postgres"] = cleanup_postgres()

    if "kafka" in components_to_clean:
        logger.info("🧹 Cleaning Kafka...")
        results["kafka"] = cleanup_kafka()

    if "snowflake" in components_to_clean:
        logger.info("🧹 Cleaning Snowflake...")
        results["snowflake"] = cleanup_snowflake()

    if "files" in components_to_clean:
        logger.info("🧹 Cleaning local files...")
        results["files"] = cleanup_local_files()

    # Summary
    logger.info("\n" + "=" * 50)
    logger.info("🧹 CLEANUP SUMMARY")
    logger.info("=" * 50)

    for component, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"{component.upper():12} : {status}")

    all_success = all(results.values())
    if all_success:
        logger.info("\n🎉 All cleanup operations completed successfully!")
        logger.info("Your environment is now ready for a fresh demo.")
    else:
        logger.warning("\n⚠️  Some cleanup operations failed. Check the logs above.")

    logger.info("=" * 50)


if __name__ == "__main__":
    main()
