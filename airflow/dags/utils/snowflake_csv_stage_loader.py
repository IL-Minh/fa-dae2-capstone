"""
Snowflake loading utilities for Airflow DAGs.
Provides reusable functions for processing CSV files and bulk loading into Snowflake.
"""

import csv
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeCSVStageLoader:
    """
    Utility class for processing CSV files and loading them into Snowflake using COPY INTO.
    Uses proper logging instead of print statements.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_csv_files(
        self, incoming_dir: str = "data/incoming"
    ) -> List[Dict[str, Any]]:
        """
        Process CSV files from the incoming directory.

        Args:
            incoming_dir: Path to directory containing CSV files

        Returns:
            List of processing results for each file
        """
        self.logger.info(f"Processing CSV files from {incoming_dir}")

        # Check if directory exists
        incoming_path = Path(incoming_dir)
        if not incoming_path.exists():
            raise FileNotFoundError(f"Directory {incoming_dir} does not exist")

        # List CSV files
        csv_files = list(incoming_path.glob("*.csv"))
        if not csv_files:
            self.logger.info("No CSV files found in incoming directory")
            return []

        self.logger.info(f"Found {len(csv_files)} CSV files to process")

        results = []

        # Process each CSV file
        for csv_file in csv_files:
            try:
                result = self._process_single_csv(csv_file)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error processing {csv_file.name}: {e}")
                results.append(
                    {"file": csv_file.name, "status": "error", "error": str(e)}
                )

        return results

    def _process_single_csv(self, csv_file: Path) -> Dict[str, Any]:
        """
        Process a single CSV file and load it into Snowflake.

        Args:
            csv_file: Path to the CSV file

        Returns:
            Dict containing processing results
        """
        self.logger.info(f"Processing file: {csv_file.name}")

        # Create a temporary file with cleaned data for Snowflake
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline=""
        ) as temp_file:
            temp_path = temp_file.name

            try:
                # Process CSV data
                row_count = self._clean_csv_data(csv_file, temp_file)

                if row_count == 0:
                    return {
                        "file": csv_file.name,
                        "status": "skipped",
                        "reason": "No valid rows found",
                    }

                # Load to Snowflake
                load_result = self._load_to_snowflake(temp_path, csv_file.name)

                return {
                    "file": csv_file.name,
                    "status": "success",
                    "rows_processed": row_count,
                    "load_result": load_result,
                }

            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_path)
                except OSError:
                    # Ignore errors when cleaning up temp file
                    pass

    def _clean_csv_data(self, csv_file: Path, temp_file) -> int:
        """
        Clean and validate CSV data, writing to temporary file.

        Args:
            csv_file: Path to the original CSV file
            temp_file: Temporary file object to write cleaned data

        Returns:
            Number of rows processed
        """
        # Read original CSV and write cleaned data to temp file
        with open(csv_file, "r") as original_file:
            reader = csv.reader(original_file)
            writer = csv.writer(temp_file)

            # Read header first
            header = next(reader)
            self.logger.info(f"  Header: {header}")

            # Verify required columns exist
            required_columns = [
                "tx_id",
                "user_id",
                "amount",
                "currency",
                "merchant",
                "category",
                "timestamp",
            ]
            missing_columns = [col for col in required_columns if col not in header]
            if missing_columns:
                self.logger.warning(
                    f"  ⚠️  Skipping {csv_file.name} - missing columns: {missing_columns}"
                )
                return 0

            # Write header to temp file
            writer.writerow(header)

            # Process and write data rows
            row_count = 0
            for row in reader:
                row_count += 1
                if len(row) >= 7:  # Ensure we have enough columns
                    try:
                        # Clean and validate data
                        cleaned_row = [
                            row[0],  # tx_id
                            int(row[1]),  # user_id
                            float(row[2]),  # amount
                            row[3],  # currency
                            row[4],  # merchant
                            row[5],  # category
                            row[6],  # timestamp (keep as-is, Snowflake will parse it)
                            csv_file.name,  # source_file
                            datetime.now(timezone.utc).isoformat(),  # ingested_at
                        ]
                        writer.writerow(cleaned_row)
                    except (ValueError, IndexError) as e:
                        self.logger.warning(
                            f"    ⚠️  Error processing row {row_count}: {e}"
                        )
                        continue

        self.logger.info(f"  ✅ Processed {row_count} rows from {csv_file.name}")
        return row_count

    def _load_to_snowflake(
        self,
        temp_path: str,
        source_filename: str,
        snowflake_conn_id: str = "snowflake_default",
    ) -> Dict[str, Any]:
        """
        Load cleaned CSV data into Snowflake using COPY INTO.

        Args:
            temp_path: Path to temporary CSV file
            source_filename: Original source filename for tracking
            snowflake_conn_id: Airflow connection ID for Snowflake

        Returns:
            Dict containing load results
        """
        self.logger.info("  🗄️  Bulk loading data into Snowflake using COPY INTO...")

        try:
            snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

            # Upload CSV file to the permanent stage
            put_sql = f"""
            PUT file://{temp_path} @STG_TRANSACTIONS_BATCH_CSV
            """
            snowflake_hook.run(put_sql)
            self.logger.info("    ✅ Uploaded CSV to permanent stage")

            # Use COPY INTO to bulk load data
            copy_sql = f"""
            COPY INTO TRANSACTIONS_BATCH_CSV (
                TX_ID, USER_ID, AMOUNT, CURRENCY, MERCHANT,
                CATEGORY, TIMESTAMP, SOURCE_FILE, INGESTED_AT
            )
            FROM @STG_TRANSACTIONS_BATCH_CSV/{os.path.basename(temp_path)}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('NULL', 'null', '')
                EMPTY_FIELD_AS_NULL = TRUE
                TIMESTAMP_FORMAT = 'AUTO'
            )
            ON_ERROR = 'CONTINUE'
            """

            result = snowflake_hook.run(copy_sql)
            self.logger.info("    ✅ COPY INTO completed successfully")
            self.logger.info(f"    📊 Result: {result}")

            return {
                "status": "success",
                "result": result,
                "source_file": source_filename,
            }

        except Exception as e:
            self.logger.error(f"    ❌ Error during Snowflake bulk load: {e}")
            raise

    def process_csv_batch(
        self,
        incoming_dir: str = "data/incoming",
        snowflake_conn_id: str = "snowflake_default",
    ) -> Dict[str, Any]:
        """
        Complete CSV batch processing pipeline.

        Args:
            incoming_dir: Path to directory containing CSV files
            snowflake_conn_id: Airflow connection ID for Snowflake

        Returns:
            Dict containing overall processing results
        """
        try:
            results = self.process_csv_files(incoming_dir)

            # Calculate summary statistics
            total_files = len(results)
            successful_files = len([r for r in results if r["status"] == "success"])
            failed_files = len([r for r in results if r["status"] == "error"])
            skipped_files = len([r for r in results if r["status"] == "skipped"])

            total_rows = sum(
                [
                    r.get("rows_processed", 0)
                    for r in results
                    if r["status"] == "success"
                ]
            )

            self.logger.info("🎉 CSV batch ingestion completed successfully!")

            return {
                "status": "success",
                "summary": {
                    "total_files": total_files,
                    "successful_files": successful_files,
                    "failed_files": failed_files,
                    "skipped_files": skipped_files,
                    "total_rows_processed": total_rows,
                },
                "file_results": results,
            }

        except Exception as e:
            self.logger.error(f"Error in CSV batch processing: {e}")
            raise
