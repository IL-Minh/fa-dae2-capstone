"""
Snowflake connector for querying SC_MRT schema data
"""

import logging
from typing import Any, Dict, List, Optional

import snowflake.connector
from snowflake.connector.errors import Error as SnowflakeError


class SnowflakeConnector:
    """Connector for Snowflake database operations"""

    def __init__(self, config: Dict[str, str]):
        """Initialize Snowflake connector"""
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            # Read private key from file
            with open(self.config["private_key_file"], "r") as f:
                private_key = f.read()

            self.connection = snowflake.connector.connect(
                account=self.config["account"],
                user=self.config["user"],
                private_key=private_key,
                private_key_passphrase=self.config["private_key_passphrase"],
                database=self.config["database"],
                warehouse=self.config["warehouse"],
                role=self.config["role"],
                schema=self.config["schema"],
            )

            self.logger.info("Successfully connected to Snowflake")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            return False

    def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from Snowflake")

    def execute_query(self, query: str) -> Optional[List[Dict[str, Any]]]:
        """Execute a SQL query and return results"""
        if not self.connection:
            if not self.connect():
                return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Fetch results
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))

            cursor.close()
            return results

        except SnowflakeError as e:
            self.logger.error(f"Snowflake query error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error executing query: {e}")
            return None

    def get_user_data(
        self, limit: int = 10, filters: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Get user data from dim_users table"""
        query = """
        SELECT
            USER_ID,
            USER_NAME,
            EMAIL,
            REGISTRATION_DATE,
            USER_SEGMENT,
            LOCATION,
            CREATED_AT,
            UPDATED_AT
        FROM SC_MRT.DIM_USERS
        """

        if filters:
            where_clauses = []
            for key, value in filters.items():
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        query += f" ORDER BY CREATED_AT DESC LIMIT {limit}"

        return self.execute_query(query)

    def get_transaction_data(
        self, limit: int = 10, filters: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Get transaction data from fact_transactions table"""
        query = """
        SELECT
            TX_ID,
            USER_ID,
            MERCHANT_ID,
            CATEGORY_ID,
            AMOUNT,
            CURRENCY,
            TRANSACTION_DATE,
            STATUS,
            CREATED_AT
        FROM SC_MRT.FACT_TRANSACTIONS
        """

        if filters:
            where_clauses = []
            for key, value in filters.items():
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        query += f" ORDER BY TRANSACTION_DATE DESC LIMIT {limit}"

        return self.execute_query(query)

    def get_user_transactions(
        self, user_id: str, limit: int = 20
    ) -> Optional[List[Dict[str, Any]]]:
        """Get transactions for a specific user"""
        query = f"""
        SELECT
            t.TX_ID,
            t.USER_ID,
            u.USER_NAME,
            t.MERCHANT_ID,
            t.CATEGORY_ID,
            t.AMOUNT,
            t.CURRENCY,
            t.TRANSACTION_DATE,
            t.STATUS,
            t.CREATED_AT
        FROM SC_MRT.FACT_TRANSACTIONS t
        JOIN SC_MRT.DIM_USERS u ON t.USER_ID = u.USER_ID
        WHERE t.USER_ID = '{user_id}'
        ORDER BY t.TRANSACTION_DATE DESC
        LIMIT {limit}
        """

        return self.execute_query(query)

    def get_transaction_summary(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Get summary statistics for transactions"""
        query = """
        SELECT
            COUNT(*) as total_transactions,
            SUM(AMOUNT) as total_amount,
            AVG(AMOUNT) as avg_amount,
            MIN(AMOUNT) as min_amount,
            MAX(AMOUNT) as max_amount,
            COUNT(DISTINCT USER_ID) as unique_users,
            COUNT(DISTINCT MERCHANT_ID) as unique_merchants
        FROM SC_MRT.FACT_TRANSACTIONS
        """

        if filters:
            where_clauses = []
            for key, value in filters.items():
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        results = self.execute_query(query)
        return results[0] if results else None

    def get_user_summary(
        self, filters: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Get summary statistics for users"""
        query = """
        SELECT
            COUNT(*) as total_users,
            COUNT(DISTINCT USER_SEGMENT) as unique_segments,
            COUNT(DISTINCT LOCATION) as unique_locations,
            MIN(REGISTRATION_DATE) as earliest_registration,
            MAX(REGISTRATION_DATE) as latest_registration
        FROM SC_MRT.DIM_USERS
        """

        if filters:
            where_clauses = []
            for key, value in filters.items():
                if isinstance(value, str):
                    where_clauses.append(f"{key} = '{value}'")
                else:
                    where_clauses.append(f"{key} = {value}")

            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        results = self.execute_query(query)
        return results[0] if results else None

    def test_connection(self) -> bool:
        """Test the Snowflake connection"""
        try:
            if not self.connection:
                return self.connect()

            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()

            return result[0] == 1

        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
