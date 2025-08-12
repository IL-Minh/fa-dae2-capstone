"""
Shared Snowflake connection module for Airflow DAGs.
Uses Airflow's connection management system for secure credential handling.
"""

from typing import Any, Dict

from snowflake.connector import connect

from airflow.hooks.base import BaseHook


def get_snowflake_connection_params(
    conn_id: str = "snowflake_default",
) -> Dict[str, Any]:
    """
    Get Snowflake connection parameters from Airflow connection.

    Args:
        conn_id: Airflow connection ID (default: 'snowflake_default')

    Returns:
        Dictionary of connection parameters for snowflake.connector.connect()

    Raises:
        ValueError: If connection is not properly configured
    """
    # Get connection from Airflow
    conn = BaseHook.get_connection(conn_id)

    if conn.conn_type != "snowflake":
        raise ValueError(f"Connection {conn_id} is not a Snowflake connection")

    # Parse extra JSON for Snowflake-specific parameters
    extra_params = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

    # Build connection parameters following sf_conn.py structure
    conn_params = {
        "account": conn.host,  # Account is stored in host field
        "user": conn.login,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": extra_params.get("private_key_file"),
        "private_key_file_pwd": conn.password,  # Password for encrypted private key
        "warehouse": extra_params.get("warehouse"),
        "database": extra_params.get("database"),
        "schema": conn.schema,
    }

    # Validate required parameters
    required = ["account", "user", "warehouse", "database", "schema"]
    missing = [k for k in required if not conn_params.get(k)]
    if missing:
        raise ValueError(f"Missing required Snowflake parameters: {missing}")

    return conn_params


def get_snowflake_connection(conn_id: str = "snowflake_default"):
    """
    Get a Snowflake connection object.

    Args:
        conn_id: Airflow connection ID (default: 'snowflake_default')

    Returns:
        Snowflake connection object
    """
    params = get_snowflake_connection_params(conn_id)
    return connect(**params)


def test_snowflake_connection(conn_id: str = "snowflake_default") -> bool:
    """
    Test Snowflake connection.

    Args:
        conn_id: Airflow connection ID (default: 'snowflake_default')

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with get_snowflake_connection(conn_id) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT current_version()")
                result = cursor.fetchone()
                print(f"Snowflake connection successful. Version: {result[0]}")
                return True
    except Exception as e:
        print(f"Snowflake connection failed: {e}")
        return False
