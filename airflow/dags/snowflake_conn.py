"""
Shared Snowflake connection module for Airflow DAGs.
Uses Airflow's connection management system for secure credential handling.
"""

from typing import Any, Dict

from airflow.hooks.base import BaseHook
from snowflake.connector import connect


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

    # Build connection parameters
    conn_params = {
        "account": extra_params.get("account") or conn.host,
        "user": conn.login,
        "password": conn.password,  # For private key passphrase if needed
        "warehouse": extra_params.get("warehouse"),
        "database": extra_params.get("database") or conn.schema,
        "schema": extra_params.get("schema") or conn.schema,
        "role": extra_params.get("role"),
    }

    # Handle private key authentication
    if "private_key_content" in extra_params:
        # Private key content stored directly in connection
        conn_params["private_key_content"] = extra_params["private_key_content"]
        conn_params["authenticator"] = "SNOWFLAKE_JWT"
    elif "private_key_file" in extra_params:
        # Private key file path
        conn_params["private_key_file"] = extra_params["private_key_file"]
        conn_params["authenticator"] = "SNOWFLAKE_JWT"

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
