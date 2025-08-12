#!/usr/bin/env python3
"""
Simple script to test Snowflake connection in Airflow container.
Run this from within the Airflow container.
"""

import sys

# Add the dags directory to Python path
sys.path.append("/opt/airflow/dags")

try:
    from snowflake_conn import test_snowflake_connection

    print("✅ Successfully imported snowflake_conn module")

    # Test the connection
    if test_snowflake_connection():
        print("✅ Snowflake connection test successful!")
    else:
        print("❌ Snowflake connection test failed!")

except ImportError as e:
    print(f"❌ Import error: {e}")
    print("This suggests the module path is not correct")
except Exception as e:
    print(f"❌ Error testing connection: {e}")
