from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv
import snowflake.connector as sc


TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    tx_id STRING PRIMARY KEY,
    user_id INTEGER,
    amount NUMBER(18,2),
    currency STRING,
    merchant STRING,
    category STRING,
    timestamp TIMESTAMP_NTZ,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
)
"""


def build_sf_conn_params() -> Dict[str, Any]:
    # Load .env once (no-op if already loaded)
    load_dotenv()
    params: Dict[str, Any] = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        params["role"] = role
    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR")
    if authenticator:
        params["authenticator"] = authenticator

    password = os.getenv("SNOWFLAKE_PASSWORD")
    if password:
        params["password"] = password
        return params

    # Key-file-based auth without loading the key in Python (let connector handle it)
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH")
    key_pwd = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD")
    if key_path:
        params["private_key_file"] = key_path
        if key_pwd:
            params["private_key_file_pwd"] = key_pwd
        # Default to JWT auth if not explicitly set
        params.setdefault("authenticator", "SNOWFLAKE_JWT")
        return params

    raise RuntimeError("Snowflake auth not configured. Set SNOWFLAKE_PASSWORD or private key envs.")


def load_csv_to_snowflake(
    csv_path: Path,
    table_name: str = "TRANSACTIONS_RAW",
    stage_name: str = "STG_TRANSACTIONS",
) -> int:
    csv_path = csv_path.resolve()
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)

    params = build_sf_conn_params()
    rows_loaded = 0

    with sc.connect(**params) as ctx:
        cs = ctx.cursor()
        try:
            # Ensure role/warehouse/database/schema are active (assume they already exist)
            role = params.get("role")
            if role:
                cs.execute(f"USE ROLE {role}")
            wh = params.get("warehouse")
            if not wh:
                raise RuntimeError("SNOWFLAKE_WAREHOUSE is required in environment")
            cs.execute(f"USE WAREHOUSE {wh}")

            db = params.get("database")
            sch = params.get("schema")
            if not db or not sch:
                raise RuntimeError("SNOWFLAKE_DATABASE and SNOWFLAKE_SCHEMA must be set in environment")

            cs.execute(f"USE DATABASE {db}")
            # Allow fully-qualified schema in env; otherwise qualify it
            fq_schema = sch if "." in sch else f"{db}.{sch}"
            cs.execute(f"USE SCHEMA {fq_schema}")

            # Assume objects were created via bootstrap SQL. We only USE and then PUT/COPY.

            # Upload the file to the stage
            cs.execute(f"PUT file://{csv_path} @{stage_name} OVERWRITE=TRUE AUTO_COMPRESS=TRUE")

            # COPY the specific staged file by name using FILES clause
            filename = csv_path.name
            staged_basename = filename + ".gz"  # PUT used AUTO_COMPRESS=TRUE
            rows_loaded = 0
            copy_sql = (
                f"COPY INTO {table_name} (TX_ID, USER_ID, AMOUNT, CURRENCY, MERCHANT, CATEGORY, TIMESTAMP) "
                f"FROM @{stage_name} "
                f"FILES = ('{staged_basename}') "
                f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='\"') "
                f"ON_ERROR = CONTINUE"
            )
            res = cs.execute(copy_sql)
            for row in res.fetchall():
                rows_loaded += int(row[3] or 0)
        finally:
            cs.close()

    return rows_loaded


def main() -> None:
    parser = argparse.ArgumentParser(description="Load a CSV directly into Snowflake transactions table via PUT/COPY")
    parser.add_argument("csv", type=Path, help="Path to CSV file")
    parser.add_argument("--table", default="TRANSACTIONS_RAW")
    parser.add_argument("--stage", default="STG_TRANSACTIONS")
    args = parser.parse_args()

    count = load_csv_to_snowflake(args.csv, args.table, args.stage)
    print(f"Loaded {count} rows into {args.table}")


if __name__ == "__main__":
    main()


