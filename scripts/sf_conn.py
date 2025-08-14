import os

import snowflake.connector as sc
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

ctx = sc.connect(**conn_params)
cs = ctx.cursor()

cs.execute("SELECT current_version()")
result = cs.fetchone()
print(result)

cs.close()
