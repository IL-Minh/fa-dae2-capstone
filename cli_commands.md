Quickly import all variables from .env to your shell
```export $(grep -v '^#' .env | xargs)```


Variable
add these for dbt, easier to manage with current folder struct.
DBT_PROFILES_DIR
DBT_PROJECT_DIR


Export requirement.txt before building docker image
```uv export --format requirements-txt --no-dev > requirements.txt```


Set Github secret (privatekey) using local gh package
```sh
    gh secret set SNOWFLAKE_PRIVATE_KEY_FILE --body "$(cat "$SNOWFLAKE_PRIVATE_KEY_FILE_PATH")"
    gh secret set SNOWFLAKE_PRIVATE_KEY_FILE_PWD --body "$SNOWFLAKE_PRIVATE_KEY_FILE_PWD"
    gh secret set SNOWFLAKE_PRIVATE_KEY_FILE_PATH --body "$SNOWFLAKE_PRIVATE_KEY_FILE_PATH"

    gh variable set SNOWFLAKE_ACCOUNT --body "$SNOWFLAKE_ACCOUNT"
    gh variable set SNOWFLAKE_USER --body "$SNOWFLAKE_USER"
    gh variable set SNOWFLAKE_DATABASE --body "$SNOWFLAKE_DATABASE"
    gh variable set SNOWFLAKE_WAREHOUSE --body "$SNOWFLAKE_WAREHOUSE"
    gh variable set SNOWFLAKE_ROLE --body "$SNOWFLAKE_ROLE"
    gh variable set SNOWFLAKE_SCHEMA --body "$SNOWFLAKE_SCHEMA"
    gh variable set DBT_PROFILES_DIR --body "$DBT_PROFILES_DIR"
    gh variable set DBT_PROJECT_DIR --body "$DBT_PROJECT_DIR"
```
