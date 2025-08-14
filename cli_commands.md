Quickly import all variables from .env to your shell
```export $(grep -v '^#' .env | xargs)```


Variable
add these for dbt, easier to manage with current folder struct.
DBT_PROFILES_DIR
DBT_PROJECT_DIR


Export requirement.txt before building docker image
```uv export --format requirements-txt --no-dev > requirements.txt```
