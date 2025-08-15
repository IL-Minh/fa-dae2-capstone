# Data Cleanup Script

## Overview
The `cleanup_all_data.py` script provides a comprehensive way to clear all data from your data pipeline components for fresh demos.

## What It Cleans

### 🗄️ **PostgreSQL**
- Clears all data from Kafka sink database tables
- Preserves table structure and migrations
- Uses environment variables for connection

### 📨 **Kafka**
- Deletes all topics (clears all data)
- Preserves system topics (e.g., `__consumer_offsets`)
- Uses environment variables for connection

### ❄️ **Snowflake**
- Clears all data from `SC_RAW` schema tables and views
- Drops `SC_STG`, `SC_INT`, and `SC_MRT` schemas completely
- Uses JWT authentication from environment variables

### 📁 **Local Files**
- Removes generated CSV files from `data/incoming/`
- Preserves original sample files
- Safe for repeated use

## Usage

### **Clean Everything (Default)**
```bash
uv run scripts/cleanup_all_data.py
```
⚠️ **Requires confirmation** - Type `yes` when prompted

### **Clean Specific Components**
```bash
# Clean only PostgreSQL
uv run scripts/cleanup_all_data.py --components postgres

# Clean PostgreSQL and Kafka
uv run scripts/cleanup_all_data.py --components postgres kafka

# Clean only local files
uv run scripts/cleanup_all_data.py --components files
```

### **Skip Confirmation (Use with caution!)**
```bash
uv run scripts/cleanup_all_data.py --confirm
```

### **Help**
```bash
uv run scripts/cleanup_all_data.py --help
```

## Environment Variables Required

### **PostgreSQL**
```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=kafka_sink
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
```

### **Kafka**
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
```

### **Snowflake**
```bash
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_JWT=SNOWFLAKE_JWT
SNOWFLAKE_PRIVATE_KEY_FILE_PATH=path/to/key
SNOWFLAKE_PRIVATE_KEY_FILE_PWD=your_passphrase
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

## Safety Features

### **Confirmation Prompt**
- Script asks for confirmation before deleting data
- Type `yes` or `y` to proceed
- Any other input cancels the operation

### **System Protection**
- Preserves PostgreSQL migration tables
- Preserves Kafka system topics
- Preserves original sample CSV files

### **Error Handling**
- Continues with other components if one fails
- Provides detailed logging of all operations
- Shows summary of successes and failures

## Use Cases

### **Fresh Demo Setup**
```bash
# Clear everything for a completely fresh start
python scripts/cleanup_all_data.py
```

### **Component Testing**
```bash
# Test just the file generation
python scripts/cleanup_all_data.py --components files
# Generate new data
python scripts/generate_monthly_data.py
```

### **Development Iteration**
```bash
# Clear Snowflake for model testing
python scripts/cleanup_all_data.py --components snowflake
# Re-run dbt models
dbt run
```

## Example Workflow

```bash
# 1. Clean everything for fresh demo
python scripts/cleanup_all_data.py

# 2. Generate new sample data
python scripts/generate_monthly_data.py --year 2025 --month 8 --transactions 500 --users 100

# 3. Run your data pipeline
# ... your Airflow DAGs, dbt models, etc.

# 4. When ready for next demo, repeat from step 1
```

## Troubleshooting

### **Connection Issues**
- Verify environment variables are set correctly
- Check if services (PostgreSQL, Kafka, Snowflake) are running
- Ensure network connectivity

### **Permission Issues**
- Verify database user has TRUNCATE permissions
- Check Kafka admin permissions
- Ensure Snowflake user has schema management rights

### **Partial Failures**
- Check logs for specific error messages
- Some components may fail while others succeed
- Use `--components` to retry specific components

## ⚠️ **Important Notes**

- **This script DELETES ALL DATA** - use with caution
- **No backup is created** - ensure you have backups if needed
- **Designed for development/demo environments** - not for production
- **Always review what will be cleaned** before running

## Support

If you encounter issues:
1. Check the logs for specific error messages
2. Verify environment variables are set correctly
3. Ensure all services are running and accessible
4. Check permissions for each component
