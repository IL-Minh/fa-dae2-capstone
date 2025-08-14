# Batch Data Ingestion DAG - Quick Setup

## 🚀 Quick Start Checklist

### Prerequisites
- [ ] Airflow is running (`docker compose -f airflow-docker-compose.yml up -d`)
- [ ] Kafka stack is running (`docker compose -f kafka-docker-compose.yml up -d`)
- [ ] Snowflake connection is configured
- [ ] Environment variables are set in `.env`
- [ ] Airflow UID is set (`export AIRFLOW_UID=$(id -u)`)

### Setup Steps

#### 1. Create Snowflake Tables and Stage
```bash
# Copy and execute the SQL from scripts/snowflake_bootstrap_batch_ingestion.sql
# in your Snowflake worksheet

# This creates:
# - TRANSACTIONS_STREAMING_KAFKA (for PostgreSQL streaming data)
# - TRANSACTIONS_BATCH_CSV (for CSV batch data)
# - STG_TRANSACTIONS_BATCH_CSV (permanent stage for bulk loading)
```

#### 2. Set Up PostgreSQL Connection
```bash
# From project root
./scripts/setup_airflow_postgres_connection.sh
```

#### 3. Verify Snowflake Connection
```bash
# From project root
./scripts/setup_airflow_snowflake_connection.sh
```

#### 4. Check DAG Status
- Open Airflow UI: http://localhost:8080
- Look for `batch_data_ingestion_dag`
- Ensure it's not paused
- Check for any connection errors

### Expected Behavior

- **DAG Schedule**: Runs every 15 minutes
- **Timezone**: Asia/Bangkok (GMT+7)
- **Connection Test Task**: Validates all external connections first
- **PostgreSQL Task**: Syncs records from `transactions_sink` to `TRANSACTIONS_STREAMING_KAFKA`
- **CSV Task**: Processes files from `data/incoming/` to `TRANSACTIONS_BATCH_CSV` using `COPY INTO`
- **Task Dependencies**: Connection test → [PostgreSQL task, CSV task] (parallel execution)

### Architecture Overview

The DAG uses a modular architecture with utility classes:

```
airflow/dags/utils/
├── connection_test.py                    # Connection validation
├── postgres_to_snowflake_processor.py    # PostgreSQL processing
└── snowflake_csv_stage_loader.py         # CSV bulk loading
```

**Benefits:**
- Reusable across multiple DAGs
- Easier testing and debugging
- Structured logging instead of print statements
- Clear separation of concerns

### Test the Setup

1. **Generate some data**:
   ```bash
   # Start the kafka producer to generate transaction data
   docker compose -f kafka-docker-compose.yml --profile app up producer
   ```

2. **Check PostgreSQL**:
   ```bash
   # Connect to postgres and check data
   docker compose -f kafka-docker-compose.yml exec kafka-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB
   # Then: SELECT COUNT(*) FROM transactions_sink;
   ```

3. **Monitor DAG execution**:
   - Check Airflow logs for successful execution
   - Verify data appears in Snowflake tables
   - Check connection test task logs for validation

### Data Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │     Airflow      │    │    Snowflake    │
│ transactions_sink│───▶│  Streaming Task  │───▶│TRANSACTIONS_    │
└─────────────────┘    └──────────────────┘    │STREAMING_KAFKA  │
                                               └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │  CSV Task        │───▶│TRANSACTIONS_    │
                       │  (COPY INTO)     │    │BATCH_CSV        │
                       └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │data/incoming/    │
                       │*.csv files       │
                       └──────────────────┘
```

### Snowflake Tables

#### TRANSACTIONS_STREAMING_KAFKA
- **Purpose**: Streaming data from Kafka → PostgreSQL pipeline
- **Key Features**: Includes `AIRFLOW_INGESTED_AT` timestamp
- **Data Source**: PostgreSQL `transactions_sink` table

#### TRANSACTIONS_BATCH_CSV
- **Purpose**: Batch data from CSV files
- **Key Features**: Uses `COPY INTO` with permanent stage for efficiency
- **Data Source**: CSV files in `data/incoming/` folder

#### STG_TRANSACTIONS_BATCH_CSV
- **Purpose**: Permanent stage for CSV bulk loading
- **Configuration**: `TIMESTAMP_FORMAT = 'AUTO'` for automatic timestamp detection
- **Benefits**: Reused across batch runs, no need to recreate

### Troubleshooting

- **Connection errors**: Check connection IDs in Airflow UI
- **Missing tables**: Execute Snowflake bootstrap script
- **Permission issues**: Verify Snowflake role permissions
- **Data not syncing**: Check connection test task logs
- **CSV task hanging**: Verify Snowflake JWT authentication
- **Timezone issues**: Ensure Airflow is configured for Asia/Bangkok

### Files Created

- `airflow/dags/batch_data_ingestion_dag.py` - Main DAG file
- `airflow/dags/utils/` - Modular utility classes
- `scripts/snowflake_bootstrap_batch_ingestion.sql` - Snowflake table setup
- `scripts/setup_airflow_postgres_connection.sh` - PostgreSQL connection setup
- `README.md` - Updated with comprehensive project documentation

## 🎯 Next Steps

1. Monitor the DAG execution in Airflow UI
2. Verify data is flowing to Snowflake
3. Customize the schedule if needed (currently 15 minutes)
4. Add data quality checks or transformations as needed
5. Extend utility classes for additional data sources

## 🔧 Development Notes

### Utility Class Usage

```python
from airflow.dags.utils import (
    ConnectionTest,
    PostgresToSnowflakeProcessor,
    SnowflakeCSVStageLoader
)

# In your DAG tasks:
@task()
def test_connections():
    tester = ConnectionTest()
    return tester.test_all_connections()

@task()
def process_postgres():
    processor = PostgresToSnowflakeProcessor()
    return processor.process_data()

@task()
def process_csv():
    loader = SnowflakeCSVStageLoader()
    return loader.process_files()
```

### Adding New Data Sources

1. Create a new utility class in `airflow/dags/utils/`
2. Follow the existing pattern with proper logging
3. Add the class to `airflow/dags/utils/__init__.py`
4. Create corresponding Snowflake tables
5. Add the task to the DAG
