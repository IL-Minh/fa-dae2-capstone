# Batch Data Ingestion DAG - Quick Setup

## 🚀 Quick Start Checklist

### Prerequisites
- [ ] Airflow is running (`docker compose -f airflow-docker-compose.yml up -d`)
- [ ] Kafka stack is running (`docker compose -f kafka-docker-compose.yml up -d`)
- [ ] Snowflake connection is configured
- [ ] Environment variables are set in `.env`

### Setup Steps

#### 1. Create Snowflake Tables
```bash
# Copy and execute the SQL from snowflake/bootstrap_batch_ingestion.sql
# in your Snowflake worksheet
```

#### 2. Set Up PostgreSQL Connection
```bash
# From project root
./scripts/setup_postgres_connection.sh
```

#### 3. Verify Snowflake Connection
```bash
# From project root
./scripts/setup_airflow_connection.sh
```

#### 4. Check DAG Status
- Open Airflow UI: http://localhost:8080
- Look for `batch_data_ingestion_dag`
- Ensure it's not paused
- Check for any connection errors

### Expected Behavior

- **DAG Schedule**: Runs every 15 minutes
- **PostgreSQL Task**: Syncs new records from `transactions_sink` to `TRANSACTIONS_CDC`
- **CSV Task**: Processes files from `data/incoming/` to `TRANSACTIONS_BATCH`
- **CDC Tracking**: Only syncs new records since last successful sync
- **Independent Tasks**: Both tasks run in parallel, no dependencies

### Test the Setup

1. **Generate some data**:
   ```bash
   # Start the kafka producer to generate transaction data
   docker compose -f kafka-docker-compose.yml up producer
   ```

2. **Check PostgreSQL**:
   ```bash
   # Connect to postgres and check data
   docker compose -f kafka-docker-compose.yml exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB
   # Then: SELECT COUNT(*) FROM transactions_sink;
   ```

3. **Monitor DAG execution**:
   - Check Airflow logs for successful execution
   - Verify data appears in Snowflake tables

### Troubleshooting

- **Connection errors**: Check connection IDs in Airflow UI
- **Missing tables**: Execute Snowflake bootstrap script
- **Permission issues**: Verify Snowflake role permissions
- **Data not syncing**: Check CDC sync log table for last sync timestamp

### Files Created

- `airflow/dags/batch_data_ingestion_dag.py` - Main DAG file
- `snowflake/bootstrap_batch_ingestion.sql` - Snowflake table setup
- `scripts/setup_postgres_connection.sh` - PostgreSQL connection setup
- `README.md` - Updated with DAG documentation

## 🎯 Next Steps

1. Monitor the DAG execution in Airflow UI
2. Verify data is flowing to Snowflake
3. Customize the schedule if needed (currently 15 minutes)
4. Add data quality checks or transformations as needed
