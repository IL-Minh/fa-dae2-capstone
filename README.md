# FA-DAE2-Capstone: Data Engineering Pipeline

A comprehensive data engineering project that demonstrates a complete data pipeline from Kafka streaming to batch processing with Airflow orchestration and Snowflake data warehousing.

## 🏗️ Project Architecture

This project implements a modern data engineering architecture with:

- **Kafka Ecosystem**: Real-time streaming with producer/consumer patterns
- **PostgreSQL**: Transactional data storage and streaming sink
- **Airflow**: Workflow orchestration and scheduling
- **Snowflake**: Cloud data warehouse for analytics
- **Docker Compose**: Containerized development environment

## 📁 Project Structure

```
fa-dae2-capstone/
├── airflow/                          # Airflow orchestration
│   ├── dags/
│   │   ├── batch_data_ingestion_dag.py  # Main DAG for data ingestion
│   │   └── utils/                    # Modular utility classes
│   │       ├── connection_test.py    # Connection validation utilities
│   │       ├── postgres_to_snowflake_processor.py  # PostgreSQL processing
│   │       └── snowflake_csv_stage_loader.py       # CSV bulk loading
│   └── config/                       # Airflow configuration
├── kafka/                            # Kafka ecosystem
│   ├── kafka_producer_faker.py       # Fake transaction producer
│   ├── kafka_to_postgres.py          # Kafka consumer to PostgreSQL
│   ├── utils/                        # Kafka utilities
│   │   ├── faker_generator.py        # Fake data generation
│   │   ├── postgres_client.py        # PostgreSQL operations
│   │   └── avro_utils.py             # Avro serialization (placeholder)
│   └── README.md                     # Kafka setup documentation
├── scripts/                          # Setup and utility scripts
│   ├── setup_airflow_postgres_connection.sh    # PostgreSQL connection setup
│   ├── setup_airflow_snowflake_connection.sh   # Snowflake connection setup
│   ├── sf_conn.py                    # Snowflake connection utility
│   └── snowflake_bootstrap_batch_ingestion.sql # Snowflake schema setup
├── data/                             # Data files
│   └── incoming/                     # CSV files for batch processing
├── infra/                            # Docker infrastructure
│   ├── Dockerfile.airflow            # Airflow container
│   ├── Dockerfile.consumer           # Kafka consumer container
│   └── Dockerfile.producer           # Kafka producer container
├── airflow-docker-compose.yml        # Airflow services
├── kafka-docker-compose.yml          # Kafka ecosystem services
└── requirements.txt                  # Python dependencies
```

## 🚀 Quick Start

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd fa-dae2-capstone

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials

# Set Airflow UID (required for Docker)
export AIRFLOW_UID=$(id -u)
```

### 2. Start Infrastructure Services

```bash
# Start Kafka ecosystem (PostgreSQL, Kafka, Kafdrop)
docker compose -f kafka-docker-compose.yml up -d

# Start Airflow ecosystem
docker compose -f airflow-docker-compose.yml up -d
```

### 3. Set Up Airflow Connections

```bash
# Set up PostgreSQL connection (for Kafka PostgreSQL)
./scripts/setup_airflow_postgres_connection.sh

# Set up Snowflake connection
./scripts/setup_airflow_snowflake_connection.sh
```

### 4. Bootstrap Snowflake

Execute the SQL script in Snowflake:
```sql
-- Run scripts/snowflake_bootstrap_batch_ingestion.sql
-- This creates tables and stages for data ingestion
```

## 🔄 Data Flow

### Streaming Pipeline (Kafka → PostgreSQL)
```
Fake Transactions → Kafka Producer → Kafka Topic → Consumer → PostgreSQL
```

### Batch Pipeline (Airflow)
```
CSV Files → Airflow DAG → Snowflake (COPY INTO)
PostgreSQL → Airflow DAG → Snowflake (Streaming)
```

## 📊 Airflow DAG: Batch Data Ingestion

The main DAG (`batch_data_ingestion_dag`) orchestrates two independent data ingestion tasks:

### Tasks

1. **Connection Test** (`test_connections`)
   - Validates PostgreSQL, Snowflake, and CSV file availability
   - Ensures prerequisites are met before main tasks run

2. **PostgreSQL to Snowflake** (`postgres_to_snowflake`)
   - Extracts data from Kafka PostgreSQL `transactions_sink` table
   - Loads into Snowflake `TRANSACTIONS_STREAMING_KAFKA` table
   - Implements simple inserts (no CDC complexity)

3. **CSV to Snowflake Batch** (`csv_to_snowflake_batch`)
   - Processes CSV files from `data/incoming/` folder
   - Uses Snowflake staging and `COPY INTO` for efficient bulk loading
   - Loads into Snowflake `TRANSACTIONS_BATCH_CSV` table

### DAG Configuration
- **Schedule**: Every 15 minutes
- **Timezone**: Asia/Bangkok (GMT+7)
- **Dependencies**: Connection test → [PostgreSQL task, CSV task]

## 🏗️ Modular Architecture

### Utility Classes

The DAG uses modular utility classes for maintainability:

- **`ConnectionTest`**: Validates all external connections
- **`PostgresToSnowflakeProcessor`**: Handles PostgreSQL data extraction and Snowflake loading
- **`SnowflakeCSVStageLoader`**: Manages CSV processing and bulk loading via Snowflake stages

### Benefits
- **Reusability**: Utility classes can be used across multiple DAGs
- **Testability**: Individual components can be tested in isolation
- **Maintainability**: Clear separation of concerns
- **Logging**: Structured logging instead of print statements

## 🐳 Docker Compose Architecture

### Kafka Ecosystem (`kafka-docker-compose.yml`)
- **Core Services** (always running):
  - `kafka-postgres`: PostgreSQL for transaction sink
  - `kafka`: Apache Kafka broker
  - `kafdrop`: Web UI for Kafka monitoring
- **Application Services** (profile: "app"):
  - `producer`: Fake transaction generator
  - `consumer`: Kafka to PostgreSQL consumer

### Airflow Ecosystem (`airflow-docker-compose.yml`)
- **PostgreSQL**: Airflow metadata database
- **Airflow Services**: Scheduler, worker, webserver, triggerer
- **Cross-Network**: Connects to Kafka PostgreSQL for data access

### Service Management

```bash
# Start only infrastructure (no producer/consumer)
docker compose -f kafka-docker-compose.yml up -d

# Start application services
docker compose -f kafka-docker-compose.yml --profile app up -d

# Start Airflow
docker compose -f airflow-docker-compose.yml up -d

# Stop specific services
docker compose -f kafka-docker-compose.yml --profile app down
docker compose -f airflow-docker-compose.yml down
```

## 🔧 Development Workflow

### Local Testing

```bash
# Test Kafka producer locally
uv run kafka/kafka_producer_faker.py

# Test Kafka consumer locally
uv run kafka/kafka_to_postgres.py

# Test CSV processing logic
uv run scripts/generate_monthly_transaction_csv.py
```

### Dependency Management

This project uses `uv` for dependency management:

```bash
# Export dependencies for Docker
uv export --format requirements-txt --no-dev > requirements.txt

# Install dependencies locally
uv sync
```

## 📋 Snowflake Schema

### Tables

1. **`TRANSACTIONS_STREAMING_KAFKA`**
   - Streaming data from Kafka → PostgreSQL pipeline
   - Includes `AIRFLOW_INGESTED_AT` timestamp

2. **`TRANSACTIONS_BATCH_CSV`**
   - Batch data from CSV files
   - Includes `SOURCE_FILE` and `INGESTED_AT` tracking

### Stage
- **`STG_TRANSACTIONS_BATCH_CSV`**: Permanent stage for CSV bulk loading
- Configured with `TIMESTAMP_FORMAT = 'AUTO'` for automatic timestamp detection

## 🔍 Monitoring and Debugging

### Airflow UI
- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

### Kafdrop
- **URL**: http://localhost:9000
- **Purpose**: Monitor Kafka topics, messages, and consumer groups

### Logs
- **Airflow**: Check task logs in Airflow UI
- **Docker**: `docker compose logs <service-name>`
- **Local**: Scripts output structured logging

## 🚨 Troubleshooting

### Common Issues

1. **Docker Compose Profiles**
   - Producer/consumer services use `profiles: ["app"]`
   - Use `--profile app` to manage these services

2. **Network Issues**
   - Airflow connects to Kafka PostgreSQL via external network
   - Ensure `kafka_network` is properly configured

3. **Timezone Issues**
   - Airflow configured for Asia/Bangkok (GMT+7)
   - DAGs use local timezone for scheduling

4. **Connection Issues**
   - Verify Airflow connections in UI
   - Check environment variables in `.env` file

### Useful Commands

```bash
# Clean up Docker resources
docker compose down -v
docker network prune
docker system prune

# Check service status
docker compose ps
docker compose logs <service>

# Rebuild specific service
docker compose build <service>
docker compose up -d <service>
```

## 📚 Documentation

- **`kafka/README.md`**: Detailed Kafka setup and usage
- **`NETWORK_ARCHITECTURE.md`**: Docker networking explanation
- **`SETUP_BATCH_DAG.md`**: Airflow DAG setup guide
- **`capstone_design.md`**: Project design documentation
- **`capstone_implementation_plan.md`**: Implementation roadmap

## 🤝 Contributing

1. Follow the modular architecture pattern
2. Use structured logging instead of print statements
3. Place utility classes in appropriate `utils/` directories
4. Update documentation for any architectural changes
5. Test locally before committing

## 📄 License

This project is part of the Data AI Engineering capstone project.
