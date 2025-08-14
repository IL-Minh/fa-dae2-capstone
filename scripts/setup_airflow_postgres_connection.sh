#!/bin/bash

# Script to set up PostgreSQL connection for the batch data ingestion DAG
# This connects to the kafka postgres instance (not the Airflow metadata postgres)
# Run this script from the project root directory

set -e

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Error: .env file not found in current directory"
    exit 1
fi

# Source the .env file
source .env

# Check if Airflow is running
if ! docker compose -f airflow-docker-compose.yml ps | grep -q "airflow-scheduler"; then
    echo "Error: Airflow is not running. Start it first: docker compose -f airflow-docker-compose.yml up -d"
    exit 1
fi

echo "Setting up PostgreSQL connection: postgres_kafka_default"

# Delete existing connection if it exists
docker compose -f airflow-docker-compose.yml exec -T airflow-scheduler airflow connections delete postgres_kafka_default 2>/dev/null || true

# Add new connection to the kafka postgres instance
docker compose -f airflow-docker-compose.yml exec -T airflow-scheduler airflow connections add 'postgres_kafka_default' \
    --conn-type 'postgres' \
    --conn-login "$POSTGRES_USER" \
    --conn-password "$POSTGRES_PASSWORD" \
    --conn-host "kafka-postgres" \
    --conn-port "5432" \
    --conn-schema "$POSTGRES_DB"

echo "✅ PostgreSQL connection setup complete"
echo ""
echo "📋 Connection details:"
echo "   - Connection ID: postgres_kafka_default"
echo "   - Host: kafka-postgres (kafka docker network)"
echo "   - Database: $POSTGRES_DB"
echo "   - Schema: $POSTGRES_DB"
echo "   - User: $POSTGRES_USER"
echo ""
echo "🚀 The batch_data_ingestion_dag can now connect to PostgreSQL!"
