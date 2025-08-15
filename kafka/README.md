# Kafka Pipeline

## Overview
This directory contains the Kafka producer and consumer scripts that simulate real-time transaction streaming for our data pipeline demo.

## Components

### **kafka_producer_faker.py**
- **Purpose**: Generates simulated real-time transactions and publishes them to Kafka
- **Topic**: `transactions`
- **Frequency**: Every 10 seconds
- **Data**: Simulated financial transactions with realistic patterns

### **kafka_to_postgres.py**
- **Purpose**: Consumes transactions from Kafka and stores them in PostgreSQL
- **Database**: `kafka_sink` (PostgreSQL)
- **Table**: `transactions_sink`
- **Processing**: Real-time consumption and storage

## ⚠️ **Important Design Note: Circular Dependency**

### **Current Implementation**
The Kafka producer has a **circular dependency** that's important to understand:

1. **`generate_monthly_data.py`** creates user registration CSV files
2. **`kafka_producer_faker.py`** reads those CSV files to get user IDs
3. **Kafka producer** generates streaming transactions using those same user IDs
4. **Both sources** (batch CSV + streaming Kafka) now reference the same users

### **Why This Exists**
- **Demo purposes**: Ensures referential integrity for dimensional modeling
- **Realistic simulation**: Shows how different data sources can share common entities
- **Testing**: Validates that our dimensional model can handle unified user data

### **Production Alternative**
In production, this would be handled by:
- Master Data Management (MDM) systems
- Identity resolution services
- Real-time user registration events
- Database lookups for user validation

## Configuration

### **Environment Variables**
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (loaded from .env file)
- `POSTGRES_HOST`: PostgreSQL host (default: localhost)
- `POSTGRES_PORT`: PostgreSQL port (default: 5432)
- `POSTGRES_DB`: PostgreSQL database name
- `POSTGRES_USER`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password

### **Kafka Configuration**
- **Bootstrap Servers**: localhost:9092 (internal Docker network)
- **Topic**: transactions
- **Partitions**: Default
- **Replication**: 1 (single broker for demo)

## Usage

### **Start the Pipeline**
```bash
# 1. Start Kafka and PostgreSQL (from root directory)
docker compose -f kafka-docker-compose.yml up -d

# 2. Start the producer (generates transactions)
uv run kafka/kafka_producer_faker.py

# 3. Start the consumer (stores in PostgreSQL)
uv run kafka/kafka_to_postgres.py
```

### **Monitor the Pipeline**
- **Kafka Topics**: Use Kafdrop at http://localhost:9000
- **PostgreSQL**: Connect to localhost:5432, database `kafka_sink`
- **Logs**: Check console output for both producer and consumer

## Data Flow

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Faker Data     │    │     Kafka       │    │   PostgreSQL    │
│   Generator     │───▶│    Producer     │───▶│   Consumer      │
│                 │    │                 │    │                 │
│ Generates       │    │ Publishes to    │    │ Stores in       │
│ transactions    │    │ topic:transactions│   │ transactions_sink│
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Data Schema

### **Transaction Message (JSON)**
```json
{
  "tx_id": "uuid",
  "user_id": "uuid",
  "amount": 123.45,
  "currency": "USD",
  "merchant": "Amazon",
  "category": "online_shopping",
  "timestamp": "2025-08-15T10:30:00Z",
  "source_system": "streaming_kafka",
  "transaction_type": "real_time",
  "processing_time_ms": 150
}
```

### **PostgreSQL Table Schema**
```sql
CREATE TABLE transactions_sink (
    tx_id VARCHAR PRIMARY KEY,
    user_id VARCHAR NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Troubleshooting

### **Common Issues**
1. **Connection refused**: Ensure Kafka and PostgreSQL are running
2. **Topic not found**: Check if Kafka is healthy and topics are created
3. **Authentication failed**: Verify PostgreSQL credentials in .env file

### **Debug Commands**
```bash
# Check Kafka health
docker compose -f kafka-docker-compose.yml ps

# View Kafka logs
docker compose -f kafka-docker-compose.yml logs kafka

# Check PostgreSQL connection
docker compose -f kafka-docker-compose.yml logs kafka-postgres
```

## Performance

### **Current Settings**
- **Producer**: 1 message every 10 seconds
- **Consumer**: Real-time processing
- **Batch Size**: 1 message per batch
- **Retries**: 3 attempts for failed messages

### **Scaling Considerations**
- Increase producer frequency for higher throughput
- Implement batch processing in consumer
- Add multiple consumer instances
- Use partitioning for parallel processing
