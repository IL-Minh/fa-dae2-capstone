# Docker Network Architecture

This document explains the network architecture for the data pipeline, including how Airflow connects to the Kafka PostgreSQL instance.

## Network Overview

The system uses **two Docker networks** with **cross-network communication** for optimal security and performance:

### **Network 1: Kafka Network (`kafka_network`)**
- **Purpose**: Kafka ecosystem services
- **Services**:
  - `kafka-postgres` - PostgreSQL for transaction data
  - `kafka` - Kafka broker
  - `kafdrop` - Kafka monitoring UI
  - `producer` - Kafka producer (app profile)
  - `consumer` - Kafka consumer (app profile)

### **Network 2: Airflow Network (`airflow_network`)**
- **Purpose**: Airflow orchestration services
- **Services**:
  - `airflow-postgres` - PostgreSQL for Airflow metadata
  - `redis` - Redis for Celery
  - `airflow-scheduler` - Airflow scheduler
  - `airflow-worker` - Airflow worker
  - `airflow-apiserver` - Airflow API server
  - `airflow-triggerer` - Airflow triggerer
  - `airflow-dag-processor` - Airflow DAG processor

## Cross-Network Communication

### **Airflow → Kafka PostgreSQL Connection**

Airflow services are configured with **dual network access**:
- **Primary network**: `airflow_network` (for Airflow services)
- **Secondary network**: `kafka_network` (for data access)

```yaml
# In airflow-docker-compose.yml
networks:
  - default          # airflow_network
  - kafka_network    # External network for data access
```

### **Connection Details**

When Airflow DAGs need to connect to Kafka PostgreSQL:

```python
# In batch_data_ingestion_dag.py
pg_hook = PostgresHook(
    postgres_conn_id="postgres_kafka_default",
    schema="DB_T0",
)
```

**Connection Configuration**:
- **Host**: `kafka-postgres` (service name in kafka_network)
- **Port**: `5432` (internal PostgreSQL port)
- **Database**: `${POSTGRES_DB}` (from .env)
- **User**: `${POSTGRES_USER}` (from .env)

## Security Benefits

### **1. Network Isolation**
- Airflow metadata PostgreSQL is **internal only** (no host exposure)
- Kafka PostgreSQL is **exposed to host** for development/testing
- Services can only communicate through defined network connections

### **2. Service Discovery**
- Container-to-container communication using service names
- No need for IP addresses or host networking
- Automatic DNS resolution within Docker networks

### **3. Access Control**
- Only Airflow services that need data access are on both networks
- Kafka services are isolated from Airflow metadata
- Clear separation of concerns

## Setup Commands

### **1. Start Kafka Infrastructure**
```bash
docker compose -f kafka-docker-compose.yml up -d
```

### **2. Start Airflow Infrastructure**
```bash
docker compose -f airflow-docker-compose.yml up -d
```

### **3. Setup Airflow Connection**
```bash
# Run after both systems are up
./scripts/setup_airflow_postgres_connection.sh
```

## Network Verification

### **Check Network Connectivity**
```bash
# Verify networks exist
docker network ls | grep -E "(kafka|airflow)"

# Check which containers are on which networks
docker network inspect fa-dae2-capstone_kafka_network
docker network inspect airflow_network
```

### **Test Connection from Airflow**
```bash
# Test PostgreSQL connection from Airflow container
docker compose -f airflow-docker-compose.yml exec airflow-scheduler \
  airflow connections test postgres_kafka_default
```

## Troubleshooting

### **Common Issues**

1. **Connection Refused**
   - Ensure both networks are running
   - Verify service names are correct
   - Check if containers are healthy

2. **Network Not Found**
   - Start Kafka infrastructure first
   - Ensure external network is properly referenced

3. **DNS Resolution Issues**
   - Use service names, not IP addresses
   - Verify containers are on the same network

### **Debug Commands**
```bash
# Check container networks
docker inspect <container_name> | grep -A 10 "Networks"

# Test network connectivity
docker compose -f airflow-docker-compose.yml exec airflow-scheduler ping kafka-postgres

# Check PostgreSQL logs
docker compose -f kafka-docker-compose.yml logs kafka-postgres
```

## Best Practices

### **1. Service Naming**
- Use descriptive service names (`kafka-postgres`, `airflow-postgres`)
- Avoid generic names that could cause conflicts

### **2. Network Design**
- Separate concerns with different networks
- Use external networks for cross-service communication
- Minimize network exposure

### **3. Security**
- Don't expose internal services to host unless necessary
- Use service names for internal communication
- Keep sensitive databases internal

### **4. Monitoring**
- Use Kafdrop for Kafka monitoring
- Use Airflow UI for DAG monitoring
- Monitor network connectivity in logs
