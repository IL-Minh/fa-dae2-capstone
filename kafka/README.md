# Kafka Scripts

This folder contains the Kafka producer and consumer scripts for the data pipeline, organized with a modular architecture for better maintainability and reusability.

## Scripts

### `kafka_producer_faker.py`
Generates fake transaction data and sends it to the `transactions` Kafka topic.

**Features:**
- Generates realistic transaction data using Faker
- Configurable message interval (default: 10 seconds)
- Comprehensive logging with structured format
- Producer configuration for reliability (acks=all, retries=3)
- Graceful shutdown handling

**Usage:**
```bash
# Run locally (uses .env file automatically)
uv run kafka/kafka_producer_faker.py

# Or run in Docker (after starting infrastructure)
docker compose -f kafka-docker-compose.yml --profile app up producer
```

**Configuration:**
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (loaded from .env file)
- Message interval: 10 seconds (hardcoded)
- Categories: groceries, rent, salary, entertainment, utilities, transport, health

**Note:** All environment variables are automatically loaded from the `.env` file in the project root.

### `kafka_to_postgres.py`
Consumes messages from the `transactions` Kafka topic and stores them in PostgreSQL.

**Features:**
- Consumes from `transactions` topic
- Stores data in `transactions_sink` table
- Handles duplicate transactions gracefully
- Comprehensive error handling and logging
- Progress monitoring and statistics
- **Modular design** using `utils/postgres_client.py`

**Usage:**
```bash
# Run locally (uses .env file automatically)
uv run kafka/kafka_to_postgres.py

# Or run in Docker (after starting infrastructure)
docker compose -f kafka-docker-compose.yml --profile app up consumer
```

**Configuration:**
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (loaded from .env file)
- `POSTGRES_*`: PostgreSQL connection parameters (loaded from .env file)
- Consumer group: `transactions-consumer`
- Auto-commit: enabled with 5-second intervals

**Note:** All environment variables are automatically loaded from the `.env` file in the project root.

## Utils

### `utils/postgres_client.py`
Modular PostgreSQL client module for database operations.

**Features:**
- Connection management with automatic cleanup
- Table creation and verification
- Transaction insertion (single and batch)
- Error handling and logging
- Environment variable configuration
- Connection testing and health checks

**Usage:**
```python
from utils.postgres_client import get_postgres_client

# Get client instance
pg_client = get_postgres_client()

# Test connection
if pg_client.test_connection():
    # Create table
    pg_client.create_transactions_table()

    # Insert transaction
    success = pg_client.insert_transaction(transaction_data)

    # Get row count
    count = pg_client.get_row_count()
```

### `utils/faker_generator.py`
Modular fake data generator for testing and development.

**Features:**
- Transaction data generation with Faker
- Configurable categories and currencies
- Batch generation capabilities
- Seed setting for reproducible data
- Extensible category management

**Usage:**
```python
from utils.faker_generator import get_faker_generator

# Get generator instance
faker_gen = get_faker_generator()

# Generate single transaction
transaction = faker_gen.generate_transaction()

# Generate batch of transactions
transactions = faker_gen.generate_transactions_batch(100)

# Add custom category
faker_gen.add_category("gaming")

# Set seed for reproducible data
faker_gen.set_seed(42)
```

## Local Development Workflow

### **Environment Configuration**
- **Local scripts**: Automatically use `.env` file configuration
- **Docker containers**: Use container networking (`kafka:9092`, `postgres:5432`)
- **Host access**: Kafka available at `localhost:29092`, PostgreSQL at `localhost:5432`

### **Development Steps**

1. **Start Infrastructure Only:**
   ```bash
   docker compose -f kafka-docker-compose.yml up -d
   ```

2. **Test Producer Locally:**
   ```bash
   # Uses .env file automatically for configuration
   uv run kafka/kafka_producer_faker.py
   ```

3. **Test Consumer Locally:**
   ```bash
   # Uses .env file automatically for configuration
   uv run kafka/kafka_to_postgres.py
   ```

4. **Monitor with Kafdrop:**
   - Open http://localhost:9000
   - Watch messages flow through topics
   - Monitor consumer groups and lag

## Docker Compose Commands

The docker-compose file uses profiles to separate infrastructure from application services:

### **Service Groups:**
- **Infrastructure Services**: `postgres`, `kafka`, `kafdrop` (core services)
- **Application Services**: `producer`, `consumer` (data processing)

### **Important: Profile Management**
**Why `docker compose down` doesn't stop producer/consumer:**

The `producer` and `consumer` services are defined with `profiles: ["app"]`. This means:
- **Default behavior**: `docker compose up/down` (no profile) only manages infrastructure services
- **Profile-specific**: `docker compose --profile app up/down` manages ALL services including producer/consumer
- **This is intentional**: Allows infrastructure to run continuously while starting/stopping application services as needed

### **Starting Services:**

#### **1. Start Only Infrastructure (Default):**
```bash
docker compose -f kafka-docker-compose.yml up -d
```
This starts:
- ✅ `postgres` - PostgreSQL database
- ✅ `kafka` - Kafka broker
- ✅ `kafdrop` - Kafka monitoring UI

#### **2. Start Infrastructure + Producer + Consumer:**
```bash
docker compose -f kafka-docker-compose.yml --profile app up -d
```
This starts everything:
- ✅ `postgres` - PostgreSQL database
- ✅ `kafka` - Kafka broker
- ✅ `kafdrop` - Kafka monitoring UI
- ✅ `producer` - Kafka producer (generates fake data)
- ✅ `consumer` - Kafka consumer (stores data in PostgreSQL)

#### **3. Start Specific Services:**
```bash
# Start infrastructure first
docker compose -f kafka-docker-compose.yml up -d

# Then start producer only
docker compose -f kafka-docker-compose.yml --profile app up producer

# Or start consumer only
docker compose -f kafka-docker-compose.yml --profile app up consumer
```

#### **4. Stop Services:**
```bash
# Stop infrastructure services only (default)
docker compose -f kafka-docker-compose.yml down

# Stop ALL services including producer/consumer
docker compose -f kafka-docker-compose.yml --profile app down

# Stop with cleanup (removes volumes)
docker compose -f kafka-docker-compose.yml --profile app down -v
```

## Logging

Both scripts use structured logging with:
- Timestamp and log level
- Service identifier ([PRODUCER] or [CONSUMER])
- Detailed message information
- Progress tracking and statistics
- Error handling with stack traces

## Data Flow

```
Faker Producer → Kafka Topic → Consumer → PostgreSQL
     ↓              ↓           ↓          ↓
  Fake Data    transactions  Process   transactions_sink
```

## Troubleshooting

- **Connection Issues**: Verify Kafka and PostgreSQL are running
- **Port Conflicts**: Ensure ports 29092 (Kafka) and 5432 (PostgreSQL) are available
- **Environment Variables**: Check all required env vars are set
- **Dependencies**: Ensure `uv` and required packages are installed
