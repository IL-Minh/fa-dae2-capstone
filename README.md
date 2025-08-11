# PostgreSQL Docker Setup

A simple PostgreSQL database setup using Docker for learning and development.

## Quick Start

### 1. Start PostgreSQL

```bash
# Start the database
docker-compose up -d

# Check if the service is running
docker-compose ps
```

### 2. Access Your Database

**PostgreSQL Database:**
- Host: `localhost`
- Port: `5432`
- Database: `postgres` (or your custom database name)
- Username: `postgres` (or your custom username)
- Password: `your_password` (from your .env file)

### 3. Connect with DBeaver

1. Download and install DBeaver Community from https://dbeaver.io/download/
2. Open DBeaver and click "New Database Connection"
3. Select PostgreSQL from the database list
4. Fill in connection details:
   - Host: `localhost`
   - Port: `5432`
   - Database: `postgres` (or your custom database name)
   - Username: `postgres` (or your custom username)
   - Password: `your_password` (from your .env file)
5. Test the connection and click "Finish"

## Useful Commands

```bash
# Start database
docker-compose up -d

# Stop database
docker-compose down

# View logs
docker-compose logs postgres

# Access PostgreSQL directly via command line
docker exec -it postgres_db psql -U postgres -d postgres

# Remove everything (including data)
docker-compose down -v
```

## Environment Configuration

This project uses environment variables for configuration. Create a `.env` file in the root directory with your specific settings:

```bash
# Example .env file
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=postgres
POSTGRES_PORT=5432
```

The `.env` file is already in `.gitignore` to keep your credentials secure.

## Learning PostgreSQL

### Basic SQL Commands to Try

```sql
-- List all databases
\l

-- Connect to a database
\c database_name

-- List all tables
\dt

-- Describe a table
\d table_name

-- Basic SELECT query
SELECT * FROM table_name LIMIT 10;

-- Create a simple table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert data
INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');

-- Query data
SELECT * FROM users;
```

## Troubleshooting

- If port 5432 is already in use, change the port mapping in `docker-compose.yml`
- If you need to reset the database, run `docker-compose down -v` and then `docker-compose up -d`
- Check container logs with `docker-compose logs postgres`
