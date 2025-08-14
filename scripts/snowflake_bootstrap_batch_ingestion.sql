-- Bootstrap Snowflake objects for batch data ingestion DAG (Simplified)
-- Instructions:
-- 1) Replace the placeholders in ALL_CAPS with your values or run in a worksheet after setting:
--      USE ROLE YOUR_ROLE;
--      USE WAREHOUSE YOUR_WAREHOUSE;
-- 2) Execute this script once to create database/schema/tables.
-- 3) After this, the Airflow DAG should work properly.

-- Set your context (optional if already selected in UI)
-- USE ROLE YOUR_ROLE;
-- USE WAREHOUSE YOUR_WAREHOUSE;

-- Replace these names or keep as-is
-- CREATE DATABASE IF NOT EXISTS DB_T0;
USE DATABASE DB_T0;

CREATE SCHEMA IF NOT EXISTS SC_RAW;
USE SCHEMA SC_RAW;

-- 1. Table for streaming data from Kafka → PostgreSQL
CREATE TABLE IF NOT EXISTS TRANSACTIONS_STREAMING_KAFKA (
    TX_ID STRING PRIMARY KEY,
    USER_ID INTEGER,
    AMOUNT NUMBER(18,2),
    CURRENCY STRING,
    MERCHANT STRING,
    CATEGORY STRING,
    TIMESTAMP TIMESTAMP_NTZ,
    INGESTED_AT TIMESTAMP_NTZ,
    AIRFLOW_INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- 2. Table for batch CSV data (optimized for COPY INTO)
CREATE TABLE IF NOT EXISTS TRANSACTIONS_BATCH_CSV (
    TX_ID STRING PRIMARY KEY,
    USER_ID INTEGER,
    AMOUNT NUMBER(18,2),
    CURRENCY STRING,
    MERCHANT STRING,
    CATEGORY STRING,
    TIMESTAMP TIMESTAMP_NTZ,  -- Proper timestamp type for better querying
    SOURCE_FILE STRING,  -- filename that was processed
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- 3. Permanent stage for CSV uploads (reused across batch runs)
CREATE STAGE IF NOT EXISTS STG_TRANSACTIONS_BATCH_CSV
  FILE_FORMAT = (
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    TIMESTAMP_FORMAT = 'AUTO'  -- Automatically detect timestamp formats
  );

-- Optional: grants (adjust ROLE/USER/SHARE as needed)
-- GRANT USAGE ON DATABASE DB_T0 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON SCHEMA DB_T0.SC_RAW TO ROLE YOUR_ROLE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE TRANSACTIONS_POSTGRES TO ROLE YOUR_ROLE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE TRANSACTIONS_BATCH TO ROLE YOUR_ROLE;
-- GRANT USAGE ON STAGE STG_TRANSACTIONS TO ROLE YOUR_ROLE;

-- Verify tables were created
SHOW TABLES IN SCHEMA SC_RAW;
