-- Bootstrap Snowflake objects for batch CSV ingestion
-- Instructions:
-- 1) Replace the placeholders in ALL_CAPS with your values or run in a worksheet after setting:
--      USE ROLE YOUR_ROLE;
--      USE WAREHOUSE YOUR_WAREHOUSE;
-- 2) Execute this script once to create database/schema/table/stage.
-- 3) After this, run the loader:
--      uv run loaders/csv_to_snowflake.py data/incoming/transactions_YYYY_MM.csv

-- Set your context (optional if already selected in UI)
-- USE ROLE YOUR_ROLE;
-- USE WAREHOUSE YOUR_WAREHOUSE;

-- Replace these names or keep as-is
-- CREATE DATABASE IF NOT EXISTS DB_T0;
USE DATABASE DB_T0;

CREATE SCHEMA IF NOT EXISTS SC_RAW;
USE SCHEMA SC_RAW;

-- Batch landing table (matches realtime schema)
CREATE TABLE IF NOT EXISTS TRANSACTIONS_RAW (
    TX_ID STRING PRIMARY KEY,
    USER_ID INTEGER,
    AMOUNT NUMBER(18,2),
    CURRENCY STRING,
    MERCHANT STRING,
    CATEGORY STRING,
    TIMESTAMP TIMESTAMP_NTZ,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Internal stage for CSV uploads (dev)
CREATE STAGE IF NOT EXISTS STG_TRANSACTIONS
  FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
  );

-- Optional: grants (adjust ROLE/USER/SHARE as needed)
-- GRANT USAGE ON DATABASE DB_T0 TO ROLE YOUR_ROLE;
-- GRANT USAGE ON SCHEMA DB_T0.SC_RAW TO ROLE YOUR_ROLE;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE TRANSACTIONS_RAW TO ROLE YOUR_ROLE;
-- GRANT USAGE ON STAGE STG_TRANSACTIONS TO ROLE YOUR_ROLE;


