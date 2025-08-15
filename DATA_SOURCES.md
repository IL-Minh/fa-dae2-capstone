# Data Sources Documentation

## Overview
This document describes the data sources used in our data engineering pipeline, including their origin, format, update frequency, and data quality characteristics.

## Data Source 1: Batch Transaction Data (CSV)

### **Source Description**
- **Origin**: Credit card statements and bank transaction exports
- **Format**: Monthly CSV files
- **Location**: `data/incoming/transactions_YYYY_MM.csv`
- **Update Frequency**: Monthly (end of month)
- **Data Freshness**: 1-30 days old

### **Data Characteristics**
- **Volume**: 500+ transactions per month
- **Granularity**: Individual transaction level
- **Time Range**: Monthly batches
- **Quality**: High (structured financial data)

### **Fields**
```csv
tx_id,user_id,amount,currency,merchant,category,timestamp
```

### **Business Context**
- **Purpose**: Financial analysis and reporting
- **Use Case**: Monthly spending analysis, budget tracking
- **Limitation**: Not real-time, may miss recent transactions

---

## Data Source 2: Real-time Transaction Data (Kafka Stream)

### **Source Description**
- **Origin**: Simulated real-time transaction system via Kafka producer
- **Format**: JSON messages in Kafka topics
- **Location**: Kafka → PostgreSQL → Snowflake
- **Update Frequency**: Real-time (continuous)
- **Data Freshness**: Near real-time (seconds to minutes)

### **Data Characteristics**
- **Volume**: Continuous stream of transactions
- **Granularity**: Individual transaction level
- **Time Range**: Real-time
- **Quality**: High (simulated but realistic)

### **Fields**
```json
{
  "tx_id": "uuid",
  "user_id": "uuid",
  "amount": "decimal",
  "currency": "string",
  "merchant": "string",
  "category": "string",
  "timestamp": "iso_timestamp"
}
```

### **Business Context**
- **Purpose**: Real-time transaction monitoring
- **Use Case**: Fraud detection, live dashboards, instant notifications
- **Advantage**: Immediate visibility into financial activity

---

## Data Source 3: User Profile Data (CSV)

### **Source Description**
- **Origin**: Third-party user registration system
- **Format**: Monthly CSV files
- **Location**: `data/incoming/user_registrations_YYYY_MM.csv`
- **Update Frequency**: Monthly (end of month)
- **Data Freshness**: 1-30 days old

### **Data Characteristics**
- **Volume**: 100+ new users per month
- **Granularity**: Individual user level
- **Time Range**: Monthly batches
- **Quality**: High (structured demographic data)

### **Fields**
```csv
user_id,first_name,last_name,email,age,income_bracket,customer_tier,risk_profile,city,state,country,registration_date,preferred_categories,is_active,source_system
```

### **Business Context**
- **Purpose**: Customer segmentation and personalization
- **Use Case**: Marketing campaigns, risk assessment, customer service
- **Limitation**: Not connected to transaction system (separate user IDs)

---

## Data Integration Challenges

### **1. User ID Mismatch**
- **Problem**: Transaction system uses different user IDs than registration system
- **Impact**: Cannot directly join user profiles with transactions
- **Solution**: Need data matching/cleansing strategy

### **2. Update Frequency Mismatch**
- **Problem**: Real-time transactions vs. monthly user updates
- **Impact**: User profile may be outdated when analyzing recent transactions
- **Solution**: Implement data freshness indicators and update schedules

### **3. Data Quality Variations**
- **Problem**: Different data quality standards across sources
- **Impact**: Inconsistent data for analysis
- **Solution**: Implement data validation and quality checks

---

## Data Flow Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Batch CSV     │    │  Real-time      │    │  User Profile   │
│  Transactions   │    │  Transactions   │    │      CSV        │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                       │                       │
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow       │    │   Kafka →       │    │   Airflow       │
│  Batch Load     │    │  PostgreSQL     │    │  Batch Load     │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                       │                       │
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Snowflake Data Warehouse                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ TRANSACTIONS_   │  │ TRANSACTIONS_   │  │ USER_PROFILES   │ │
│  │ BATCH_CSV       │  │ STREAMING_KAFKA │  │                 │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Quality Metrics

### **Completeness**
- **Batch Transactions**: 95%+ (missing data rare in financial records)
- **Real-time Transactions**: 99%+ (immediate validation)
- **User Profiles**: 90%+ (some optional fields may be empty)

### **Accuracy**
- **Batch Transactions**: 98%+ (financial data highly regulated)
- **Real-time Transactions**: 95%+ (simulated but realistic)
- **User Profiles**: 85%+ (self-reported data, may have errors)

### **Timeliness**
- **Batch Transactions**: 1-30 days
- **Real-time Transactions**: Seconds to minutes
- **User Profiles**: 1-30 days

---

## Recommendations

### **Immediate Actions**
1. **Implement data matching** between transaction and user systems
2. **Add data quality monitoring** for all sources
3. **Create data lineage documentation** for compliance

### **Future Improvements**
1. **Real-time user profile updates** via API integration
2. **Automated data quality alerts** for anomalies
3. **Data catalog implementation** for better governance

---

## Usage Examples

### **Generate Sample Data**
```bash
# Generate both transaction and user data for current month
python scripts/generate_monthly_transaction_csv.py

# Generate data for specific month
python scripts/generate_monthly_transaction_csv.py --year 2024 --month 8

# Customize volumes
python scripts/generate_monthly_transaction_csv.py --transactions 1000 --users 200
```

### **Data Analysis Queries**
```sql
-- Join transactions with user profiles (when IDs match)
SELECT
    t.amount,
    t.category,
    u.customer_tier,
    u.income_bracket
FROM transactions_batch_csv t
JOIN user_profiles u ON t.user_id = u.user_id;

-- Analyze spending by customer tier
SELECT
    u.customer_tier,
    AVG(t.amount) as avg_transaction,
    COUNT(*) as transaction_count
FROM transactions_streaming_kafka t
JOIN user_profiles u ON t.user_id = u.user_id
GROUP BY u.customer_tier;
```
