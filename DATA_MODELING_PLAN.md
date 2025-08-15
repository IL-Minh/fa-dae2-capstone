# Data Modeling Plan

## 🎯 **Executive Summary**

Based on our **3 data sources** and business requirements, we'll implement a **proper dimensional modeling approach** using star schema design for optimal query performance and business intelligence.

## 📊 **Current Data Sources Analysis**

### **Source 1: Batch Transaction CSV**
- **Volume**: 500+ transactions/month
- **Update**: Monthly batches
- **Quality**: High (financial data)
- **Use Case**: Monthly reporting, historical analysis

### **Source 2: Real-time Transaction Stream**
- **Volume**: Continuous stream
- **Update**: Real-time
- **Quality**: High (simulated)
- **Use Case**: Live monitoring, fraud detection

### **Source 3: User Profile CSV**
- **Volume**: 100+ users/month
- **Update**: Monthly batches
- **Quality**: Medium (self-reported)
- **Use Case**: Customer segmentation, personalization

## 🏗️ **Dimensional Model Architecture**

### **Approach: Star Schema with Conformed Dimensions**

We'll implement a **proper dimensional model** with:
- **Fact tables** containing business metrics and transaction details
- **Dimension tables** for descriptive attributes and hierarchies
- **Bridge tables** for many-to-many relationships
- **Conformed dimensions** across all fact tables

## 📋 **Proposed Model Structure**

### **FACT TABLES**
```
1. fct_transactions (Grain: One row per transaction)
   - Transaction metrics (amount, count)
   - Foreign keys to dimensions
   - Business keys (tx_id, user_id)

2. fct_daily_transactions (Grain: One row per day per dimension combination)
   - Daily aggregated metrics
   - Pre-calculated KPIs
   - Fast for reporting dashboards

3. fct_user_metrics (Grain: One row per user per month)
   - User behavior metrics
   - Spending patterns
   - Risk indicators
```

### **DIMENSION TABLES**
```
1. dim_users
   - User demographics and profiles
   - Customer segmentation
   - Registration information

2. dim_merchants
   - Merchant details and categories
   - Business information
   - Risk ratings

3. dim_categories
   - Transaction categories
   - Category hierarchies
   - Business rules

4. dim_currencies
   - Currency information
   - Exchange rates
   - Conversion factors

5. dim_dates
   - Date dimensions
   - Business calendars
   - Fiscal periods

6. dim_locations
   - Geographic hierarchies
   - Regional attributes
   - Cost of living indicators

7. dim_user_segments
   - Behavioral segments
   - Value tiers
   - Risk profiles
```

## 🔄 **Data Flow Architecture**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Batch CSV     │    │  Real-time      │    │  User Profile   │
│  Transactions   │    │  Transactions   │    │      CSV        │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                       │                       │
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    dbt Transformation Layer                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   Staging       │  │   Intermediate  │  │     Marts       │ │
│  │   Models        │  │     Models      │  │     Models      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
          │                       │                       │
          │                       │                       │
          ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Snowflake Data Warehouse                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ fct_transactions│  │fct_daily_trans  │  │ dim_users       │ │
│  │ (Star Schema)   │  │ (Aggregated)    │  │ (Conformed)     │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 📝 **Detailed Model Specifications**

### **1. fct_transactions (Fact Table)**
```sql
-- Transaction fact table with proper dimensional keys
SELECT
    -- Business keys
    tx_id,
    user_id,

    -- Foreign keys to dimensions
    user_key,
    merchant_key,
    category_key,
    currency_key,
    date_key,
    location_key,

    -- Fact measures
    amount,
    transaction_count,

    -- Metadata
    source_system,
    ingested_at,
    processed_at
FROM transactions_unified
```

**Benefits:**
- ✅ **Proper dimensional design** for optimal query performance
- ✅ **Conformed dimensions** across all fact tables
- ✅ **Scalable architecture** for future growth
- ✅ **Business intelligence ready** for complex analytics

### **2. dim_users (Dimension Table)**
```sql
-- User dimension with comprehensive attributes
SELECT
    user_key,
    user_id,
    first_name,
    last_name,
    email,
    age,
    age_group,
    income_bracket,
    customer_tier,
    risk_profile,
    city,
    state,
    country,
    region,
    registration_date,
    preferred_categories,
    is_active,
    source_system,
    effective_date,
    end_date,
    is_current
FROM user_profiles
```

**Benefits:**
- ✅ **Slowly changing dimensions** for historical tracking
- ✅ **Rich attributes** for segmentation and analysis
- ✅ **Conformed across** all fact tables

### **3. dim_merchants (Dimension Table)**
```sql
-- Merchant dimension with business context
SELECT
    merchant_key,
    merchant_id,
    merchant_name,
    merchant_category,
    business_type,
    risk_rating,
    location_city,
    location_state,
    location_country,
    is_active,
    effective_date,
    end_date,
    is_current
FROM merchant_master
```

**Benefits:**
- ✅ **Merchant risk analysis** for fraud detection
- ✅ **Geographic distribution** analysis
- ✅ **Business type categorization** for insights

### **4. dim_categories (Dimension Table)**
```sql
-- Category dimension with hierarchies
SELECT
    category_key,
    category_name,
    category_group,
    category_type,
    business_unit,
    is_active,
    effective_date,
    end_date,
    is_current
FROM category_master
```

**Benefits:**
- ✅ **Category hierarchies** for roll-up analysis
- ✅ **Business unit mapping** for organizational insights
- ✅ **Flexible categorization** for different business needs

### **5. dim_dates (Dimension Table)**
```sql
-- Date dimension with business calendar
SELECT
    date_key,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_year,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday,
    fiscal_year,
    fiscal_quarter,
    fiscal_month
FROM date_dimension
```

**Benefits:**
- ✅ **Business calendar** support for fiscal reporting
- ✅ **Time-based analysis** for trends and seasonality
- ✅ **Multiple calendar** systems support

## 🎯 **Business Questions This Model Answers**

### **Customer Analysis**
```
1. "Which customer segments generate the most revenue by category?"
2. "How do spending patterns vary by age, income, and location?"
3. "What's the customer lifetime value by risk profile?"
4. "Which user segments show the highest growth rates?"
```

### **Operational Insights**
```
1. "Which merchant categories are trending by region?"
2. "How does real-time vs. batch data compare by user tier?"
3. "What's the transaction volume by time and location?"
4. "Which merchants have the highest risk ratings?"
```

### **Risk & Compliance**
```
1. "Which user segments show unusual spending patterns?"
2. "How does risk profile correlate with merchant categories?"
3. "What's the geographic distribution of high-risk transactions?"
4. "Which merchant types have the highest fraud rates?"
```

## 🚀 **Implementation Phases**

### **Phase 1: Foundation (Week 1)**
- [ ] Create dimension tables (dim_users, dim_merchants, dim_categories)
- [ ] Build fct_transactions with proper dimensional keys
- [ ] Implement slowly changing dimension logic
- [ ] Basic data quality tests

### **Phase 2: Enrichment (Week 2)**
- [ ] Add dim_dates and dim_locations
- [ ] Build fct_daily_transactions
- [ ] Implement user segmentation logic
- [ ] Add data lineage tracking

### **Phase 3: Intelligence (Week 3)**
- [ ] Create fct_user_metrics
- [ ] Add behavioral analytics
- [ ] Implement advanced reporting views
- [ ] Performance optimization

### **Phase 4: Optimization (Week 4)**
- [ ] Add aggregate tables for common queries
- [ ] Implement incremental processing
- [ ] Data quality monitoring and alerts
- [ ] Documentation and training

## 💡 **Key Design Decisions**

### **1. Star Schema vs. Snowflake**
- **Decision**: Star schema for most tables, snowflake for complex hierarchies
- **Reason**: Optimal query performance, easier maintenance
- **Implementation**: Keep dimensions denormalized, use bridge tables for complex relationships

### **2. Slowly Changing Dimensions**
- **Decision**: Type 2 SCD for users and merchants
- **Reason**: Historical tracking for compliance and analysis
- **Implementation**: Effective date/end date pattern

### **3. Conformed Dimensions**
- **Decision**: Standardize dimension keys across all fact tables
- **Reason**: Consistent reporting and analysis
- **Implementation**: Shared dimension tables with consistent keys

## 🔍 **Data Quality & Governance**

### **Quality Checks**
- **Referential integrity**: All foreign keys must exist in dimension tables
- **Completeness**: Required fields not null
- **Accuracy**: Amount ranges, valid categories, valid dates
- **Consistency**: User IDs match across sources
- **Timeliness**: Data freshness indicators

### **Governance**
- **Data lineage**: Track data from source to consumption
- **Change management**: Version control for model changes
- **Access control**: Role-based permissions by business unit
- **Audit trail**: Track data modifications and access

## 📊 **Success Metrics**

### **Technical Metrics**
- **Query performance**: < 3 seconds for standard reports
- **Data freshness**: Real-time within 5 minutes, batch within 24 hours
- **Uptime**: 99.9% availability
- **Data quality**: < 0.5% error rate

### **Business Metrics**
- **User adoption**: 90% of analysts using the model
- **Query efficiency**: 70% reduction in report generation time
- **Insight generation**: 20+ new business insights per month
- **Decision speed**: 50% faster business decisions

## 🎯 **Next Steps**

1. **Review this dimensional model** with stakeholders
2. **Create implementation timeline** with technical details
3. **Set up development environment** for dbt models
4. **Begin Phase 1 implementation** with dimension tables

---

## 📚 **References**

- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [Star Schema Design](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/star-schema-olap-facts/)
- [Slowly Changing Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/slowly-changing-dimensions/)
