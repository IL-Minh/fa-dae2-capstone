# Dimensional Modeling Plan

## 🎯 **Executive Summary**

Based on our **3 data sources** and business requirements, we'll implement a **hybrid approach** that balances simplicity with dimensional modeling best practices.

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

## 🏗️ **Recommended Dimensional Model**

### **Approach: Hybrid (One Big Table + Strategic Dimensions)**

Given our **small data volume** and **simplicity needs**, we'll use:
- **One big fact table** for transactions (easier to query)
- **Strategic dimension tables** for external data (locations, user segments)
- **Extracted dimensions** only when performance becomes an issue

## 📋 **Proposed Model Structure**

### **FACT TABLES**
```
1. fct_transactions (One Big Table)
   - All transaction details
   - User profile fields (when available)
   - Location fields (when available)
   - No joins needed for basic queries

2. fct_daily_transactions (Aggregated)
   - Daily summaries by category, user tier, location
   - Pre-calculated metrics
   - Fast for reporting dashboards
```

### **DIMENSION TABLES**
```
1. dim_locations (dbt seed CSV)
   - Cities, states, countries
   - Geographic hierarchies
   - Cost of living indicators

2. dim_user_segments (derived)
   - Customer tiers, risk profiles
   - Age groups, income brackets
   - Behavioral segments

3. dim_calendar (generated)
   - Date dimensions
   - Business days, holidays
   - Fiscal periods
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
│  │ fct_transactions│  │fct_daily_trans  │  │ dim_locations   │ │
│  │ (One Big Table) │  │ (Aggregated)    │  │ (Seed CSV)      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 📝 **Detailed Model Specifications**

### **1. fct_transactions (One Big Table)**
```sql
-- All transaction details in one place
SELECT
    tx_id,
    user_id,
    amount,
    currency,
    merchant,
    category,
    timestamp,
    -- User profile fields (when available)
    user_age,
    user_income_bracket,
    user_customer_tier,
    user_risk_profile,
    -- Location fields (when available)
    user_city,
    user_state,
    user_country,
    -- Metadata
    source_system,
    ingested_at
FROM transactions_unified
```

**Benefits:**
- ✅ **No joins needed** for basic queries
- ✅ **Easy to understand** and maintain
- ✅ **Fast for small datasets** (your current volume)
- ✅ **Simple to evolve** later

### **2. fct_daily_transactions (Aggregated)**
```sql
-- Pre-calculated daily metrics
SELECT
    transaction_date,
    source_system,
    category,
    user_customer_tier,
    user_income_bracket,
    user_city,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM fct_transactions
GROUP BY 1, 2, 3, 4, 5, 6
```

**Benefits:**
- ✅ **Fast reporting** (pre-aggregated)
- ✅ **Dashboard friendly** (ready for BI tools)
- ✅ **Historical trends** (daily snapshots)

### **3. dim_locations (dbt seed CSV)**
```csv
city,state,country,region,population,cost_of_living_index
New York,NY,USA,Northeast,8336817,100
Los Angeles,CA,USA,West,3979576,95
Chicago,IL,USA,Midwest,2693976,85
```

**Benefits:**
- ✅ **External data** (not from transactions)
- ✅ **Stable reference** (rarely changes)
- ✅ **Rich context** (population, cost of living)

### **4. dim_user_segments (derived)**
```sql
-- Extract user segments from transaction patterns
SELECT
    user_id,
    CASE
        WHEN avg_amount > 1000 THEN 'high_value'
        WHEN avg_amount > 100 THEN 'medium_value'
        ELSE 'low_value'
    END as spending_segment,
    CASE
        WHEN transaction_frequency > 50 THEN 'frequent'
        WHEN transaction_frequency > 20 THEN 'regular'
        ELSE 'occasional'
    END as frequency_segment
FROM user_transaction_metrics
```

**Benefits:**
- ✅ **Behavioral insights** (not just demographics)
- ✅ **Dynamic segmentation** (based on actual usage)
- ✅ **Marketing ready** (targeted campaigns)

## 🎯 **Business Questions This Model Answers**

### **Customer Analysis**
```
1. "Which customer tiers generate the most revenue?"
2. "How do spending patterns vary by age and income?"
3. "Which cities have the highest-value customers?"
4. "What's the customer lifetime value by segment?"
```

### **Operational Insights**
```
1. "Which categories are trending this month?"
2. "How does real-time vs. batch data compare?"
3. "What's the transaction volume by time of day?"
4. "Which merchants are most popular by location?"
```

### **Risk & Compliance**
```
1. "Which users show unusual spending patterns?"
2. "How does risk profile correlate with transaction behavior?"
3. "What's the geographic distribution of high-risk transactions?"
```

## 🚀 **Implementation Phases**

### **Phase 1: Foundation (Week 1)**
- [ ] Create `dim_locations` seed CSV
- [ ] Build `fct_transactions` (one big table)
- [ ] Basic data quality tests

### **Phase 2: Aggregation (Week 2)**
- [ ] Build `fct_daily_transactions`
- [ ] Add calendar dimensions
- [ ] Performance optimization

### **Phase 3: Intelligence (Week 3)**
- [ ] Create `dim_user_segments`
- [ ] Add behavioral analytics
- [ ] Advanced reporting

### **Phase 4: Optimization (Week 4)**
- [ ] Performance monitoring
- [ ] Data quality alerts
- [ ] Documentation updates

## 💡 **Key Design Decisions**

### **1. One Big Table vs. Star Schema**
- **Decision**: One big table for now
- **Reason**: Small data volume, simplicity, no performance issues
- **Future**: Extract dimensions when scaling

### **2. Real-time vs. Batch Integration**
- **Decision**: Hybrid approach
- **Reason**: Different use cases, different requirements
- **Implementation**: Separate pipelines, unified warehouse

### **3. User ID Matching Strategy**
- **Decision**: UUID-based matching
- **Reason**: Simulates real-world integration challenges
- **Solution**: Data quality monitoring and alerts

## 🔍 **Data Quality & Governance**

### **Quality Checks**
- **Completeness**: Required fields not null
- **Accuracy**: Amount ranges, valid categories
- **Consistency**: User IDs match across sources
- **Timeliness**: Data freshness indicators

### **Governance**
- **Data lineage**: Track data from source to consumption
- **Change management**: Version control for model changes
- **Access control**: Role-based permissions
- **Audit trail**: Track data modifications

## 📊 **Success Metrics**

### **Technical Metrics**
- **Query performance**: < 5 seconds for standard reports
- **Data freshness**: Real-time within 5 minutes, batch within 24 hours
- **Uptime**: 99.9% availability
- **Data quality**: < 1% error rate

### **Business Metrics**
- **User adoption**: 80% of analysts using the model
- **Query efficiency**: 50% reduction in report generation time
- **Insight generation**: 10+ new business insights per month
- **Decision speed**: 30% faster business decisions

## 🎯 **Next Steps**

1. **Review this plan** with stakeholders
2. **Create implementation timeline**
3. **Set up development environment**
4. **Begin Phase 1 implementation**

---

## 📚 **References**

- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [Data Vault Modeling](https://www.datavaultalliance.com/)
- [Modern Data Stack](https://www.modern-data-stack.com/)
