# dbt Data Warehouse

This dbt project implements a **proper dimensional modeling approach** using star schema design for optimal query performance and business intelligence.

## 🏗️ **Model Architecture**

### **Staging Layer**
- **`stg_transactions_batch_csv`**: Batch CSV transaction data with deduplication
- **`stg_transactions_streaming_kafka`**: Real-time streaming transaction data
- **`stg_transactions_unified`**: Unified view combining both transaction sources
- **`stg_users`**: User profile data from CSV files

### **Intermediate Layer**
- **`int_transactions_deduplicated`**: Deduplicated transactions with business logic
- **`int_transactions_metrics`**: Pre-aggregated daily transaction metrics

### **Marts Layer (Dimensional Model)**

#### **Fact Tables**
- **`fct_transactions`**: Transaction-level facts with dimensional keys
- **`fct_daily_transactions`**: Daily aggregated transaction summaries
- **`fct_user_metrics`**: User behavioral metrics and spending patterns

#### **Dimension Tables**
- **`dim_users`**: User demographics, location, and business attributes
- **`dim_merchants`**: Merchant business context and risk ratings
- **`dim_categories`**: Transaction categories with hierarchies
- **`dim_currencies`**: Currency information and exchange rates
- **`dim_dates`**: Date dimensions with business calendar support

## 🔗 **Dimensional Model Design**

### **Star Schema Structure**
```
                    fct_transactions
                         |
        ┌────────────────┼────────────────┐
        |                |                |
    dim_users      dim_merchants    dim_categories
        |                |                |
    dim_dates      dim_currencies   dim_locations
```

### **Key Benefits**
- ✅ **Proper dimensional design** for optimal query performance
- ✅ **Conformed dimensions** across all fact tables
- ✅ **Scalable architecture** for future growth
- ✅ **Business intelligence ready** for complex analytics

## 📊 **Data Sources**

### **Raw Tables**
- **`TRANSACTIONS_STREAMING_KAFKA`**: Real-time transaction stream
- **`TRANSACTIONS_BATCH_CSV`**: Monthly batch transaction files
- **`USER_REGISTRATIONS`**: User profile data from third-party system

## 🚀 **Getting Started**

### **Prerequisites**
- Snowflake connection configured
- Environment variables set for Snowflake credentials
- dbt installed and configured

### **Install Dependencies**
```bash
dbt deps
```

### **Run Models**
```bash
# Run all models
dbt build

# Run specific model
dbt run --select dim_users

# Run tests
dbt test
```

### **Documentation**
```bash
dbt docs generate
dbt docs serve
```

## 🧪 **Testing Strategy**

### **Test Types**
- **`not_null`**: Required fields must have values
- **`unique`**: Primary keys must be unique
- **Referential integrity**: Foreign keys must exist in dimension tables

### **Data Quality**
- Source data validation
- Business rule enforcement
- Data lineage tracking

## 📈 **Business Questions Answered**

### **Customer Analysis**
- Which customer segments generate the most revenue by category?
- How do spending patterns vary by age, income, and location?
- What's the customer lifetime value by risk profile?

### **Operational Insights**
- Which merchant categories are trending by region?
- How does real-time vs. batch data compare by user tier?
- What's the transaction volume by time and location?

### **Risk & Compliance**
- Which user segments show unusual spending patterns?
- How does risk profile correlate with merchant categories?
- What's the geographic distribution of high-risk transactions?

## 🔄 **Data Flow**

1. **Data Ingestion**: Airflow loads data into Snowflake raw tables
2. **Staging**: dbt transforms raw data into clean, consistent formats
3. **Intermediate**: Business logic applied, deduplication, metrics calculation
4. **Marts**: Dimensional model with fact and dimension tables
5. **Consumption**: BI tools and analytics consume the dimensional model

## 📚 **Best Practices**

- **Incremental processing** for large fact tables
- **Slowly changing dimensions** for historical tracking
- **Conformed dimensions** across all fact tables
- **Proper surrogate keys** for optimal join performance
- **Comprehensive documentation** for all models and columns

## 🛠️ **Maintenance**

### **Regular Tasks**
- Monitor model performance
- Update dimension tables as needed
- Refresh fact tables incrementally
- Validate data quality metrics

### **Troubleshooting**
- Check dbt logs for compilation errors
- Verify Snowflake connection and permissions
- Validate source data availability
- Review model dependencies and references
