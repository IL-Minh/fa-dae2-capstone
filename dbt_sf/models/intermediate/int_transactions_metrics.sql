{{
  config(
    materialized='incremental',
    unique_key='metric_key',
    tags=['intermediate', 'transactions', 'metrics']
  )
}}

with source as (
    select * from {{ ref('int_transactions_deduplicated') }}
),

daily_metrics as (
    select
        transaction_date,
        source_system,
        transaction_tier,
        -- Count metrics
        count(*) as transaction_count,
        count(distinct user_id) as unique_users,
        count(distinct merchant) as unique_merchants,
        count(distinct category) as unique_categories,
        -- Amount metrics
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(amount) as min_amount,
        max(amount) as max_amount,
        -- Currency distribution
        count(distinct currency) as currency_count,
        -- Create unique key for incremental processing using cross-database hash macro
        {{ dbt.hash("concat_ws('|', transaction_date, source_system, transaction_tier)") }} as metric_key,
        -- Metadata using Snowflake native function
        current_timestamp() as dbt_processed_at
    from source
    group by 1, 2, 3
)

select * from daily_metrics

{% if is_incremental() %}
  -- Only process new records
  where metric_key not in (
    select metric_key from {{ this }}
  )
{% endif %}
