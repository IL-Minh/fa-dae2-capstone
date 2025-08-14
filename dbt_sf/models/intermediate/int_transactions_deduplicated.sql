{{
  config(
    materialized='incremental',
    unique_key='hash_key',
    tags=['intermediate', 'transactions', 'deduplicated']
  )
}}

with source as (
    select * from {{ ref('stg_transactions_unified') }}
),

final as (
    select
        tx_id,
        user_id,
        amount,
        currency,
        merchant,
        category,
        timestamp,
        source_system,
        ingested_at,
        processed_at,
        source_file,
        hash_key,
        -- Add business logic fields
        case
            when amount > 1000 then 'high_value'
            when amount > 100 then 'medium_value'
            else 'low_value'
        end as transaction_tier,
        -- Add time-based fields using Snowflake native functions
        date_trunc('day', timestamp) as transaction_date,
        date_trunc('hour', timestamp) as transaction_hour,
        date_trunc('day', timestamp) as day_of_week,
        -- Add metadata using Snowflake native function
        current_timestamp() as dbt_processed_at
    from source
)

select * from final

{% if is_incremental() %}
  -- Only process new records
  where hash_key not in (
    select hash_key from {{ this }}
  )
{% endif %}
