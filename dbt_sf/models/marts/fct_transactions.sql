{{
  config(
    materialized='table',
    tags=['marts', 'facts', 'transactions']
  )
}}

with transactions as (
    select * from {{ ref('int_transactions') }}
),

final as (
    select
        -- Business keys
        t.tx_id,
        t.user_id,

        -- Foreign keys to dimensions
        du.user_key,
        dm.merchant_key,
        dc.category_key,
        dcu.currency_key,
        dd.date_key,

        -- Fact measures
        t.amount,
        1 as transaction_count,

        -- Transaction attributes
        t.timestamp,
        t.source_system,

        -- Metadata
        t.ingested_at,
        t.processed_at,
        current_timestamp() as dbt_processed_at
    from transactions t
    left join {{ ref('dim_users') }} du on t.user_id = du.user_id and du.is_current = true
    left join {{ ref('dim_merchants') }} dm on t.merchant = dm.merchant_name and dm.is_current = true
    left join {{ ref('dim_categories') }} dc on t.category = dc.category_name and dc.is_current = true
    left join {{ ref('dim_currencies') }} dcu on t.currency = dcu.currency_code and dcu.is_current = true
    left join {{ ref('dim_dates') }} dd on date(t.timestamp) = dd.full_date
)

select * from final
