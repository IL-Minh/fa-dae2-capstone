{{
  config(
    materialized='table',
    tags=['marts', 'transactions', 'daily_summary']
  )
}}

with daily_transactions as (
    select
        transaction_date,
        source_system,
        transaction_tier,
        transaction_count,
        total_amount,
        avg_amount,
        min_amount,
        max_amount,
        unique_users,
        unique_merchants,
        unique_categories,
        currency_count
    from {{ ref('int_transactions_metrics') }}
    where transaction_date is not null
),

final as (
    select
        transaction_date,
        source_system,
        transaction_tier,
        transaction_count,
        total_amount,
        avg_amount,
        min_amount,
        max_amount,
        unique_users,
        unique_merchants,
        unique_categories,
        currency_count,
        -- Add business metrics based on transaction tier
        case
            when transaction_tier = 'high_value' then 'premium'
            when transaction_tier = 'medium_value' then 'standard'
            else 'basic'
        end as value_category,
        -- Add date dimensions using Snowflake native functions
        extract(year from transaction_date) as year,
        extract(month from transaction_date) as month,
        extract(day from transaction_date) as day,
        extract(dayofweek from transaction_date) as day_of_week
    from daily_transactions
)

select * from final
