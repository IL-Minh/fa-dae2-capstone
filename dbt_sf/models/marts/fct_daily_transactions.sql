{{
  config(
    materialized='table',
    tags=['marts', 'facts', 'daily_transactions']
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
        -- Business keys
        transaction_date,
        source_system,
        transaction_tier,

        -- Foreign key to date dimension
        dd.date_key,

        -- Metrics
        transaction_count,
        total_amount,
        avg_amount,
        min_amount,
        max_amount,
        unique_users,
        unique_merchants,
        unique_categories,
        currency_count,

        -- Business metrics
        case
            when transaction_tier = 'high_value' then 'premium'
            when transaction_tier = 'medium_value' then 'standard'
            else 'basic'
        end as value_category,

        -- Date dimensions
        extract(year from transaction_date) as year,
        extract(month from transaction_date) as month,
        extract(day from transaction_date) as day,
        extract(dayofweek from transaction_date) as day_of_week,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from daily_transactions dt
    left join {{ ref('dim_dates') }} dd on dt.transaction_date = dd.full_date
)

select * from final
