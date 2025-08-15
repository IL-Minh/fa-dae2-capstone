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
        dt.transaction_date,
        dt.source_system,
        dt.transaction_tier,

        -- Foreign key to date dimension
        dd.date_key,

        -- Metrics
        dt.transaction_count,
        dt.total_amount,
        dt.avg_amount,
        dt.min_amount,
        dt.max_amount,
        dt.unique_users,
        dt.unique_merchants,
        dt.unique_categories,
        dt.currency_count,

        -- Business metrics
        case
            when dt.transaction_tier = 'high_value' then 'premium'
            when dt.transaction_tier = 'medium_value' then 'standard'
            else 'basic'
        end as value_category,

        -- Date dimensions
        extract(year from dt.transaction_date) as year,
        extract(month from dt.transaction_date) as month,
        extract(day from dt.transaction_date) as day,
        extract(dayofweek from dt.transaction_date) as day_of_week,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from daily_transactions as dt
    left join {{ ref('dim_dates') }} as dd on dt.transaction_date = dd.full_date
)

select * from final
