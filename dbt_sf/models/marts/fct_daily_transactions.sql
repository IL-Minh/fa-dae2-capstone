{{
  config(
    materialized='table',
    tags=['marts', 'transactions', 'daily_summary']
  )
}}

with daily_transactions as (
    select
        date(timestamp) as transaction_date,
        source_system,
        category,
        currency,
        count(*) as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(amount) as min_amount,
        max(amount) as max_amount,
        count(distinct user_id) as unique_users,
        count(distinct merchant) as unique_merchants
    from {{ ref('stg_transactions_unified') }}
    where timestamp is not null
    group by 1, 2, 3, 4
),

final as (
    select
        transaction_date,
        source_system,
        category,
        currency,
        transaction_count,
        total_amount,
        avg_amount,
        min_amount,
        max_amount,
        unique_users,
        unique_merchants,
        -- Add business metrics
        case
            when category in ('salary', 'bonus') then 'income'
            when category in ('rent', 'utilities', 'groceries') then 'essential'
            else 'discretionary'
        end as spending_category,
        -- Add date dimensions
        extract(year from transaction_date) as year,
        extract(month from transaction_date) as month,
        extract(day from transaction_date) as day,
        extract(dayofweek from transaction_date) as day_of_week
    from daily_transactions
)

select * from final
