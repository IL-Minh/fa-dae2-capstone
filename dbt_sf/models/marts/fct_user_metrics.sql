{{
  config(
    materialized='table',
    tags=['marts', 'facts', 'user_metrics']
  )
}}

with user_transactions as (
    select
        user_id,
        date_trunc('month', timestamp) as month_start,
        count(*) as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(amount) as min_amount,
        max(amount) as max_amount,
        count(distinct category) as unique_categories,
        count(distinct merchant) as unique_merchants,
        -- Spending pattern analysis
        case
            when avg(amount) > 1000 then 'high_spender'
            when avg(amount) > 100 then 'medium_spender'
            else 'low_spender'
        end as spending_pattern,
        -- Frequency analysis
        case
            when count(*) > 50 then 'frequent'
            when count(*) > 20 then 'regular'
            else 'occasional'
        end as frequency_pattern,
        -- Risk analysis based on spending patterns
        case
            when count(*) > 100 and avg(amount) > 500 then 'high_risk'
            when count(*) > 50 and avg(amount) > 200 then 'medium_risk'
            else 'low_risk'
        end as risk_indicator
    from {{ ref('int_transactions') }}
    group by user_id, date_trunc('month', timestamp)
),

final as (
    select
        -- Business keys
        ut.user_id,
        ut.month_start,

        -- Foreign key to user dimension
        du.user_key,

        -- Metrics
        ut.transaction_count,
        ut.total_amount,
        ut.avg_amount,
        ut.min_amount,
        ut.max_amount,
        ut.unique_categories,
        ut.unique_merchants,

        -- Behavioral patterns
        ut.spending_pattern,
        ut.frequency_pattern,
        ut.risk_indicator,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from user_transactions ut
    left join {{ ref('dim_users') }} du on ut.user_id = du.user_id and du.is_current = true
)

select * from final
