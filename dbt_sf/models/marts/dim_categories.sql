{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'categories']
  )
}}

with category_data as (
    select distinct
        category,
        -- Create category groups for analysis
        case
            when category in ('groceries', 'utilities', 'rent') then 'essential'
            when
                category in ('entertainment', 'travel', 'shopping')
                then 'discretionary'
            when category in ('salary', 'investment') then 'income'
            when category in ('transport', 'health') then 'lifestyle'
            else 'other'
        end as category_group,

        -- Business unit mapping
        case
            when category_group = 'essential' then 'operations'
            when category_group = 'discretionary' then 'marketing'
            when category_group = 'income' then 'finance'
            when category_group = 'lifestyle' then 'hr'
            else 'general'
        end as business_unit,

        -- Category type classification
        case
            when category in ('salary', 'investment') then 'income'
            when
                category in (
                    'groceries', 'utilities', 'rent', 'transport', 'health'
                )
                then 'expense'
            when
                category in ('entertainment', 'travel', 'shopping')
                then 'expense'
            else 'expense'
        end as category_type,

        current_timestamp() as ingested_at
    from {{ ref('stg_transactions_unified') }}
),

final as (
    select
        -- Generate surrogate key for the dimension
        {{ dbt_utils.generate_surrogate_key(['category']) }} as category_key,

        -- Business keys
        category as category_name,

        -- Category attributes
        category_group,
        category_type,
        business_unit,

        -- Status
        true as is_active,

        -- Slowly changing dimension fields
        ingested_at as effective_date,
        null as end_date,
        true as is_current,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from category_data
    qualify
        row_number() over (partition by category order by ingested_at desc) = 1
)

select * from final
