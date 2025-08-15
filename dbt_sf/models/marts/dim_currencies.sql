{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'currencies']
  )
}}

with currency_data as (
    select distinct
        currency,
        -- Currency type classification
        case
            when currency = 'USD' then 'major'
            when currency in ('EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD') then 'major'
            when currency in ('CNY', 'INR', 'BRL', 'MXN') then 'emerging'
            else 'other'
        end as currency_type,

        -- Region mapping
        case
            when currency = 'USD' then 'North America'
            when currency = 'EUR' then 'Europe'
            when currency = 'GBP' then 'Europe'
            when currency = 'JPY' then 'Asia'
            when currency = 'CNY' then 'Asia'
            when currency = 'INR' then 'Asia'
            when currency = 'BRL' then 'South America'
            when currency = 'MXN' then 'North America'
            else 'Other'
        end as currency_region,

        -- Base exchange rate (simplified - in real scenario would come from external API)
        case
            when currency = 'USD' then 1.0
            when currency = 'EUR' then 0.85
            when currency = 'GBP' then 0.73
            when currency = 'JPY' then 110.0
            when currency = 'CNY' then 6.45
            when currency = 'INR' then 74.5
            when currency = 'BRL' then 5.2
            when currency = 'MXN' then 20.0
            else 1.0
        end as base_exchange_rate,

        current_timestamp() as ingested_at
    from {{ ref('stg_transactions_unified') }}
),

final as (
    select
        -- Generate surrogate key for the dimension
        {{ dbt_utils.generate_surrogate_key(['currency']) }} as currency_key,

        -- Business keys
        currency as currency_code,

        -- Currency attributes
        currency_type,
        currency_region,
        base_exchange_rate,

        -- Status
        true as is_active,

        -- Slowly changing dimension fields
        ingested_at as effective_date,
        null as end_date,
        true as is_current,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from currency_data
    qualify row_number() over (partition by currency order by ingested_at desc) = 1
)

select * from final
