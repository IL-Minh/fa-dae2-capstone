{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'merchants']
  )
}}

with merchant_data as (
    select
        merchant,
        category,
        -- Extract business type from merchant name patterns
        case
            when lower(merchant) like '%bank%' or lower(merchant) like '%credit%' then 'financial'
            when lower(merchant) like '%store%' or lower(merchant) like '%market%' then 'retail'
            when lower(merchant) like '%restaurant%' or lower(merchant) like '%cafe%' then 'food_service'
            when lower(merchant) like '%gas%' or lower(merchant) like '%fuel%' then 'automotive'
            when lower(merchant) like '%hotel%' or lower(merchant) like '%travel%' then 'hospitality'
            when lower(merchant) like '%medical%' or lower(merchant) like '%health%' then 'healthcare'
            else 'other'
        end as business_type,

        -- Risk rating based on business type and category
        case
            when business_type = 'financial' then 'low'
            when business_type = 'healthcare' then 'low'
            when business_type = 'retail' and category in ('groceries', 'utilities') then 'low'
            when business_type = 'food_service' then 'medium'
            when business_type = 'automotive' then 'medium'
            when business_type = 'hospitality' then 'medium'
            else 'medium'
        end as risk_rating,

        -- Location extraction (simplified - in real scenario would come from merchant master data)
        'Unknown' as location_city,
        'Unknown' as location_state,
        'USA' as location_country,

        current_timestamp() as ingested_at
    from {{ ref('stg_transactions_unified') }}
),

final as (
    select
        -- Generate surrogate key for the dimension
        {{ dbt_utils.generate_surrogate_key(['merchant']) }} as merchant_key,

        -- Business keys
        merchant as merchant_id,
        merchant as merchant_name,

        -- Business attributes
        category as merchant_category,
        business_type,
        risk_rating,

        -- Location attributes
        location_city,
        location_state,
        location_country,

        -- Status
        true as is_active,

        -- Slowly changing dimension fields
        ingested_at as effective_date,
        null as end_date,
        true as is_current,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from merchant_data
    qualify row_number() over (partition by merchant order by ingested_at desc) = 1
)

select * from final
