{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'users']
  )
}}

with user_profiles as (
    select * from {{ ref('stg_users') }}
),

final as (
    select
        -- Generate surrogate key for the dimension
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_key,

        -- Business keys
        user_id,

        -- User attributes
        first_name,
        last_name,
        email,
        age,
        age_group,
        income_bracket,
        customer_tier,
        risk_profile,

        -- Location attributes
        city,
        state,
        country,
        region,

        -- Business attributes
        registration_date,
        preferred_categories,
        is_active,
        source_system,

        -- Slowly changing dimension fields
        ingested_at as effective_date,
        null as end_date,
        true as is_current,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from user_profiles
)

select * from final
