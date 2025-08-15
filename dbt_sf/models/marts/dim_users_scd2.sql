{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'users', 'scd2', 'historical']
  )
}}

with user_snapshot as (
    select * from {{ ref('int_users_snapshot') }}
),

final as (
    select
        -- Generate surrogate key for the dimension (includes snapshot version)
        {{ dbt_utils.generate_surrogate_key(['user_id', 'dbt_valid_from']) }} as user_key,

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

        -- SCD2 fields from snapshot
        dbt_valid_from as effective_date,
        dbt_valid_to as end_date,
        case
            when dbt_valid_to is null then true
            else false
        end as is_current,
        dbt_updated_at,
        dbt_scd_id,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from user_snapshot
)

select * from final
