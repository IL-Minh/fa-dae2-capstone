{{
  config(
    materialized='view',
    tags=['staging', 'users', 'profiles']
  )
}}

with user_profiles as (
    select
        user_id,
        first_name,
        last_name,
        email,
        age,
        income_bracket,
        customer_tier,
        risk_profile,
        city,
        state,
        country,
        registration_date,
        preferred_categories,
        is_active,
        source_system,
        -- Add metadata
        current_timestamp() as ingested_at
    from {{ source('raw', 'USER_REGISTRATIONS') }}
),

final as (
    select
        user_id,
        first_name,
        last_name,
        email,
        age,
        -- Create age groups for analysis
        income_bracket,
        customer_tier,
        risk_profile,
        city,
        state,
        country,
        registration_date,
        -- Add region based on state
        preferred_categories,
        is_active,
        source_system,
        ingested_at,
        case
            when age < 25 then '18-24'
            when age < 35 then '25-34'
            when age < 45 then '35-44'
            when age < 55 then '45-54'
            when age < 65 then '55-64'
            else '65+'
        end as age_group,
        case
            when state in ('ME', 'NH', 'VT', 'MA', 'RI', 'CT') then 'Northeast'
            when state in ('NY', 'NJ', 'PA') then 'Northeast'
            when
                state in (
                    'OH',
                    'IN',
                    'IL',
                    'MI',
                    'WI',
                    'MN',
                    'IA',
                    'MO',
                    'ND',
                    'SD',
                    'NE',
                    'KS'
                )
                then 'Midwest'
            when
                state in (
                    'DE',
                    'MD',
                    'DC',
                    'VA',
                    'WV',
                    'NC',
                    'SC',
                    'GA',
                    'FL',
                    'KY',
                    'TN',
                    'AL',
                    'MS',
                    'AR',
                    'LA',
                    'OK'
                )
                then 'South'
            when
                state in (
                    'MT',
                    'ID',
                    'WY',
                    'CO',
                    'NM',
                    'AZ',
                    'UT',
                    'NV',
                    'WA',
                    'OR',
                    'CA',
                    'AK',
                    'HI'
                )
                then 'West'
            else 'Other'
        end as region
    from user_profiles
)

select * from final
