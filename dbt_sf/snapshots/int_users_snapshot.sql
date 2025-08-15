{% snapshot int_users_snapshot %}

{{
    config(
      unique_key='user_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email', 'age', 'age_group', 'income_bracket', 'customer_tier', 'risk_profile', 'city', 'state', 'country', 'region', 'registration_date', 'preferred_categories', 'is_active', 'source_system'],
      invalidate_hard_deletes=True,
    )
}}

    select * from {{ ref('stg_users') }}

{% endsnapshot %}
