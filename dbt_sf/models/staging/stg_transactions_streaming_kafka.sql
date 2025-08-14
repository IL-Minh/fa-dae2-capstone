{{
  config(
    materialized='view',
    tags=['staging', 'transactions', 'streaming']
  )
}}

with source as (
    select * from {{ source('raw', 'TRANSACTIONS_STREAMING_KAFKA') }}
),

cleaned as (
    select
        tx_id::{{ dbt.type_string() }} as tx_id,
        user_id::{{ dbt.type_int() }} as user_id,
        amount::{{ dbt.type_numeric() }} as amount,
        currency::{{ dbt.type_string() }} as currency,
        merchant::{{ dbt.type_string() }} as merchant,
        category::{{ dbt.type_string() }} as category,
        timestamp::{{ dbt.type_timestamp() }} as timestamp,
        ingested_at::{{ dbt.type_timestamp() }} as ingested_at,
        airflow_ingested_at::{{ dbt.type_timestamp() }} as airflow_ingested_at,
        -- Add hash key for deduplication using cross-database hash macro
        {{ dbt.hash("concat_ws('|', tx_id, user_id, amount, currency, merchant, category, timestamp, ingested_at)") }} as hash_key
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by tx_id
            order by airflow_ingested_at desc, hash_key
        ) as rn
    from cleaned
)

select
    tx_id,
    user_id,
    amount,
    currency,
    merchant,
    category,
    timestamp,
    ingested_at,
    airflow_ingested_at,
    hash_key
from deduplicated
where rn = 1  -- Keep only the latest record for each tx_id
