{{
  config(
    materialized='view',
    tags=['staging', 'transactions', 'batch']
  )
}}

with source as (
    select * from {{ source('raw', 'TRANSACTIONS_BATCH_CSV') }}
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
        source_file::{{ dbt.type_string() }} as source_file,
        ingested_at::{{ dbt.type_timestamp() }} as ingested_at,
        -- Add file processing metadata
        split_part(source_file, '_', 1) as data_source,
        split_part(source_file, '_', 2) as year,
        split_part(source_file, '_', 3) as month,
        -- Add hash key for deduplication using cross-database hash macro
        {{ dbt.hash("concat_ws('|', tx_id, user_id, amount, currency, merchant, category, timestamp, source_file)") }} as hash_key
    from source
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by tx_id
            order by ingested_at desc, hash_key
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
    source_file,
    ingested_at,
    data_source,
    year,
    month,
    hash_key
from deduplicated
where rn = 1  -- Keep only the latest record for each tx_id
