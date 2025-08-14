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
        tx_id,
        user_id,
        amount,
        currency,
        merchant,
        category,
        timestamp,
        source_file,
        ingested_at,
        -- Add data quality checks
        case
            when tx_id is not null and tx_id != '' then true
            else false
        end as is_valid_tx_id,
        case
            when user_id is not null and user_id > 0 then true
            else false
        end as is_valid_user_id,
        case
            when amount is not null and amount > 0 then true
            else false
        end as is_valid_amount,
        case
            when timestamp is not null then true
            else false
        end as is_valid_timestamp,
        -- Add file processing metadata
        split_part(source_file, '_', 1) as data_source,
        split_part(source_file, '_', 2) as year,
        split_part(source_file, '_', 3) as month
    from source
)

select * from cleaned
