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
        tx_id,
        user_id,
        amount,
        currency,
        merchant,
        category,
        timestamp,
        ingested_at,
        airflow_ingested_at,
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
        end as is_valid_timestamp
    from source
)

select * from cleaned
