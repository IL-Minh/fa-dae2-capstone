{{
  config(
    materialized='view',
    tags=['staging', 'transactions', 'unified']
  )
}}

with streaming as (
    select
        tx_id,
        user_id,
        amount,
        currency,
        merchant,
        category,
        timestamp,
        'streaming_kafka' as source_system,
        ingested_at,
        airflow_ingested_at as processed_at,
        null as source_file
    from {{ ref('stg_transactions_streaming_kafka') }}
),

batch as (
    select
        tx_id,
        user_id,
        amount,
        currency,
        merchant,
        category,
        timestamp,
        'batch_csv' as source_system,
        ingested_at,
        ingested_at as processed_at,
        source_file
    from {{ ref('stg_transactions_batch_csv') }}
),

unified as (
    select * from streaming
    union all
    select * from batch
)

select * from unified
