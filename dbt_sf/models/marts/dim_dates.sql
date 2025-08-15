{{
  config(
    materialized='table',
    tags=['marts', 'dimensions', 'dates']
  )
}}

with date_spine as (
    select
        date_value
    from (
        select
            dateadd(day, seq4(), '2024-01-01'::date) as date_value
        from table(generator(rowcount => 1000))
    )
    where date_value <= '2026-12-31'::date
),

final as (
    select
        -- Generate surrogate key for the dimension
        {{ dbt_utils.generate_surrogate_key(['date_value']) }} as date_key,

        -- Date attributes
        date_value as full_date,
        extract(year from date_value) as year,
        extract(quarter from date_value) as quarter,
        extract(month from date_value) as month,
        monthname(date_value) as month_name,
        extract(week from date_value) as week_of_year,
        extract(dayofyear from date_value) as day_of_year,
        extract(dayofweek from date_value) as day_of_week,
        dayname(date_value) as day_name,

        -- Business calendar flags
        case when dayofweek(date_value) in (1, 7) then true else false end as is_weekend,
        case when date_value in ('2024-01-01', '2024-07-04', '2024-12-25') then true else false end as is_holiday,

        -- Fiscal calendar (assuming fiscal year starts in July)
        case
            when month(date_value) >= 7 then year(date_value) + 1
            else year(date_value)
        end as fiscal_year,
        case
            when month(date_value) >= 7 then month(date_value) - 6
            else month(date_value) + 6
        end as fiscal_month,
        case
            when fiscal_month <= 3 then 1
            when fiscal_month <= 6 then 2
            when fiscal_month <= 9 then 3
            else 4
        end as fiscal_quarter,

        -- Metadata
        current_timestamp() as dbt_processed_at
    from date_spine
)

select * from final
