-- int_finnhub_intraday_cleaned.sql
with
    base as (select * from {{ ref("stg_stock_quotes") }}),

    cleaned as (
        select
            *,
            (current_price is null or current_price <= 0) as is_price_invalid,
            (day_high < day_low) as is_range_inconsistent,
            (
                current_price > day_high or current_price < day_low
            ) as is_price_out_of_range,

            case
                when (current_price is null or current_price <= 0)
                then 'INVALID'
                when
                    (day_high < day_low)
                    or (current_price > day_high or current_price < day_low)
                then 'INCONSISTENT'
                else 'VALID'
            end as data_quality_status

        from base
    )

select
    symbol,
    quote_date,
    quote_timestamp,
    open_price,
    day_high,
    day_low,
    current_price,
    daily_price_change,
    daily_price_change_percentage,
    previous_close_price,
    ingest_timestamp,
    data_quality_status,
    data_source  -- new

from cleaned
