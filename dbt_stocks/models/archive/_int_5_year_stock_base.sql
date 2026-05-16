{{ config(enabled=false) }}

with
    stock_price_history as (
        select
            *,
            -- previous_close_price,
            nullif(
                lag(close_price) over (partition by symbol order by quote_date), 0
            ) as previous_close_price
        from {{ ref("stg_5_year_stock_price") }}

    ),

    validated as (
        select
            *,

            -- open_price,
            cast(
                case
                    when open_price <= 0 or open_price is null then null else open_price
                end as float64
            ) as open_price_validated,

            -- day_high,
            cast(
                case
                    when high_price <= 0 or high_price is null then null else high_price
                end as float64
            ) as day_high_validated,

            -- day_low,
            cast(
                case
                    when low_price <= 0 or low_price is null then null else low_price
                end as float64
            ) as day_low_validated,

            -- close_price,
            cast(
                case
                    when close_price <= 0 or close_price is null
                    then null
                    else close_price
                end as float64
            ) as close_price_validated,

            case
                when close_price_validated is null or close_price_validated <= 0
                then 'INVALID'
                when day_high_validated < day_low_validated
                then 'INCONSISTENT'
                when
                    close_price_validated > day_high_validated
                    or close_price_validated < day_low_validated
                then 'INCONSISTENT'
                else 'VALID'
            end as data_quality_status

        from stock_price_history
    ),

    enriched as (
        select
            symbol,
            quote_date,
            open_price_validated,
            day_high_validated,
            day_low_validated,
            close_price_validated,
            previous_close_price,
            volume,
            ingest_timestamp,
            data_source,
            data_quality_status,

            -- daily_price_change (Current - Previous Close),
            close_price_validated - previous_close_price as daily_price_change,

            -- Daily % Change (using NULLIF to prevent division by zero)
            safe_divide(
                close_price_validated - previous_close_price, previous_close_price
            ) as daily_price_change_percentage,

            -- gain
            abs(
                case
                    when close_price_validated > previous_close_price
                    then close_price_validated - previous_close_price
                    else 0
                end
            ) as gain,

            -- loss
            abs(
                case
                    when close_price_validated < previous_close_price
                    then close_price_validated - previous_close_price
                    else 0
                end
            ) as loss

        from validated
    )

select *
from enriched
