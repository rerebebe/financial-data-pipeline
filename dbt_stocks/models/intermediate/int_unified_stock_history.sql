-- int_unified_stock_history.sql
-- partition_by & cluster_by only scan the relevant partitions for incremental runs,
-- hence improving performance
{{
    config(
        materialized="incremental",
        unique_key="unique_int_id",
        incremental_strategy="merge",
        partition_by={
            "field": "quote_date",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["symbol"],
    )
}}


with
    daily_summary_from_today as (
        select *
        from {{ ref("int_daily_summary_from_intraday") }}
        where quote_date >= current_date() - 3
    ),

    raw_unioned as (
        {% if not is_incremental() %}
            -- FIRST RUN: Load the full 5-year history
            select
                symbol,
                quote_date,
                open_price,
                day_high,
                day_low,
                close_price,
                {# volume, #}
                ingest_timestamp,
                data_source
            from {{ ref("stg_5_year_stock_price") }}

        {% else %}

            select
                symbol,
                quote_date,
                open_price,
                day_high,
                day_low,
                close_price,
                ingest_timestamp,
                data_source
            from daily_summary_from_today

            union all

            select
                symbol,
                quote_date,
                open_price_validated as open_price,
                day_high_validated as day_high,
                day_low_validated as day_low,
                close_price_validated as close_price,
                ingest_timestamp,
                data_source
            from {{ this }}
            where
                quote_date
                >= (select date_sub(max(quote_date), interval 10 day) from {{ this }})

        {% endif %}
    ),

    with_previous_close as (
        select
            *,
            -- Calculate previous close AFTER the union
            nullif(
                lag(close_price) over (partition by symbol order by quote_date), 0
            ) as previous_close_price,
            {{ dbt_utils.generate_surrogate_key(["symbol", "quote_date"]) }}
            as unique_int_id

        from raw_unioned
        qualify
            row_number() over (
                partition by symbol, quote_date order by ingest_timestamp desc
            )
            = 1
    ),

    validated as (
        select
            *,
            -- Casting to FLOAT64 for high-precision technical analysis
            safe_cast(nullif(open_price, 0) as float64) as open_price_validated,
            safe_cast(nullif(day_high, 0) as float64) as day_high_validated,
            safe_cast(nullif(day_low, 0) as float64) as day_low_validated,
            safe_cast(nullif(close_price, 0) as float64) as close_price_validated

        from with_previous_close
    ),

    enriched as (
        select
            unique_int_id,
            symbol,
            quote_date,
            open_price_validated,
            day_high_validated,
            day_low_validated,
            close_price_validated,
            previous_close_price,
            ingest_timestamp,
            data_source,

            -- Price Change Math
            close_price_validated - previous_close_price as daily_price_change,

            safe_divide(
                close_price_validated - previous_close_price, previous_close_price
            ) as daily_price_change_percentage,

            -- Corrected RSI Gain/Loss Logic (Magnitudes must be positive)
            case
                when close_price_validated > previous_close_price
                then close_price_validated - previous_close_price
                else 0
            end as gain,
            case
                when close_price_validated < previous_close_price
                then previous_close_price - close_price_validated
                else 0
            end as loss,

            case
                when close_price_validated <= 0 or close_price_validated is null
                then 'INVALID'
                when day_high_validated < day_low_validated
                then 'INCONSISTENT'
                when
                    close_price_validated > day_high_validated
                    or close_price_validated < day_low_validated
                then 'INCONSISTENT'
                else 'VALID'
            end as data_quality_status

        from validated
    )

select *
from enriched
{% if is_incremental() %}
    -- Only update/insert the most recent date to avoid bloating the merge
    where quote_date >= (select min(quote_date) from daily_summary_from_today)
{% endif %}
