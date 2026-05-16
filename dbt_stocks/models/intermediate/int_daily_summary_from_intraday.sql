-- int_daily_summary_from_intraday.sql
{{ config(
    materialized='table',
    labels={'contains_pii': 'no', 'priority': 'high'}
) }}

with
    base as (
        select *
        from {{ ref("int_finnhub_intraday_cleaned") }}
        where data_quality_status = 'VALID'
    ),

    final as (
        select
            symbol,
            quote_date,
            any_value(data_source) as data_source,
            array_agg(current_price order by quote_timestamp limit 1)[
                offset(0)
            ] as open_price,  -- The first price of the day

            max(current_price) as day_high,
            min(current_price) as day_low,

            array_agg(current_price order by quote_timestamp desc limit 1)[
                offset(0)
            ] as close_price,  -- The last price of the day

            max(ingest_timestamp) as ingest_timestamp

        from base
        group by 1, 2
    )

select *
from final
