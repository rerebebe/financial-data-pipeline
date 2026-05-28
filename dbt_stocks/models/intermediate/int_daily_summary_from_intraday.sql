-- int_daily_summary_from_intraday.sql
{{
    config(
        materialized="incremental",
        unique_key="unique_summary_id",
        labels={"contains_pii": "no", "priority": "high"},
    )
}}

with
    base as (
        select *
        from {{ ref("int_finnhub_intraday_cleaned") }}
        where
            data_quality_status = 'VALID'
            {% if is_incremental() %}
                -- Only look at the last 3 days of streaming data to keep runs fast
                and quote_date >= current_date() - 3
            {% endif %}

    ),

    aggregated as (
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
    ),

    final as (
        select
            -- Generate the deterministic MD5 surrogate key
            {{ dbt_utils.generate_surrogate_key(["symbol", "quote_date"]) }}
            as unique_summary_id,
            symbol,
            quote_date,
            data_source,
            open_price,
            day_high,
            day_low,
            close_price,
            ingest_timestamp
        from aggregated
    )

select *
from final
