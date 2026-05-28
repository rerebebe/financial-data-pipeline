-- fct_stock_intraday.sql
-- it's a view just in case that the `fct_latest_indicators`'s join logic will scan
-- the massive 'fct_stock_history_performance` every five minutes, which would be bad
-- for performance.
{{
    config(
        materialized="view",
        description="Powers the real-time UI. Filters for today only and calculates live price action metrics.",
    )
}}

with
    today_minutes_base as (
        select *
        from {{ ref("int_finnhub_intraday_cleaned") }}

        {# where
            quote_date = current_date('America/New_York')
            and data_quality_status = 'VALID' #}
        where data_quality_status = 'VALID'
        qualify quote_date = max(quote_date) over (partition by symbol)
    ),

    latest_indicators as (select * from {{ ref("fct_latest_indicators") }})

select
    tmb.symbol,
    tmb.quote_timestamp,
    tmb.open_price,
    tmb.day_high,
    tmb.day_low,
    tmb.current_price,
    tmb.daily_price_change,
    tmb.daily_price_change_percentage,

    li.sma_20,
    li.sma_50,
    li.sma_200,
    li.ema_9,
    li.ema_20,
    li.rsi_14
from today_minutes_base tmb
left join latest_indicators li on tmb.symbol = li.symbol
