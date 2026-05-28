-- fct_stock_snapshot.sql
{{
    config(
        materialized="view",
        description="Latest price snapshot per symbol for Overview cards in Power BI.",
    )
}}

with
    latest_snapshot as (
        select *
        from {{ ref("int_finnhub_intraday_cleaned") }}
        where data_quality_status = 'VALID'
        qualify
            row_number() over (partition by symbol order by quote_timestamp desc) = 1
    )

{# latest_indicators as (select * from {{ ref("fct_latest_indicators") }}) #}
select
    symbol,
    quote_timestamp,
    open_price,
    day_high,
    day_low,
    current_price,
    daily_price_change,
    daily_price_change_percentage,
{# 
    li.sma_20,
    li.sma_50,
    li.sma_200,
    li.rsi_14 #}
from
    latest_snapshot
    {# left join latest_indicators li on s.symbol = li.symbol #}
