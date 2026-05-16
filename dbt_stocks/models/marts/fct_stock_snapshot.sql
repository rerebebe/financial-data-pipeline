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
    ),

    latest_indicators as (select * from {{ ref("fct_latest_indicators") }})

select
    s.symbol,
    s.quote_timestamp,
    s.open_price,
    s.day_high,
    s.day_low,
    s.current_price,
    s.daily_price_change,
    s.daily_price_change_percentage,

    li.sma_20,
    li.sma_50,
    li.sma_200,
    li.rsi_14

from latest_snapshot s
left join latest_indicators li on s.symbol = li.symbol
