-- fct_stock_indicators_unpivoted.sql
{{ config(materialized="view") }}

with
    base as (
        select
            symbol,
            quote_date,
            'close_price' as indicator,
            close_price_validated as value

        from {{ ref("fct_stock_history_performance") }}

        union all

        select symbol, quote_date, 'SMA 20', sma_20
        from {{ ref("fct_stock_history_performance") }}

        union all

        select symbol, quote_date, 'SMA 50', sma_50
        from {{ ref("fct_stock_history_performance") }}

        union all

        select symbol, quote_date, 'SMA 200', sma_200
        from {{ ref("fct_stock_history_performance") }}
    )

select *
from base
where value is not null
