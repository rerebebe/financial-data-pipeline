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
where
    value is not null

    {# {{ config(materialized="view") }}

with unpivoted_data as (
    select 
        symbol,
        quote_date,
        indicator,
        value
    from {{ ref("fct_stock_history_performance") }}
    unpivot(
        value for indicator in (
            close_price_validated as 'close_price',
            sma_20 as 'SMA 20',
            sma_50 as 'SMA 50',
            sma_200 as 'SMA 200'
        )
)

select * 
from unpivoted_data
-- BigQuery's UNPIVOT automatically excludes null values by default, 
-- but explicitly stating it keeps it clean!
where value is not null #}
