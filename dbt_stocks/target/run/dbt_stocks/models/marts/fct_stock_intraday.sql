

  create or replace view `stock-market-project-487008`.`stock_market_data_marts`.`fct_stock_intraday`
  OPTIONS()
  as -- fct_stock_intraday.sql


with
    today_minutes_base as (
        select *
        from `stock-market-project-487008`.`stock_market_data_intermediate`.`int_finnhub_intraday_cleaned`

        
        where data_quality_status = 'VALID'
        qualify quote_date = max(quote_date) over (partition by symbol)
    ),

    latest_indicators as (select * from `stock-market-project-487008`.`stock_market_data_marts`.`fct_latest_indicators`)

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
left join latest_indicators li on tmb.symbol = li.symbol;

