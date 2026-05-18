-- fct_latest_indicators.sql


select symbol, quote_date, sma_20, sma_50, sma_200, ema_9, ema_20, rsi_14
from `stock-market-project-487008`.`stock_market_data_marts`.`fct_stock_history_performance`
qualify row_number() over (partition by symbol order by quote_date desc) = 1