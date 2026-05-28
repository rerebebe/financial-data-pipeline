-- fct_latest_indicators.sql
{{
    config(
        materialized="table",
        description="Latest technical indicators per symbol, pre-computed for real-time UI consumption.          This model is a thin slice of fct_stock_history_performance — it adds no new         calculations or business logic. It exists purely to avoid scanning the full         history table (7,800+ rows) on every intraday query.          Design note:         This fct model intentionally reads from another fct model (fct_stock_history_performance).         This is acceptable here because:         1. It is a filter-only operation (latest row per symbol via qualify)         2. No new transformations or business logic are introduced         3. Indicators are computed in fct_stock_history_performance using complex macros            (cal_ema_vectorized, cal_rsi_wilder etc.) that cannot be duplicated upstream         4. Refreshed once per EOD DAG run — indicators are daily values, not intraday          Consumers:         - fct_stock_intraday (real-time view, queried every 5 min during market hours)     ",
    )
}}

select symbol, quote_date, sma_20, sma_50, sma_200, ema_9, ema_20, rsi_14
from {{ ref("fct_stock_history_performance") }}
qualify row_number() over (partition by symbol order by quote_date desc) = 1
