-- fct_stock_history_performance.sql
-- incremental
{{ config(materialized="table", unique_key="unique_id", incremental_strategy="merge") }}

with
    -- 1. Get our baseline data with a safety lookback for window functions
    base as (
        select *
        from {{ ref("int_unified_stock_history") }}
        where
            data_quality_status = 'VALID'

            {% if is_incremental() %}
                -- Look back 210 trading rows to satisfy SMA 200
                and quote_date
                >= ({{ get_lookback_date(ref("int_unified_stock_history"), 210) }})
            {% endif %}
    ),

    -- for some macros, we need a "static" row number
    prep as (
        select
            *,
            -- Generate the row number here so it's "static" for the next CTE
            row_number() over (partition by symbol order by quote_date) as row_num
        from base
    ),

    -- 2. Calculate primary indicators (Overlays & Oscillators)
    primary_indicators as (
        select
            *,
            -- unique ID 
            {{ dbt_utils.generate_surrogate_key(["symbol", "quote_date"]) }}
            as unique_id,

            -- Basic Returns
            {{ cal_log_return("close_price_validated", "previous_close_price") }}
            as log_return,

            -- Category A: Overlays
            -- Moving Averages
            {{ cal_sma("close_price_validated", 20, "symbol", "quote_date") }}
            as sma_20,
            {{ cal_sma("close_price_validated", 50, "symbol", "quote_date") }}
            as sma_50,
            {{ cal_sma("close_price_validated", 200, "symbol", "quote_date") }}
            as sma_200,

            -- Foundation for Bollinger Bands
            stddev(close_price_validated) over (
                partition by symbol
                order by quote_date
                rows between 19 preceding and current row
            ) as stddev_20,

            -- ema with Wilder's Smoothing
            {{
                cal_ema_vectorized(
                    "close_price_validated", "row_num", 9, "symbol", "quote_date"
                )
            }} as ema_9,  -- noqa: 
            {{
                cal_ema_vectorized(
                    "close_price_validated", "row_num", 20, "symbol", "quote_date"
                )
            }} as ema_20,

            -- Category B: Oscillators (RSI)
            {{ cal_rsi_wilder("gain", "loss", "row_num", 14, "symbol", "quote_date") }}
            as rsi_14,

            -- Category C: Risk/Volatility (for later calculation)
            -- True Range (Foundation for ATR)
            {{
                cal_true_range(
                    "day_high_validated",
                    "day_low_validated",
                    "close_price_validated",
                    "previous_close_price",
                    "symbol",
                    "quote_date",
                )
            }} as true_range

        from prep
    ),

    -- 3. Calculate secondary indicators (Things that depend on primary indicators)
    derived_indicators as (
        select
            *,
            -- Bollinger Bands
            {{ cal_bollinger_band("sma_20", "stddev_20", 2, "upper") }} as bb_upper_20,
            {{ cal_bollinger_band("sma_20", "stddev_20", 2, "lower") }} as bb_lower_20,

            -- Category C: The Risk & Performance (Returns) --
            {{ cal_atr("true_range", "symbol", "quote_date", 14) }} as atr_14,
            {{ cumulative_return_from_log("log_return", "symbol", "quote_date") }}
            as cumulative_return,  -- Cumulative Return

            {{ cal_volatility("log_return", 21, "symbol", "quote_date") }}
            as volatility_21d
        from primary_indicators
    )

-- 4. Final Output: Filtering for only new data in incremental runs
select *
from derived_indicators
{% if is_incremental() %}
    -- MERGE the new dates into the final table.
    where
        quote_date
        >= (select min(quote_date) from {{ ref("int_daily_summary_from_intraday") }})
{% endif %}
