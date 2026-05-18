
  
    

    create or replace table `stock-market-project-487008`.`stock_market_data_marts`.`fct_stock_history_performance`
      
    
    

    
    OPTIONS()
    as (
      -- fct_stock_history_performance.sql
-- incremental


with
    -- 1. Get our baseline data with a safety lookback for window functions
    base as (
        select *
        from `stock-market-project-487008`.`stock_market_data_intermediate`.`int_unified_stock_history`
        where
            data_quality_status = 'VALID'

            
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
            to_hex(md5(cast(coalesce(cast(symbol as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(quote_date as string), '_dbt_utils_surrogate_key_null_') as string)))
            as unique_id,

            -- Basic Returns
            
    ln(close_price_validated / previous_close_price)

            as log_return,

            -- Category A: Overlays
            -- Moving Averages
            
    avg(close_price_validated) over (
        partition by symbol
        order by
            quote_date rows between 19 preceding and current row
    )

            as sma_20,
            
    avg(close_price_validated) over (
        partition by symbol
        order by
            quote_date rows between 49 preceding and current row
    )

            as sma_50,
            
    avg(close_price_validated) over (
        partition by symbol
        order by
            quote_date rows between 199 preceding and current row
    )

            as sma_200,

            -- Foundation for Bollinger Bands
            stddev(close_price_validated) over (
                partition by symbol
                order by quote_date
                rows between 19 preceding and current row
            ) as stddev_20,

            -- ema with Wilder's Smoothing
            

    
    

    -- We use the sum of weighted prices divided by the sum of weights
    (
        sum(close_price_validated * power(0.8, - ((row_num)))) over (
            partition by symbol
            order by quote_date
            rows between unbounded preceding and current row
        )
    ) / (
        sum(power(0.8, - ((row_num)))) over (
            partition by symbol
            order by quote_date
            rows between unbounded preceding and current row
        )
    )

 as ema_9,  -- noqa: 
            

    
    

    -- We use the sum of weighted prices divided by the sum of weights
    (
        sum(close_price_validated * power(0.9047619047619048, - ((row_num)))) over (
            partition by symbol
            order by quote_date
            rows between unbounded preceding and current row
        )
    ) / (
        sum(power(0.9047619047619048, - ((row_num)))) over (
            partition by symbol
            order by quote_date
            rows between unbounded preceding and current row
        )
    )

 as ema_20,

            -- Category B: Oscillators (RSI)
            

    
    

    
    
    

    case
        when (SUM(loss * POWER(0.9285714285714286, - row_num)) OVER (PARTITION BY symbol ORDER BY quote_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 0
        then 100
        else 100 - (100 / (1 + ((SUM(gain * POWER(0.9285714285714286, - row_num)) OVER (PARTITION BY symbol ORDER BY quote_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) / (SUM(loss * POWER(0.9285714285714286, - row_num)) OVER (PARTITION BY symbol ORDER BY quote_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)))))
    end


            as rsi_14,

            -- Category C: Risk/Volatility (for later calculation)
            -- True Range (Foundation for ATR)
            
    greatest(
        day_high_validated - day_low_validated,
        abs(day_high_validated - previous_close_price),
        abs(day_low_validated - previous_close_price)
    )
 as true_range

        from prep
    ),

    -- 3. Calculate secondary indicators (Things that depend on primary indicators)
    derived_indicators as (
        select
            *,
            -- Bollinger Bands
            
    
        sma_20 + (2 * stddev_20)
    

 as bb_upper_20,
            
     sma_20 - (2 * stddev_20)
    

 as bb_lower_20,

            -- Category C: The Risk & Performance (Returns) --
            
    avg(true_range) over (
        partition by symbol
        order by
            quote_date rows between 13 preceding and current row
    )
 as atr_14,
            
    
        exp(
            sum(log_return) over (
                partition by symbol order by quote_date
            )
        )
        - 1

    

            as cumulative_return,  -- Cumulative Return

            
    stddev(log_return) over (
        partition by symbol
        order by
            quote_date rows between 20 preceding and current row
    )
    * sqrt(252)  -- 252 trading days in a year

            as volatility_21d
        from primary_indicators
    )

-- 4. Final Output: Filtering for only new data in incremental runs
select *
from derived_indicators

    );
  