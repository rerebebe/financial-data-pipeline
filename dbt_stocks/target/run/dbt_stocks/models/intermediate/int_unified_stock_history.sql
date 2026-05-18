-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `stock-market-project-487008`.`stock_market_data_intermediate`.`int_unified_stock_history` as DBT_INTERNAL_DEST
        using (-- int_unified_stock_history.sql
-- partition_by & cluster_by only scan the relevant partitions for incremental runs,
-- hence improving performance



with
    daily_summary_from_today as (
        select *
        from `stock-market-project-487008`.`stock_market_data_intermediate`.`int_daily_summary_from_intraday`
        where quote_date >= current_date() - 3
    ),

    raw_unioned as (
        

            select
                symbol,
                quote_date,
                open_price,
                day_high,
                day_low,
                close_price,
                ingest_timestamp,
                data_source
            from daily_summary_from_today

            union all

            select
                symbol,
                quote_date,
                open_price_validated as open_price,
                day_high_validated as day_high,
                day_low_validated as day_low,
                close_price_validated as close_price,
                ingest_timestamp,
                data_source
            from `stock-market-project-487008`.`stock_market_data_intermediate`.`int_unified_stock_history`
            where
                quote_date
                >= (select date_sub(max(quote_date), interval 10 day) from `stock-market-project-487008`.`stock_market_data_intermediate`.`int_unified_stock_history`)

        
    ),

    with_previous_close as (
        select
            *,
            -- Calculate previous close AFTER the union
            nullif(
                lag(close_price) over (partition by symbol order by quote_date), 0
            ) as previous_close_price,
            to_hex(md5(cast(coalesce(cast(symbol as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(quote_date as string), '_dbt_utils_surrogate_key_null_') as string)))
            as unique_int_id

        from raw_unioned
        qualify
            row_number() over (
                partition by symbol, quote_date order by ingest_timestamp desc
            )
            = 1
    ),

    validated as (
        select
            *,
            -- Casting to FLOAT64 for high-precision technical analysis
            safe_cast(nullif(open_price, 0) as float64) as open_price_validated,
            safe_cast(nullif(day_high, 0) as float64) as day_high_validated,
            safe_cast(nullif(day_low, 0) as float64) as day_low_validated,
            safe_cast(nullif(close_price, 0) as float64) as close_price_validated

        from with_previous_close
    ),

    enriched as (
        select
            unique_int_id,
            symbol,
            quote_date,
            open_price_validated,
            day_high_validated,
            day_low_validated,
            close_price_validated,
            previous_close_price,
            ingest_timestamp,
            data_source,

            -- Price Change Math
            close_price_validated - previous_close_price as daily_price_change,

            safe_divide(
                close_price_validated - previous_close_price, previous_close_price
            ) as daily_price_change_percentage,

            -- Corrected RSI Gain/Loss Logic (Magnitudes must be positive)
            case
                when close_price_validated > previous_close_price
                then close_price_validated - previous_close_price
                else 0
            end as gain,
            case
                when close_price_validated < previous_close_price
                then previous_close_price - close_price_validated
                else 0
            end as loss,

            case
                when close_price_validated <= 0 or close_price_validated is null
                then 'INVALID'
                when day_high_validated < day_low_validated
                then 'INCONSISTENT'
                when
                    close_price_validated > day_high_validated
                    or close_price_validated < day_low_validated
                then 'INCONSISTENT'
                else 'VALID'
            end as data_quality_status

        from validated
    )

select *
from enriched

    -- Only update/insert the most recent date to avoid bloating the merge
    where quote_date >= (select min(quote_date) from daily_summary_from_today)

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.unique_int_id = DBT_INTERNAL_DEST.unique_int_id))

    
    when matched then update set
        `unique_int_id` = DBT_INTERNAL_SOURCE.`unique_int_id`,`symbol` = DBT_INTERNAL_SOURCE.`symbol`,`quote_date` = DBT_INTERNAL_SOURCE.`quote_date`,`open_price_validated` = DBT_INTERNAL_SOURCE.`open_price_validated`,`day_high_validated` = DBT_INTERNAL_SOURCE.`day_high_validated`,`day_low_validated` = DBT_INTERNAL_SOURCE.`day_low_validated`,`close_price_validated` = DBT_INTERNAL_SOURCE.`close_price_validated`,`previous_close_price` = DBT_INTERNAL_SOURCE.`previous_close_price`,`ingest_timestamp` = DBT_INTERNAL_SOURCE.`ingest_timestamp`,`data_source` = DBT_INTERNAL_SOURCE.`data_source`,`daily_price_change` = DBT_INTERNAL_SOURCE.`daily_price_change`,`daily_price_change_percentage` = DBT_INTERNAL_SOURCE.`daily_price_change_percentage`,`gain` = DBT_INTERNAL_SOURCE.`gain`,`loss` = DBT_INTERNAL_SOURCE.`loss`,`data_quality_status` = DBT_INTERNAL_SOURCE.`data_quality_status`
    

    when not matched then insert
        (`unique_int_id`, `symbol`, `quote_date`, `open_price_validated`, `day_high_validated`, `day_low_validated`, `close_price_validated`, `previous_close_price`, `ingest_timestamp`, `data_source`, `daily_price_change`, `daily_price_change_percentage`, `gain`, `loss`, `data_quality_status`)
    values
        (`unique_int_id`, `symbol`, `quote_date`, `open_price_validated`, `day_high_validated`, `day_low_validated`, `close_price_validated`, `previous_close_price`, `ingest_timestamp`, `data_source`, `daily_price_change`, `daily_price_change_percentage`, `gain`, `loss`, `data_quality_status`)


    