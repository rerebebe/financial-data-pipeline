-- stg_5_year_stock_price.sql
with
    source as (
        select * from {{ source('stock_quotes_bigquery', 'raw_5_year_stock_price') }}
    ),

    -- 9 fields
    renamed as (
        select
            symbol,
            date as quote_date,
            cast(open as float64) as open_price,
            cast(high as float64) as day_high,
            cast(low as float64) as day_low,
            cast(close as float64) as close_price,
            cast(volume as int64) as volume,
            cast(ingest_time as timestamp) as ingest_timestamp,
            data_source
        from source
    )

select *
from renamed
