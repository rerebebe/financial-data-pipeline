-- stg_stock_quotes.sql
with
    source as (select * from {{ source('stock_quotes_bigquery', 'raw_quotes') }}),

    -- 12 fields
    renamed as (
        select
            -- noqa: ST06
            json_value(data, '$.symbol') as symbol,
            date(
                safe.timestamp_seconds(safe_cast(json_value(data, '$.t') as int64))
            ) as quote_date,
            safe.timestamp_seconds(
                safe_cast(json_value(data, '$.t') as int64)
            ) as quote_timestamp,
            cast(json_value(data, '$.o') as float64) as open_price,
            cast(json_value(data, '$.h') as float64) as day_high,
            cast(json_value(data, '$.l') as float64) as day_low,
            cast(json_value(data, '$.c') as float64) as current_price,
            cast(json_value(data, '$.d') as float64) as daily_price_change,
            cast(json_value(data, '$.dp') as float64) as daily_price_change_percentage,
            cast(json_value(data, '$.pc') as float64) as previous_close_price,

            -- current API response
            safe.timestamp_seconds(
                safe_cast(json_value(data, '$.ingest_time') as int64)
            ) as ingest_timestamp,
            json_value(data, '$.data_source') as data_source
        {# current_date() as test_test #}
        from source
    )

select *
from renamed
{# quote_date = current_date('America/New_York')  -- cuz we track american stocks #}
where symbol is not null and current_price is not null and quote_timestamp is not null

-- In case of multiple records for the same symbol and timestamp, we keep the one with
-- the latest ingest_timestamp
qualify
    row_number() over (
        partition by symbol, quote_timestamp order by ingest_timestamp desc
    )
    = 1
