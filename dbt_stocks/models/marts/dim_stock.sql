-- dim_stocks.sql
{{ config(enabled=false, materialized='table') }}


-- only if later on we need to enrich with stock metadata (e.g. company name, sector
-- etc.)
