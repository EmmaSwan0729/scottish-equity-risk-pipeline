{{config(
    materialized='view',
    schema='STAGING'
)}}

SELECT
    date,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    ingested_at
FROM {{ source('raw', 'raw_stock_prices')}}