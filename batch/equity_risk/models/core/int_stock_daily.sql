{{ config(
materialized='table',
schema='CORE'
) }}
SELECT
date,
symbol,
close,
LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
ROUND(
(close - LAG(close) OVER (PARTITION BY symbol ORDER BY date))
/ NULLIF(LAG(close) OVER (PARTITION BY symbol ORDER BY date), 0), 6) AS daily_return,
open,
high,
low,
volume,
FROM {{ ref('stg_stock_prices') }}
