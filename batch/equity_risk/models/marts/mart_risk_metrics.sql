{{ config(
materialized='table'
,
schema='MARTS'
) }}
WITH daily_returns AS (
SELECT * FROM {{ ref('int_stock_daily') }}
WHERE daily_return IS NOT NULL
),
risk_calc AS (
SELECT
symbol,
COUNT(*) as trading_days,
AVG(daily_return) AS avg_daily_return,
STDDEV(daily_return) AS daily_volatility,
STDDEV(daily_return) * SQRT(252) AS annual_volatility, 
MIN(daily_return) AS max_daily_loss,
MAX(daily_return) AS max_daily_gain,
PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) AS var_95
FROM daily_returns
GROUP BY symbol
)
SELECT
symbol,
trading_days,
ROUND(avg_daily_return * 100, 4) AS avg_daily_return_pct,
ROUND(daily_volatility * 100, 4) AS daily_volatility_pct,
ROUND(annual_volatility * 100, 4) AS annual_volatility_pct,
ROUND(max_daily_loss * 100, 4) AS max_daily_loss_pct,
ROUND(max_daily_gain *100, 4) AS max_daily_gain_pct,
ROUND(var_95 * 100, 4) AS var_95_pct
FROM risk_calc
ORDER BY annual_volatility_pct DESC
