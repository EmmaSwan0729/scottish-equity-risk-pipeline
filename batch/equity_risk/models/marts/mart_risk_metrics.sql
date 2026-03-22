{{ config(
materialized='table',
schema='MARTS'
) }}
WITH daily_returns AS (
SELECT * FROM {{ ref('int_stock_daily') }}
WHERE daily_return IS NOT NULL
),

drawdown_calc AS (
    SELECT
        symbol,
        date,
        close,
        MAX(close) OVER (
            PARTITION BY symbol
            ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS rolling_max
    FROM daily_returns
),
max_drawdown AS (
    SELECT
        symbol,
        MAX((rolling_max - close) / NULLIF(rolling_max, 0)) AS max_drawdown
    FROM drawdown_calc
    GROUP BY symbol
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
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) AS var_95,
    AVG(daily_return) / NULLIF(STDDEV(daily_return), 0) * SQRT(252) AS sharpe_ratio
FROM daily_returns
GROUP BY symbol
)

SELECT
    r.symbol,
    r.trading_days,
    ROUND(r.avg_daily_return * 100, 4) AS avg_daily_return_pct,
    ROUND(r.daily_volatility * 100, 4) AS daily_volatility_pct,
    ROUND(r.annual_volatility * 100, 4) AS annual_volatility_pct,
    ROUND(r.max_daily_loss * 100, 4) AS max_daily_loss_pct,
    ROUND(r.max_daily_gain *100, 4) AS max_daily_gain_pct,
    ROUND(r.var_95 * 100, 4) AS var_95_pct,
    ROUND(r.sharpe_ratio, 4) AS sharpe_ratio,
    ROUND(d.max_drawdown * 100, 4) AS max_drawdown_pct
FROM risk_calc r
LEFT JOIN max_drawdown d ON r.symbol = d.symbol
ORDER BY annual_volatility_pct DESC
