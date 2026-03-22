{{ config(
    materialized='table',
    schema='MARTS'
)}}

WITH daily_returns AS (
    SELECT
        symbol,
        date,
        daily_return
    FROM {{ ref('int_stock_daily') }}
    WHERE daily_return IS NOT NULL
),

pivoted AS (
    SELECT
        date,
        MAX(CASE WHEN symbol = 'NWG.L' THEN daily_return END) AS NWG_L,
        MAX(CASE WHEN symbol = 'ABDN.L' THEN daily_return END) AS ABDN_L,
        MAX(CASE WHEN symbol = 'SMT.L' THEN daily_return END) AS SMT_L,
        MAX(CASE WHEN symbol = 'MNKS.L' THEN daily_return END) AS MNKS_L,
        MAX(CASE WHEN symbol = 'AV.L' THEN daily_return END) AS AV_L,
        MAX(CASE WHEN symbol = 'HIK.L' THEN daily_return END) AS HIK_L,
        MAX(CASE WHEN symbol = 'SSE.L' THEN daily_return END) AS SSE_L,
        MAX(CASE WHEN symbol = 'WEIR.L' THEN daily_return END) AS WEIR_L
    FROM daily_returns
    GROUP BY date
)

SELECT
   'NWG.L' AS symbol_1, 'ABDN.L' AS symbol_2, ROUND(CORR(NWG_L,ABDN_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'NWG.L', 'SMT.L', ROUND(CORR(NWG_L, SMT_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'NWG.L', 'MNKS.L', ROUND(CORR(NWG_L, MNKS_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'NWG.L', 'AV.L', ROUND(CORR(NWG_L, AV_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'NWG.L', 'HIK.L', ROUND(CORR(NWG_L, HIK_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'NWG.L', 'SSE.L', ROUND(CORR(NWG_L, SSE_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'NWG.L', 'WEIR.L', ROUND(CORR(NWG_L, WEIR_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'ABDN.L', 'SMT.L', ROUND(CORR(ABDN_L, SMT_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'ABDN.L', 'MNKS.L', ROUND(CORR(ABDN_L, MNKS_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'ABDN.L', 'AV.L', ROUND(CORR(ABDN_L, AV_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'ABDN.L', 'HIK.L', ROUND(CORR(ABDN_L, HIK_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'ABDN.L', 'SSE.L', ROUND(CORR(ABDN_L, SSE_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'ABDN.L', 'WEIR.L', ROUND(CORR(ABDN_L, WEIR_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'SMT.L', 'MNKS.L', ROUND(CORR(SMT_L, MNKS_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'SMT.L', 'AV.L', ROUND(CORR(SMT_L, AV_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'SMT.L', 'HIK.L', ROUND(CORR(SMT_L, HIK_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'SMT.L', 'SSE.L', ROUND(CORR(SMT_L, SSE_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'SMT.L', 'WEIR.L', ROUND(CORR(SMT_L, WEIR_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'MNKS.L', 'AV.L', ROUND(CORR(MNKS_L, AV_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'MNKS.L', 'HIK.L', ROUND(CORR(MNKS_L, HIK_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'MNKS.L', 'SSE.L', ROUND(CORR(MNKS_L, SSE_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'MNKS.L', 'WEIR.L', ROUND(CORR(MNKS_L, WEIR_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'AV.L', 'HIK.L', ROUND(CORR(AV_L, HIK_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'AV.L', 'SSE.L', ROUND(CORR(AV_L, SSE_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'AV.L', 'WEIR.L', ROUND(CORR(AV_L, WEIR_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'HIK.L', 'SSE.L', ROUND(CORR(HIK_L, SSE_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'HIK.L', 'WEIR.L', ROUND(CORR(HIK_L, WEIR_L), 4) AS correlation FROM pivoted UNION ALL
SELECT
   'SSE.L', 'WEIR.L', ROUND(CORR(SSE_L, WEIR_L), 4) AS correlation FROM pivoted
ORDER BY correlation DESC