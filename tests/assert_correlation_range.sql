-- Fails if any correlation value is outside [-1, 1]
SELECT *
FROM {{ ref('mart_correlation') }}
WHERE correlation < -1 OR correlation > 1