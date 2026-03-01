CREATE OR REPLACE VIEW `fluid-terminal-465516-s7.magic_formula.daily_price_combined` AS
WITH av AS (
  SELECT symbol, date, adj_close
  FROM `fluid-terminal-465516-s7.magic_formula.daily_price`
),
yf AS (
  SELECT symbol, date, adj_close
  FROM `fluid-terminal-465516-s7.magic_formula.daily_price_yf`
),
u AS (
  SELECT symbol, date, adj_close, 1 AS pri FROM av
  UNION ALL
  SELECT symbol, date, adj_close, 2 AS pri FROM yf
)
SELECT symbol, date, adj_close
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY pri) AS rn
  FROM u
)
WHERE rn = 1;