CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.bt_missing_buy_windows_d3` AS
WITH picks AS (
  SELECT qdate, pick1 AS symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick2 FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick3 FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  WHERE pick3 IS NOT NULL
),
buy_attempt AS (
  SELECT
    p.qdate,
    p.symbol,
    MIN(dp.date) AS buy_date
  FROM picks p
  LEFT JOIN `fluid-terminal-465516-s7.magic_formula.daily_price` dp
    ON dp.symbol = p.symbol
   AND dp.date > p.qdate
   AND dp.date <= DATE_ADD(p.qdate, INTERVAL 7 DAY)
  GROUP BY p.qdate, p.symbol
)
SELECT
  qdate,
  symbol,
  DATE_ADD(qdate, INTERVAL 1 DAY) AS need_start,
  DATE_ADD(qdate, INTERVAL 7 DAY) AS need_end
FROM buy_attempt
WHERE buy_date IS NULL;