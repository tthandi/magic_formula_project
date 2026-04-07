CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.bt_missing_sell_windows_d3` AS
WITH picks AS (
  SELECT qdate, pick1 AS symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick2 FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick3 FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  WHERE pick3 IS NOT NULL
),

-- Buy date per pick (strict: within 7 days)
buy_dates AS (
  SELECT
    p.qdate AS formation_date,
    p.symbol,
    MIN(dp.date) AS buy_date
  FROM picks p
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price_combined` dp
    ON dp.symbol = p.symbol
   AND dp.date > p.qdate
   AND dp.date <= DATE_ADD(p.qdate, INTERVAL 7 DAY)
  GROUP BY formation_date, symbol
),

-- Define required sell window: [buy_date+1y, buy_date+1y+7d]
sell_need AS (
  SELECT
    formation_date,
    symbol,
    buy_date,
    DATE_ADD(buy_date, INTERVAL 1 YEAR) AS need_start,
    DATE_ADD(DATE_ADD(buy_date, INTERVAL 1 YEAR), INTERVAL 7 DAY) AS need_end
  FROM buy_dates
  WHERE buy_date IS NOT NULL
),

-- Check if ANY price exists in that sell window
sell_found AS (
  SELECT
    s.*,
    MIN(dp.date) AS first_sell_px_date
  FROM sell_need s
  LEFT JOIN `fluid-terminal-465516-s7.magic_formula.daily_price_combined` dp
    ON dp.symbol = s.symbol
   AND dp.date >= s.need_start
   AND dp.date <= s.need_end
  GROUP BY formation_date, symbol, buy_date, need_start, need_end
)

SELECT
  formation_date,
  symbol,
  buy_date,
  need_start,
  need_end
FROM sell_found
WHERE first_sell_px_date IS NULL
ORDER BY formation_date, symbol;