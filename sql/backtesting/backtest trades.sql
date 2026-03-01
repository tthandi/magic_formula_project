CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.bt_trades_d3_norebuy` AS
WITH picks AS (
  SELECT qdate, pick1 AS symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick2 FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick3 FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  WHERE pick3 IS NOT NULL
),
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
sell_dates AS (
  SELECT
    b.formation_date,
    b.symbol,
    b.buy_date,
    MIN(dp.date) AS sell_date
  FROM buy_dates b
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price_combined` dp
    ON dp.symbol = b.symbol
   AND dp.date >= DATE_ADD(b.buy_date, INTERVAL 1 YEAR)
  GROUP BY b.formation_date, b.symbol, b.buy_date
),
priced AS (
  SELECT
    s.formation_date,
    s.symbol,
    s.buy_date,
    s.sell_date,
    bp.adj_close AS buy_adj,
    sp.adj_close AS sell_adj,
    SAFE_DIVIDE(sp.adj_close, bp.adj_close) - 1 AS stock_return
  FROM sell_dates s
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price_combined` bp
    ON bp.symbol = s.symbol AND bp.date = s.buy_date
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price_combined` sp
    ON sp.symbol = s.symbol AND sp.date = s.sell_date
)
SELECT *
FROM priced
WHERE buy_date IS NOT NULL
  AND sell_date IS NOT NULL
  AND sell_date > buy_date;