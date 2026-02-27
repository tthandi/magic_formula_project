DECLARE formation_date DATE DEFAULT DATE '2020-01-31';
DECLARE top_n INT64 DEFAULT 30;

WITH
-- 1) Rank stocks at formation date (Magic Formula style)
ranked_universe AS (
  SELECT
    symbol,
    qdate AS formation_date,
    ey,
    roc,
    ROW_NUMBER() OVER (
      ORDER BY ey DESC, roc DESC, symbol
    ) AS mf_rank
  FROM `fluid-terminal-465516-s7.magic_formula.market_magic_formula_values_with_exclusions`
  WHERE qdate = formation_date
),

-- 2) Take top N
universe AS (
  SELECT *
  FROM ranked_universe
  WHERE mf_rank <= top_n
),

-- 3) Buy date = first trading day AFTER formation date
buy_dates AS (
  SELECT
    u.symbol,
    u.mf_rank,
    u.formation_date,
    MIN(dp.date) AS buy_date
  FROM universe u
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price` dp
    ON dp.symbol = u.symbol
   AND dp.date > u.formation_date
  GROUP BY u.symbol, u.mf_rank, u.formation_date
),

-- 4) Sell date = first trading day ON/AFTER formation date + 1 year
sell_dates AS (
  SELECT
    b.symbol,
    b.mf_rank,
    b.formation_date,
    b.buy_date,
    MIN(dp.date) AS sell_date
  FROM buy_dates b
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price` dp
    ON dp.symbol = b.symbol
   AND dp.date >= DATE_ADD(b.formation_date, INTERVAL 1 YEAR)
  GROUP BY b.symbol, b.mf_rank, b.formation_date, b.buy_date
),

-- 5) Pull prices
priced AS (
  SELECT
    s.symbol,
    s.mf_rank,
    s.buy_date,
    s.sell_date,
    bp.adj_close AS buy_px,
    sp.adj_close AS sell_px,
    SAFE_DIVIDE(sp.adj_close, bp.adj_close) - 1 AS stock_return
  FROM sell_dates s
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price` bp
    ON bp.symbol = s.symbol AND bp.date = s.buy_date
  JOIN `fluid-terminal-465516-s7.magic_formula.daily_price` sp
    ON sp.symbol = s.symbol AND sp.date = s.sell_date
)

-- 6) Portfolio-level result
SELECT
  formation_date,
  COUNT(*) AS positions_filled,
  AVG(stock_return) AS equal_weight_return,
  MIN(stock_return) AS min_stock_return,
  MAX(stock_return) AS max_stock_return,
  APPROX_QUANTILES(stock_return, 5) AS return_quintiles
FROM priced
GROUP BY formation_date;
