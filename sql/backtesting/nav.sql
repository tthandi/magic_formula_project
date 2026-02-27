-- ============================================================
-- PORTFOLIO NAV CONSTRUCTION (NO correlated subqueries)
-- - Anchors: next trading day after each qdate (JOIN + MIN)
-- - Holdings: buy_date <= period_start < sell_date
-- - Prices: last adj_close ON OR BEFORE period_start/period_end
--          (JOIN + QUALIFY ROW_NUMBER)
-- - Portfolio return: equal-weight avg across active holdings
-- - NAV: compounded from 1.0
-- ============================================================

DECLARE start_qdate DATE DEFAULT DATE '2006-01-31';
DECLARE end_qdate   DATE DEFAULT DATE '2025-01-31';

-- 0) Trading calendar (all trading dates we have)
CREATE TEMP TABLE cal AS
SELECT DISTINCT date
FROM `${PROJECT_ID}.magic_formula.daily_price`;

-- 1) Monthly qdates (strategy months)
CREATE TEMP TABLE qdates AS
SELECT qdate
FROM `${PROJECT_ID}.magic_formula.bt_monthly_picks_d3_norebuy`
WHERE qdate BETWEEN start_qdate AND end_qdate;

-- 2) Anchor date = next trading day after each qdate (JOIN + MIN)
CREATE TEMP TABLE anchors AS
SELECT
  q.qdate,
  MIN(c.date) AS anchor_date
FROM qdates q
JOIN cal c
  ON c.date > q.qdate
GROUP BY q.qdate;

-- 3) Periods: start anchor -> next anchor
CREATE TEMP TABLE periods AS
SELECT
  qdate,
  anchor_date AS period_start,
  LEAD(anchor_date) OVER (ORDER BY anchor_date) AS period_end
FROM anchors
QUALIFY period_end IS NOT NULL;

-- 4) Holdings active at each period_start
CREATE TEMP TABLE holdings AS
SELECT
  p.period_start,
  p.period_end,
  t.symbol
FROM periods p
JOIN `${PROJECT_ID}.magic_formula.bt_trades_d3_norebuy` t
  ON t.buy_date <= p.period_start
 AND t.sell_date >  p.period_start;

-- 5a) Price at period_start: last adj_close on/before period_start
CREATE TEMP TABLE px_start AS
SELECT
  h.period_start,
  h.period_end,
  h.symbol,
  dp.adj_close AS px_start
FROM holdings h
JOIN `${PROJECT_ID}.magic_formula.daily_price` dp
  ON dp.symbol = h.symbol
 AND dp.date <= h.period_start
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY h.period_start, h.period_end, h.symbol
  ORDER BY dp.date DESC
) = 1;

-- 5b) Price at period_end: last adj_close on/before period_end
CREATE TEMP TABLE px_end AS
SELECT
  h.period_start,
  h.period_end,
  h.symbol,
  dp.adj_close AS px_end
FROM holdings h
JOIN `${PROJECT_ID}.magic_formula.daily_price` dp
  ON dp.symbol = h.symbol
 AND dp.date <= h.period_end
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY h.period_start, h.period_end, h.symbol
  ORDER BY dp.date DESC
) = 1;

-- 6) Stock returns per holding per period
CREATE TEMP TABLE holding_returns AS
SELECT
  s.period_start,
  s.period_end,
  s.symbol,
  s.px_start,
  e.px_end,
  SAFE_DIVIDE(e.px_end, s.px_start) - 1 AS stock_return
FROM px_start s
JOIN px_end e
  ON e.period_start = s.period_start
 AND e.period_end   = s.period_end
 AND e.symbol       = s.symbol
WHERE s.px_start IS NOT NULL
  AND e.px_end   IS NOT NULL;

-- 7) Portfolio monthly returns (equal-weight across active holdings)
CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.bt_nav_period_returns_d3_norebuy` AS
SELECT
  period_start,
  period_end,
  COUNT(*) AS n_holdings_priced,
  AVG(stock_return) AS portfolio_return,
  MIN(stock_return) AS min_stock_return,
  MAX(stock_return) AS max_stock_return,
  APPROX_QUANTILES(stock_return, 5) AS return_quintiles
FROM holding_returns
GROUP BY period_start, period_end
ORDER BY period_start;

-- 8) NAV (compounded)
CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.bt_portfolio_nav_d3_norebuy` AS
WITH r AS (
  SELECT
    period_end AS nav_date,
    portfolio_return
  FROM `${PROJECT_ID}.magic_formula.bt_nav_period_returns_d3_norebuy`
),
nav AS (
  SELECT
    nav_date,
    portfolio_return,
    EXP(SUM(LN(1 + portfolio_return)) OVER (ORDER BY nav_date)) AS nav
  FROM r
)
SELECT *
FROM nav
ORDER BY nav_date;
