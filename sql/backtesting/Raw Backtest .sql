-- ============================================================
-- FULL RAW BACKTEST (BigQuery Script)
-- Outputs:
-- 1) bt_monthly_picks_raw_norebuy
-- 2) bt_trades_raw_norebuy
-- 3) bt_nav_period_returns_raw_norebuy
-- 4) bt_portfolio_nav_raw_norebuy
-- ============================================================

-- ============
-- DECLARE (must be first)
-- ============
DECLARE start_qdate DATE DEFAULT DATE '2006-01-31';
DECLARE end_qdate   DATE DEFAULT DATE '2025-01-31';

-- RAW buys/month rule
-- - Always buy 2
-- - Optionally buy 3 if "close enough" to #2 on both EY and ROC
DECLARE allow_third BOOL DEFAULT TRUE;
DECLARE ey_close_pct  FLOAT64 DEFAULT 0.10;  -- within 10% of EY #2
DECLARE roc_close_pct FLOAT64 DEFAULT 0.10;  -- within 10% of ROC #2

-- loop helper vars (must be declared here too)
DECLARE ey2 FLOAT64;
DECLARE ey3 FLOAT64;
DECLARE roc2 FLOAT64;
DECLARE roc3 FLOAT64;
DECLARE desired_buys INT64;

-- ============================================================
-- 1) RAW monthly ranking universe
-- ============================================================

CREATE TEMP TABLE ranked_monthly_raw AS
SELECT
  qdate,
  symbol,
  ey,
  roc,
  ROW_NUMBER() OVER (
    PARTITION BY qdate
    ORDER BY ey DESC, roc DESC, symbol
  ) AS rnk
FROM `${PROJECT_ID}.magic_formula.market_magic_formula_values_with_exclusions`
WHERE qdate BETWEEN start_qdate AND end_qdate
  AND ey IS NOT NULL
  AND roc IS NOT NULL;

-- ============================================================
-- 2) Stateful monthly picks (no rebuy + replacement)
-- ============================================================

CREATE TEMP TABLE picks_long (
  qdate DATE,
  pick_order INT64,
  symbol STRING,
  ey FLOAT64,
  roc FLOAT64,
  raw_rnk INT64
);

FOR m IN (
  SELECT DISTINCT qdate
  FROM ranked_monthly_raw
  ORDER BY qdate
) DO

  -- default buys = 2
  SET desired_buys = 2;

  -- optional 3rd buy rule (RAW-based)
  IF allow_third THEN
    SET ey2  = (SELECT ey  FROM ranked_monthly_raw WHERE qdate = m.qdate AND rnk = 2);
    SET roc2 = (SELECT roc FROM ranked_monthly_raw WHERE qdate = m.qdate AND rnk = 2);

    SET ey3  = (SELECT ey  FROM ranked_monthly_raw WHERE qdate = m.qdate AND rnk = 3);
    SET roc3 = (SELECT roc FROM ranked_monthly_raw WHERE qdate = m.qdate AND rnk = 3);

    SET desired_buys = 2 + IF(
      ey3  >= ey2  * (1 - ey_close_pct)
      AND roc3 >= roc2 * (1 - roc_close_pct),
      1, 0
    );
  END IF;

  -- held symbols = picked within last year (still held)
  CREATE TEMP TABLE held AS
  SELECT DISTINCT symbol
  FROM picks_long
  WHERE qdate > DATE_SUB(m.qdate, INTERVAL 1 YEAR)
    AND qdate < m.qdate;

  -- select best available (skip held), fill desired_buys
  INSERT INTO picks_long (qdate, pick_order, symbol, ey, roc, raw_rnk)
  SELECT
    m.qdate,
    ROW_NUMBER() OVER (ORDER BY r.ey DESC, r.roc DESC, r.symbol) AS pick_order,
    r.symbol,
    r.ey,
    r.roc,
    r.rnk AS raw_rnk
  FROM ranked_monthly_raw r
  LEFT JOIN held h
    ON h.symbol = r.symbol
  WHERE r.qdate = m.qdate
    AND h.symbol IS NULL
  QUALIFY pick_order <= desired_buys;

  DROP TABLE held;

END FOR;

-- Persist monthly picks (wide)
CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.bt_monthly_picks_raw_norebuy` AS
SELECT
  qdate,
  MAX(IF(pick_order = 1, symbol, NULL)) AS pick1,
  MAX(IF(pick_order = 2, symbol, NULL)) AS pick2,
  MAX(IF(pick_order = 3, symbol, NULL)) AS pick3,
  COUNT(*) AS n_buys,

  MAX(IF(pick_order = 1, ey, NULL)) AS ey1,
  MAX(IF(pick_order = 2, ey, NULL)) AS ey2,
  MAX(IF(pick_order = 3, ey, NULL)) AS ey3,

  MAX(IF(pick_order = 1, roc, NULL)) AS roc1,
  MAX(IF(pick_order = 2, roc, NULL)) AS roc2,
  MAX(IF(pick_order = 3, roc, NULL)) AS roc3
FROM picks_long
GROUP BY qdate
ORDER BY qdate;

-- ============================================================
-- 3) Trades (buy/sell dates + 1Y returns)
-- ============================================================

CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.bt_trades_raw_norebuy` AS
WITH picks AS (
  SELECT qdate, pick1 AS symbol FROM `${PROJECT_ID}.magic_formula.bt_monthly_picks_raw_norebuy`
  UNION ALL
  SELECT qdate, pick2 AS symbol FROM `${PROJECT_ID}.magic_formula.bt_monthly_picks_raw_norebuy`
  UNION ALL
  SELECT qdate, pick3 AS symbol FROM `${PROJECT_ID}.magic_formula.bt_monthly_picks_raw_norebuy`
  WHERE pick3 IS NOT NULL
),
buy_dates AS (
  SELECT
    p.qdate AS formation_date,
    p.symbol,
    MIN(dp.date) AS buy_date
  FROM picks p
  JOIN `${PROJECT_ID}.magic_formula.daily_price` dp
    ON dp.symbol = p.symbol
   AND dp.date > p.qdate
  GROUP BY formation_date, symbol
),
sell_dates AS (
  SELECT
    b.formation_date,
    b.symbol,
    b.buy_date,
    MIN(dp.date) AS sell_date
  FROM buy_dates b
  JOIN `${PROJECT_ID}.magic_formula.daily_price` dp
    ON dp.symbol = b.symbol
   AND dp.date >= DATE_ADD(b.formation_date, INTERVAL 1 YEAR)
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
  JOIN `${PROJECT_ID}.magic_formula.daily_price` bp
    ON bp.symbol = s.symbol AND bp.date = s.buy_date
  JOIN `${PROJECT_ID}.magic_formula.daily_price` sp
    ON sp.symbol = s.symbol AND sp.date = s.sell_date
)
SELECT * FROM priced;

-- ============================================================
-- 4) NAV construction (no correlated subqueries)
-- ============================================================

-- Trading calendar
CREATE TEMP TABLE cal AS
SELECT DISTINCT date
FROM `${PROJECT_ID}.magic_formula.daily_price`;

-- Strategy months
CREATE TEMP TABLE qdates AS
SELECT qdate
FROM `${PROJECT_ID}.magic_formula.bt_monthly_picks_raw_norebuy`
WHERE qdate BETWEEN start_qdate AND end_qdate;

-- Anchor date = next trading day after qdate
CREATE TEMP TABLE anchors AS
SELECT
  q.qdate,
  MIN(c.date) AS anchor_date
FROM qdates q
JOIN cal c
  ON c.date > q.qdate
GROUP BY q.qdate;

-- Periods: anchor -> next anchor
CREATE TEMP TABLE periods AS
SELECT
  qdate,
  anchor_date AS period_start,
  LEAD(anchor_date) OVER (ORDER BY anchor_date) AS period_end
FROM anchors
QUALIFY period_end IS NOT NULL;

-- Holdings active at period_start
CREATE TEMP TABLE holdings AS
SELECT
  p.period_start,
  p.period_end,
  t.symbol
FROM periods p
JOIN `${PROJECT_ID}.magic_formula.bt_trades_raw_norebuy` t
  ON t.buy_date <= p.period_start
 AND t.sell_date >  p.period_start;

-- Price on/before period_start
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

-- Price on/before period_end
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

-- Per-holding returns per period
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

-- Portfolio monthly returns
CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.bt_nav_period_returns_raw_norebuy` AS
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

-- NAV
CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.bt_portfolio_nav_raw_norebuy` AS
WITH r AS (
  SELECT
    period_end AS nav_date,
    portfolio_return
  FROM `${PROJECT_ID}.magic_formula.bt_nav_period_returns_raw_norebuy`
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
