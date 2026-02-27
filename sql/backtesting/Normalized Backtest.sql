-- ============================================================
-- RULE D3 MONTHLY BACKTEST (STATEFUL PICKS WITH REPLACEMENT)
-- - score = z(ey) + z(roc) per qdate
-- - desired_buys = 2 + I(score3 >= score2 - delta)
-- - skip symbols currently held (bought within last 1 year)
-- - replace with next best until desired_buys filled
-- - trades: buy next trading day; sell first trading day on/after +1 YEAR
-- ============================================================

DECLARE delta FLOAT64 DEFAULT 0.25;
DECLARE start_qdate DATE DEFAULT DATE '2006-01-31';
DECLARE end_qdate   DATE DEFAULT DATE '2025-01-31';

DECLARE score2 FLOAT64;
DECLARE score3 FLOAT64;
DECLARE desired_buys INT64;

-- EDIT IF NEEDED:
DECLARE UNIVERSE_TABLE STRING DEFAULT 'fluid-terminal-465516-s7.magic_formula.market_magic_formula_values_with_exclusions';
DECLARE PRICE_TABLE    STRING DEFAULT 'fluid-terminal-465516-s7.magic_formula.daily_price';

-- ============================================================
-- 1) Precompute monthly scores + ranks for every qdate, symbol
-- ============================================================

EXECUTE IMMEDIATE FORMAT("""
CREATE TEMP TABLE ranked_monthly AS
WITH base AS (
  SELECT qdate, symbol, ey, roc
  FROM `%s`
  WHERE qdate BETWEEN @start_qdate AND @end_qdate
    AND ey IS NOT NULL
    AND roc IS NOT NULL
),
stats AS (
  SELECT
    b.*,
    AVG(ey)  OVER (PARTITION BY qdate) AS mu_ey,
    STDDEV_POP(ey)  OVER (PARTITION BY qdate) AS sd_ey,
    AVG(roc) OVER (PARTITION BY qdate) AS mu_roc,
    STDDEV_POP(roc) OVER (PARTITION BY qdate) AS sd_roc
  FROM base b
),
scored AS (
  SELECT
    qdate,
    symbol,
    SAFE_DIVIDE(ey  - mu_ey,  sd_ey) AS z_ey,
    SAFE_DIVIDE(roc - mu_roc, sd_roc) AS z_roc,
    SAFE_DIVIDE(ey  - mu_ey,  sd_ey) + SAFE_DIVIDE(roc - mu_roc, sd_roc) AS score
  FROM stats
),
ranked AS (
  SELECT
    qdate,
    symbol,
    score,
    ROW_NUMBER() OVER (PARTITION BY qdate ORDER BY score DESC, symbol) AS rnk
  FROM scored
)
SELECT * FROM ranked;
""", UNIVERSE_TABLE)
USING start_qdate AS start_qdate, end_qdate AS end_qdate;

-- ============================================================
-- 2) Stateful loop to generate picks_long (no rebuy + replacement)
-- ============================================================

CREATE TEMP TABLE picks_long (
  qdate DATE,
  pick_order INT64,
  symbol STRING,
  score FLOAT64
);

FOR m IN (
  SELECT DISTINCT qdate
  FROM ranked_monthly
  ORDER BY qdate
) DO

  -- Top-3 proximity test (Rule D3)
  SET score2 = (
    SELECT score FROM ranked_monthly
    WHERE qdate = m.qdate AND rnk = 2
  );

  SET score3 = (
    SELECT score FROM ranked_monthly
    WHERE qdate = m.qdate AND rnk = 3
  );

  SET desired_buys = 2 + IF(score3 >= score2 - delta, 1, 0);

  -- Held symbols: bought in prior 12 months (still held)
  CREATE TEMP TABLE held AS
  SELECT DISTINCT symbol
  FROM picks_long
  WHERE qdate > DATE_SUB(m.qdate, INTERVAL 1 YEAR)
    AND qdate < m.qdate;

  -- Pick best available candidates not held, fill desired_buys
  INSERT INTO picks_long (qdate, pick_order, symbol, score)
  SELECT
    m.qdate AS qdate,
    ROW_NUMBER() OVER (ORDER BY r.score DESC, r.symbol) AS pick_order,
    r.symbol,
    r.score
  FROM ranked_monthly r
  LEFT JOIN held h
    ON h.symbol = r.symbol
  WHERE r.qdate = m.qdate
    AND h.symbol IS NULL
  QUALIFY pick_order <= desired_buys;

  DROP TABLE held;

END FOR;

-- ============================================================
-- 3) Write monthly picks table (wide format)
-- ============================================================

CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy` AS
SELECT
  qdate,
  MAX(IF(pick_order = 1, symbol, NULL)) AS pick1,
  MAX(IF(pick_order = 2, symbol, NULL)) AS pick2,
  MAX(IF(pick_order = 3, symbol, NULL)) AS pick3,
  MAX(IF(pick_order = 1, score,  NULL)) AS score1,
  MAX(IF(pick_order = 2, score,  NULL)) AS score2,
  MAX(IF(pick_order = 3, score,  NULL)) AS score3,
  COUNT(*) AS n_buys
FROM picks_long
GROUP BY qdate
ORDER BY qdate;

-- ============================================================
-- 4) Convert picks into trades with buy/sell dates + returns
-- ============================================================

EXECUTE IMMEDIATE FORMAT("""
CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.bt_trades_d3_norebuy` AS
WITH picks AS (
  SELECT qdate, pick1 AS symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick2 AS symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  UNION ALL
  SELECT qdate, pick3 AS symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_monthly_picks_d3_norebuy`
  WHERE pick3 IS NOT NULL
),

buy_dates AS (
  SELECT
    p.qdate AS formation_date,
    p.symbol,
    MIN(dp.date) AS buy_date
  FROM picks p
  JOIN `%s` dp
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
  JOIN `%s` dp
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
  JOIN `%s` bp
    ON bp.symbol = s.symbol AND bp.date = s.buy_date
  JOIN `%s` sp
    ON sp.symbol = s.symbol AND sp.date = s.sell_date
)

SELECT *
FROM priced;
""", PRICE_TABLE, PRICE_TABLE, PRICE_TABLE, PRICE_TABLE);

-- ============================================================
-- 5) Cohort return series (each month’s buys held ~1 year)
-- ============================================================

CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.bt_cohort_returns_d3_norebuy` AS
SELECT
  formation_date,
  COUNT(*) AS n_buys,
  AVG(stock_return) AS cohort_return_1y,
  MIN(stock_return) AS min_stock_return,
  MAX(stock_return) AS max_stock_return,
  APPROX_QUANTILES(stock_return, 5) AS return_quintiles
FROM `fluid-terminal-465516-s7.magic_formula.bt_trades_d3_norebuy`
GROUP BY formation_date
ORDER BY formation_date;
