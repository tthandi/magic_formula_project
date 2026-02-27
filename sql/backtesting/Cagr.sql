WITH base AS (
  SELECT
    nav_date,
    nav,
    portfolio_return
  FROM `${PROJECT_ID}.magic_formula.bt_portfolio_nav_d3_norebuy`
),

bounds AS (
  SELECT
    MIN(nav_date) AS start_date,
    MAX(nav_date) AS end_date,
    COUNT(*) AS n_months
  FROM base
),

nav_start_end AS (
  SELECT
    FIRST_VALUE(nav) OVER (ORDER BY nav_date) AS nav_start,
    LAST_VALUE(nav)  OVER (
      ORDER BY nav_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS nav_end
  FROM base
  QUALIFY ROW_NUMBER() OVER (ORDER BY nav_date) = 1
),

stats AS (
  SELECT
    AVG(portfolio_return)    AS avg_monthly_return,
    STDDEV(portfolio_return) AS monthly_vol
  FROM base
)

SELECT
  b.start_date,
  b.end_date,
  n.nav_start,
  n.nav_end,
  b.n_months,

  -- CAGR
  POW(n.nav_end / n.nav_start, 12.0 / b.n_months) - 1 AS cagr,

  -- Annualized volatility
  s.monthly_vol * SQRT(12) AS annual_vol,

  -- Sharpe (rf = 0)
  SAFE_DIVIDE(
    (s.avg_monthly_return * 12),
    (s.monthly_vol * SQRT(12))
  ) AS sharpe

FROM bounds b
CROSS JOIN nav_start_end n
CROSS JOIN stats s;
