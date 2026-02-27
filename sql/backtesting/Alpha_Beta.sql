WITH months AS (
  SELECT month
  FROM UNNEST(GENERATE_DATE_ARRAY('2006-01-01', '2025-12-01', INTERVAL 1 MONTH)) AS month
),

active_holdings AS (
  SELECT
    m.month,
    COUNT(DISTINCT t.symbol) AS holdings_count
  FROM months m
  LEFT JOIN `${PROJECT_ID}.bt_trades_d3_norebuy` t
    ON DATE_TRUNC(t.buy_date, MONTH) <= m.month
   AND DATE_TRUNC(t.sell_date, MONTH) > m.month
  GROUP BY m.month
),

expo AS (
  SELECT
    month,
    LEAST(1.0, SAFE_DIVIDE(holdings_count, 30.0)) AS exposure
  FROM active_holdings
),

strat AS (
  SELECT
    DATE_TRUNC(period_end, MONTH) AS month,
    portfolio_return AS strat_return
  FROM `${PROJECT_ID}.magic_formula.bt_nav_period_returns_d3_norebuy`
),

bench AS (
  SELECT
    DATE_TRUNC(nav_date, MONTH) AS month,
    bench_return
  FROM `${PROJECT_ID}.magic_formula.sp500_monthly_returns`
),

joined AS (
  SELECT
    s.month,
    s.strat_return,
    (e.exposure * b.bench_return) AS bench_return_adj
  FROM strat s
  JOIN bench b USING (month)
  JOIN expo  e USING (month)
),

moments AS (
  SELECT
    COUNT(*) AS n_months,
    AVG(strat_return) AS mean_strat,
    AVG(bench_return_adj) AS mean_bench_adj,
    COVAR_SAMP(strat_return, bench_return_adj) AS covar,
    VAR_SAMP(bench_return_adj) AS var_bench_adj,
    CORR(strat_return, bench_return_adj) AS corr
  FROM joined
)

SELECT
  n_months,
  SAFE_DIVIDE(covar, var_bench_adj) AS beta_adj,
  (mean_strat - SAFE_DIVIDE(covar, var_bench_adj) * mean_bench_adj) AS alpha_monthly,
  12 * (mean_strat - SAFE_DIVIDE(covar, var_bench_adj) * mean_bench_adj) AS alpha_annual,
  corr,
  POW(corr, 2) AS r_squared
FROM moments;
