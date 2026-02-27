WITH strat AS (
  SELECT
    period_end AS dt,
    portfolio_return AS strat_return
  FROM `fluid-terminal-465516-s7.magic_formula.bt_nav_period_returns_raw_norebuy`
),

bench_px AS (
  SELECT
    s.dt,
    b.adj_close AS bench_adj
  FROM strat s
  JOIN `fluid-terminal-465516-s7.magic_formula.benchmark_daily_price` b
    ON b.symbol = 'SP500TR'
   AND b.date <= s.dt
  QUALIFY ROW_NUMBER() OVER (PARTITION BY s.dt ORDER BY b.date DESC) = 1
),

bench AS (
  SELECT
    dt,
    SAFE_DIVIDE(bench_adj, LAG(bench_adj) OVER (ORDER BY dt)) - 1 AS bench_return
  FROM bench_px
),

joined AS (
  SELECT
    s.dt,
    s.strat_return,
    b.bench_return
  FROM strat s
  JOIN bench b USING (dt)
  WHERE b.bench_return IS NOT NULL
),

moments AS (
  SELECT
    COUNT(*) AS n_periods,
    AVG(strat_return) AS mean_strat,
    AVG(bench_return) AS mean_bench,
    COVAR_SAMP(strat_return, bench_return) AS covar,
    VAR_SAMP(bench_return) AS var_bench,
    CORR(strat_return, bench_return) AS corr
  FROM joined
)

SELECT
  n_periods,
  SAFE_DIVIDE(covar, var_bench) AS beta,
  (mean_strat - SAFE_DIVIDE(covar, var_bench) * mean_bench) AS alpha_per_period,
  POW(1 + (mean_strat - SAFE_DIVIDE(covar, var_bench) * mean_bench), 12) - 1 AS alpha_annualized,
  corr,
  POW(corr, 2) AS r_squared
FROM moments;
