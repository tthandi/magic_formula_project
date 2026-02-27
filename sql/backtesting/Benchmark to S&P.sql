-- Benchmark returns aligned to RAW NAV dates
WITH nav_dates AS (
  SELECT nav_date
  FROM `${PROJECT_ID}.magic_formula.bt_portfolio_nav_raw_norebuy`
),
bench_px AS (
  SELECT
    n.nav_date,
    b.adj_close AS bench_adj
  FROM nav_dates n
  JOIN `${PROJECT_ID}.magic_formula.benchmark_daily_price` b
    ON b.symbol = 'SP500TR'
   AND b.date <= n.nav_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY n.nav_date ORDER BY b.date DESC) = 1
),
bench_ret AS (
  SELECT
    nav_date,
    SAFE_DIVIDE(bench_adj, LAG(bench_adj) OVER (ORDER BY nav_date)) - 1 AS bench_return
  FROM bench_px
)
SELECT *
FROM bench_ret
WHERE bench_return IS NOT NULL
ORDER BY nav_date;
