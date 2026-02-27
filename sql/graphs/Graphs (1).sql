WITH strat AS (
  SELECT
    nav_date,
    nav AS strat_nav
  FROM magic_formula.bt_portfolio_nav_raw_norebuy
),

bench AS (
  SELECT
    DATE_TRUNC(date, MONTH) AS nav_date,
    LAST_VALUE(adj_close) OVER (
      PARTITION BY DATE_TRUNC(date, MONTH)
      ORDER BY date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS bench_nav
  FROM magic_formula.benchmark_daily_price
)

SELECT
  s.nav_date,
  s.strat_nav,
  b.bench_nav
FROM strat s
JOIN bench b USING (nav_date)
ORDER BY nav_date;
