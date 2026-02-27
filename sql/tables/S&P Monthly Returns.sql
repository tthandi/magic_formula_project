CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.sp500_monthly_returns` AS
WITH daily AS (
  SELECT
    DATE(date) AS date,
    CAST(adj_close AS FLOAT64) AS adj_close
  FROM `fluid-terminal-465516-s7.magic_formula.benchmark_daily_price`
  WHERE symbol = 'SP500TR'
    AND adj_close IS NOT NULL
),
month_end AS (
  SELECT
    DATE_TRUNC(date, MONTH) AS month,
    date,
    adj_close,
    ROW_NUMBER() OVER (
      PARTITION BY DATE_TRUNC(date, MONTH)
      ORDER BY date DESC
    ) AS rn
  FROM daily
),
month_end_px AS (
  SELECT
    month,
    date AS month_end_date,
    adj_close AS month_end_adj_close
  FROM month_end
  WHERE rn = 1
),
rets AS (
  SELECT
    month_end_date AS nav_date,
    SAFE_DIVIDE(
      month_end_adj_close,
      LAG(month_end_adj_close) OVER (ORDER BY month)
    ) - 1 AS bench_return
  FROM month_end_px
)
SELECT *
FROM rets
WHERE bench_return IS NOT NULL
ORDER BY nav_date;
