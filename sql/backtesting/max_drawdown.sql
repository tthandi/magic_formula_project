WITH navs AS (
  SELECT
    nav_date,
    nav
  FROM `fluid-terminal-465516-s7.magic_formula.bt_portfolio_nav_d3_norebuy`
),

peaks AS (
  SELECT
    nav_date,
    nav,
    MAX(nav) OVER (ORDER BY nav_date) AS running_peak
  FROM navs
),

drawdowns AS (
  SELECT
    nav_date,
    nav,
    running_peak,
    nav / running_peak - 1 AS drawdown
  FROM peaks
)

SELECT
  MIN(drawdown) AS max_drawdown
FROM drawdowns;
