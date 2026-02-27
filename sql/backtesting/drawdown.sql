SELECT
  nav_date,
  nav,
  MAX(nav) OVER (ORDER BY nav_date) AS running_peak,
  nav / MAX(nav) OVER (ORDER BY nav_date) - 1 AS drawdown
FROM `fluid-terminal-465516-s7.magic_formula.bt_portfolio_nav_d3_norebuy`
ORDER BY nav_date;
