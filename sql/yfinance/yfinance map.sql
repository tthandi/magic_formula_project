CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.symbol_yf_map` AS
SELECT
  symbol,
  -- common US class share format conversion
  REPLACE(symbol, '.', '-') AS yf_ticker
FROM (
  SELECT DISTINCT symbol FROM `fluid-terminal-465516-s7.magic_formula.bt_missing_buy_windows_d3`
);