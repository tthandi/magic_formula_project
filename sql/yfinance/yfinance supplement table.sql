CREATE TABLE IF NOT EXISTS `fluid-terminal-465516-s7.magic_formula.daily_price_yf` (
  symbol STRING,
  date DATE,
  adj_close FLOAT64,
  close FLOAT64,
  open FLOAT64,
  high FLOAT64,
  low FLOAT64,
  volume INT64,
  source STRING
)
PARTITION BY date
CLUSTER BY symbol;