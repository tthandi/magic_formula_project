CREATE OR REPLACE TABLE `${PROJECT_ID}.magic_formula.market_cap_aligned` AS
WITH bs_dates AS (
  SELECT DISTINCT
    symbol,
    fiscalDateEnding
  FROM `${PROJECT_ID}.magic_formula.balance_sheet`
),
mc_candidates AS (
  SELECT
    b.symbol,
    b.fiscalDateEnding,
    m.snapshotDate,
    m.marketCap,
    m.adjustedClose,
    m.commonStockSharesOutstanding,
    ROW_NUMBER() OVER (
      PARTITION BY b.symbol, b.fiscalDateEnding
      ORDER BY m.snapshotDate DESC
    ) AS rn
  FROM bs_dates b
  LEFT JOIN `${PROJECT_ID}.magic_formula.market_cap` m
    ON m.symbol = b.symbol
   AND m.snapshotDate <= b.fiscalDateEnding
   AND m.snapshotDate >= DATE_SUB(b.fiscalDateEnding, INTERVAL 7 DAY)
)
SELECT
  symbol,
  fiscalDateEnding,
  snapshotDate,
  marketCap,
  adjustedClose,
  commonStockSharesOutstanding
FROM mc_candidates
WHERE rn = 1;
