CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.market_magic_formula_values_no_exclusions` AS
WITH
-- 1) Balance sheet
bs AS (
  SELECT
    symbol AS bs_symbol,
    fiscalDateEnding,
    totalCurrentAssets,
    cashAndCashEquivalentsAtCarryingValue AS cash,
    totalCurrentLiabilities,
    currentDebt,
    shortTermDebt,
    longTermDebt,
    longTermDebtNoncurrent,
    shortLongTermDebtTotal,
    propertyPlantEquipment,
    totalAssets,
    goodwill
  FROM `fluid-terminal-465516-s7.magic_formula.balance_sheet`
),

-- 2) Income statement
is_q AS (
  SELECT
    symbol AS is_symbol,
    fiscalDateEnding,
    COALESCE(ebit, operatingIncome) AS ebit
  FROM `fluid-terminal-465516-s7.magic_formula.income_statement`
),

-- 3) Latest company overview per symbol
co_latest AS (
  SELECT
    symbol AS co_symbol,
    peRatio,
    sector,
    industry,
    country
  FROM (
    SELECT
      symbol,
      peRatio,
      sector,
      industry,
      country,
      yyyymm,
      ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY yyyymm DESC) AS rn
    FROM `fluid-terminal-465516-s7.magic_formula.company_overview`
  )
  WHERE rn = 1
),

-- 4) Fundamentals-anchored quarter universe + precompute lag cutoff
fin_q AS (
  SELECT
    bs.bs_symbol AS symbol,
    bs.fiscalDateEnding AS qdate,
    DATE_SUB(bs.fiscalDateEnding, INTERVAL 60 DAY) AS asof_cutoff_date,  -- ✅ precomputed (no subquery)
    bs.totalCurrentAssets,
    bs.cash,
    bs.totalCurrentLiabilities,
    bs.currentDebt,
    bs.shortTermDebt,
    bs.longTermDebt,
    bs.longTermDebtNoncurrent,
    bs.shortLongTermDebtTotal,
    bs.propertyPlantEquipment,
    bs.totalAssets,
    bs.goodwill,
    is_q.ebit,
    co_latest.peRatio,
    co_latest.sector,
    co_latest.industry,
    co_latest.country
  FROM bs
  JOIN is_q
    ON bs.bs_symbol = is_q.is_symbol
   AND bs.fiscalDateEnding = is_q.fiscalDateEnding
  LEFT JOIN co_latest
    ON bs.bs_symbol = co_latest.co_symbol
),

-- 5) Attach market cap snapshot = latest trading day <= (qdate - 60)
fin_with_mc AS (
  SELECT
    f.*,
    mc.snapshotDate AS mc_snapshotDate,
    mc.marketCap    AS marketCap_q,
    mc.adjustedClose,
    mc.commonStockSharesOutstanding
  FROM fin_q f
  LEFT JOIN `fluid-terminal-465516-s7.magic_formula.market_cap_aligned` mc
    ON mc.symbol = f.symbol
   AND mc.snapshotDate <= f.asof_cutoff_date
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY f.symbol, f.qdate
      ORDER BY mc.snapshotDate DESC
    ) = 1
),

components AS (
  SELECT
    symbol,
    qdate,
    asof_cutoff_date,
    mc_snapshotDate,
    marketCap_q,
    adjustedClose,
    commonStockSharesOutstanding,
    ebit,
    totalCurrentAssets,
    cash,
    totalCurrentLiabilities,
    longTermDebt,
    propertyPlantEquipment,
    totalAssets,
    goodwill,
    peRatio,
    sector,
    industry,
    country,

    COALESCE(
      currentDebt,
      shortLongTermDebtTotal,
      shortTermDebt + COALESCE(longTermDebt - longTermDebtNoncurrent, 0)
    ) AS currentDebt_calc
  FROM fin_with_mc
),

calc AS (
  SELECT
    *,
    (totalCurrentAssets - cash)
      - (totalCurrentLiabilities - currentDebt_calc) AS nwc,
    COALESCE(
      propertyPlantEquipment,
      totalAssets - totalCurrentAssets - goodwill
    ) AS nfa
  FROM components
),

metrics AS (
  SELECT
    *,
    (marketCap_q
      + COALESCE(currentDebt_calc, 0)
      + COALESCE(longTermDebt, 0)
      - cash
    ) AS ev
  FROM calc
),

final AS (
  SELECT
    *,
    SAFE_DIVIDE(ebit, (nwc + nfa)) AS roc,
    SAFE_DIVIDE(ebit, ev)         AS ey
  FROM metrics
)

SELECT
  symbol,
  qdate,
  asof_cutoff_date,
  mc_snapshotDate,
  roc,
  ey,
  nwc,
  nfa,
  ev,
  ebit,
  marketCap_q AS marketCap,
  currentDebt_calc AS currentDebt,
  peRatio,
  sector,
  industry,
  country,
  adjustedClose,
  commonStockSharesOutstanding
FROM final
ORDER BY symbol, qdate;
