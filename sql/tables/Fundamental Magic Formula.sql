  CREATE OR REPLACE TABLE `fluid-terminal-465516-s7.magic_formula.fundamental_magic_formula_values` AS
WITH
-- 1) Balance sheet (quarterly + annual already in your table)
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

-- 2) Income statement (prefer ebit; fallback to operatingIncome)
is_q AS (
  SELECT
    symbol AS is_symbol,
    fiscalDateEnding,
    COALESCE(ebit, operatingIncome) AS ebit
  FROM `fluid-terminal-465516-s7.magic_formula.income_statement`
),

-- 3) Latest company overview per symbol (metadata only)
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

-- 4) Fundamentals-anchored quarter universe (this is the BASE)
fin_q AS (
  SELECT
    bs.bs_symbol AS symbol,
    bs.fiscalDateEnding AS qdate,           -- ✅ anchor on fiscal quarter end
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

-- 5) Attach market cap snapshot = latest trading day <= qdate
--    ✅ This prevents weekend/holiday quarter ends from being dropped.
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
   AND mc.snapshotDate <= f.qdate
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

    -- reconstructed currentDebt with sensible fallbacks
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
    -- Net Working Capital (operating)
    (totalCurrentAssets - cash)
      - (totalCurrentLiabilities - currentDebt_calc) AS nwc,

    -- Net Fixed Assets
    COALESCE(
      propertyPlantEquipment,
      totalAssets - totalCurrentAssets - goodwill
    ) AS nfa
  FROM components
),

metrics AS (
  SELECT
    *,
    -- Enterprise Value = Equity + Debt - Cash
    -- (If marketCap_q is NULL, EV will be NULL; we keep the row for completeness.)
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
WHERE
  marketCap_q > 50000000                      -- market cap > $50M
  AND peRatio > 5                             -- P/E > 5
  AND roc IS NOT NULL
  AND ey  IS NOT NULL
  AND LOWER(sector)   NOT LIKE '%financial%'  -- exclude financials
  AND LOWER(sector)   NOT LIKE '%utility%'    -- exclude utilities
  AND LOWER(industry) NOT LIKE '%financial%'
  AND LOWER(industry) NOT LIKE '%utility%'
ORDER BY symbol, qdate;
