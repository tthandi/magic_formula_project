WITH bs AS (
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

is_q AS (
  SELECT
    symbol AS is_symbol,
    fiscalDateEnding,
    COALESCE(ebit, operatingIncome) AS ebit
  FROM `fluid-terminal-465516-s7.magic_formula.income_statement`
),

-- latest overview per symbol (for metadata; no filtering here)
co_latest AS (
  SELECT
    symbol    AS co_symbol,
    marketCap AS co_marketCap,
    peRatio,
    sector,
    industry,
    country
  FROM (
    SELECT
      symbol,
      marketCap,
      peRatio,
      sector,
      industry,
      country,
      yyyymm,
      ROW_NUMBER() OVER (
        PARTITION BY symbol
        ORDER BY yyyymm DESC
      ) AS rn
    FROM `fluid-terminal-465516-s7.magic_formula.company_overview`
  )
  WHERE rn = 1
),

-- quarter-level fundamentals: BS + IS + overview
fin_q AS (
  SELECT
    bs.bs_symbol AS symbol,
    bs.fiscalDateEnding,
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

-- attach the *latest quarter with fiscalDateEnding <= snapshotDate*
snapshots_with_fundamentals AS (
  SELECT
    mc.symbol,
    mc.snapshotDate AS qdate,
    mc.marketCap    AS marketCap_q,
    mc.adjustedClose,
    mc.commonStockSharesOutstanding,

    fin_q.fiscalDateEnding,
    fin_q.ebit,
    fin_q.totalCurrentAssets,
    fin_q.cash,
    fin_q.totalCurrentLiabilities,
    fin_q.currentDebt,
    fin_q.shortTermDebt,
    fin_q.longTermDebt,
    fin_q.longTermDebtNoncurrent,
    fin_q.shortLongTermDebtTotal,
    fin_q.propertyPlantEquipment,
    fin_q.totalAssets,
    fin_q.goodwill,
    fin_q.peRatio,
    fin_q.sector,
    fin_q.industry,
    fin_q.country,

    ROW_NUMBER() OVER (
      PARTITION BY mc.symbol, mc.snapshotDate
      ORDER BY fin_q.fiscalDateEnding DESC
    ) AS rn
  FROM `fluid-terminal-465516-s7.magic_formula.market_cap` AS mc
  JOIN fin_q
    ON mc.symbol = fin_q.symbol
   AND fin_q.fiscalDateEnding <= mc.snapshotDate
),

-- keep only the latest quarter per snapshot
mc_q AS (
  SELECT *
  FROM snapshots_with_fundamentals
  WHERE rn = 1
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
  FROM mc_q
),

calc AS (
  SELECT
    symbol,
    qdate,
    marketCap_q,
    adjustedClose,
    commonStockSharesOutstanding,
    ebit,
    peRatio,
    sector,
    industry,
    country,
    currentDebt_calc,
    longTermDebt,
    totalCurrentAssets,
    totalCurrentLiabilities,
    cash,
    propertyPlantEquipment,
    totalAssets,
    goodwill,

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
    -- Enterprise Value = Equity + Debt - Cash, using quarterly marketCap_q
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
ORDER BY
  symbol,
  qdate;
