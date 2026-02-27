CREATE OR REPLACE TABLE
  `fluid-terminal-465516-s7.magic_formula.market_magic_formula_values_with_exclusions` AS

WITH base AS (
  SELECT *
  FROM `fluid-terminal-465516-s7.magic_formula.market_magic_formula_values_no_exclusions`
),

filtered AS (
  SELECT
    *
  FROM base
  WHERE
    -- ----------------------------
    -- 1) Core validity constraints
    -- ----------------------------
    capital_base_valid
    AND ev_valid
    AND marketcap_valid
    AND ebit IS NOT NULL
    AND roc IS NOT NULL
    AND ey  IS NOT NULL

    -- ----------------------------
    -- 2) Size / valuation filters
    -- ----------------------------
    AND marketCap >= 50000000
    AND peRatio IS NOT NULL
    AND peRatio > 5

    -- ----------------------------
    -- 3) Sector / industry exclusions
    -- ----------------------------
    AND (
      sector IS NULL
      OR (
        LOWER(sector)   NOT LIKE '%financial%'
        AND LOWER(sector) NOT LIKE '%utility%'
      )
    )
    AND (
      industry IS NULL
      OR (
        LOWER(industry) NOT LIKE '%financial%'
        AND LOWER(industry) NOT LIKE '%utility%'
      )
    )
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

  marketCap,
  currentDebt,

  peRatio,
  sector,
  industry,
  country,

  adjustedClose,
  commonStockSharesOutstanding
FROM filtered
ORDER BY symbol, qdate;
