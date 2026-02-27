-- ============================================================
-- MONTHLY RANKINGS (configurable)
-- - Choose the month (formation_qdate)
-- - Choose how to rank: "D3" (z-score sum) or "RAW" (ey desc, roc desc)
-- - Choose top_n
-- ============================================================

DECLARE formation_qdate DATE DEFAULT DATE '2018-01-31';
DECLARE top_n INT64 DEFAULT 50;

-- "D3" => score = z(ey) + z(roc)  (Rule D3 style ranking)
-- "RAW" => order by ey DESC, roc DESC (simple)
DECLARE rank_method STRING DEFAULT 'D3';

WITH base AS (
  SELECT
    qdate,
    symbol,
    ey,
    roc,
    -- keep any other fields you want to inspect
    sector,
    industry,
    marketCap,
    peRatio,
    ev,
    ebit
  FROM `fluid-terminal-465516-s7.magic_formula.market_magic_formula_values_with_exclusions`
  WHERE qdate = formation_qdate
    AND ey IS NOT NULL
    AND roc IS NOT NULL
),

stats AS (
  SELECT
    b.*,
    AVG(ey) OVER () AS mu_ey,
    STDDEV_POP(ey) OVER () AS sd_ey,
    AVG(roc) OVER () AS mu_roc,
    STDDEV_POP(roc) OVER () AS sd_roc
  FROM base b
),

scored AS (
  SELECT
    qdate,
    symbol,
    ey,
    roc,
    sector,
    industry,
    marketCap,
    peRatio,
    ev,
    ebit,

    SAFE_DIVIDE(ey  - mu_ey,  sd_ey)  AS z_ey,
    SAFE_DIVIDE(roc - mu_roc, sd_roc) AS z_roc,
    SAFE_DIVIDE(ey  - mu_ey,  sd_ey) + SAFE_DIVIDE(roc - mu_roc, sd_roc) AS d3_score
  FROM stats
),

ranked AS (
  SELECT
    *,
    -- Choose ordering depending on method
    ROW_NUMBER() OVER (
      ORDER BY
        IF(rank_method = 'D3', d3_score, NULL) DESC,
        IF(rank_method = 'RAW', ey, NULL) DESC,
        IF(rank_method = 'RAW', roc, NULL) DESC,
        symbol
    ) AS rank
  FROM scored
)

SELECT
  formation_qdate AS formation_date,
  rank_method,
  rank,
  symbol,
  ey,
  roc,
  d3_score,
  z_ey,
  z_roc,
  marketCap,
  peRatio,
  ev,
  ebit,
  sector,
  industry
FROM ranked
WHERE rank <= top_n
ORDER BY rank;
