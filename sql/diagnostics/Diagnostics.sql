-- ======================================================================
-- MAGIC FORMULA DIAGNOSTICS SUITE (BigQuery Standard SQL)
-- Non-correlated, BigQuery-safe. One statement per EXECUTE IMMEDIATE.
-- ======================================================================

DECLARE p_project STRING DEFAULT 'fluid-terminal-465516-s7';
DECLARE p_dataset STRING DEFAULT 'magic_formula';

DECLARE t_bs   STRING DEFAULT FORMAT('`%s.%s.balance_sheet`', p_project, p_dataset);
DECLARE t_is   STRING DEFAULT FORMAT('`%s.%s.income_statement`', p_project, p_dataset);
DECLARE t_mc   STRING DEFAULT FORMAT('`%s.%s.market_cap`', p_project, p_dataset);
DECLARE t_dp   STRING DEFAULT FORMAT('`%s.%s.daily_price`', p_project, p_dataset);

DECLARE t_fund STRING DEFAULT FORMAT('`%s.%s.fundamental_magic_formula_values_no_exclusions`', p_project, p_dataset);

-- ✅ fixed names
DECLARE t_mkt      STRING DEFAULT FORMAT('`%s.%s.market_magic_formula_values_no_exclusions`', p_project, p_dataset);
DECLARE t_mkt_excl STRING DEFAULT FORMAT('`%s.%s.market_magic_formula_values_with_exclusions`', p_project, p_dataset);

-- optional deep-dive date (not used in this suite, but kept for future extensions)
DECLARE p_target_qdate DATE DEFAULT DATE '2025-10-31';

-- ==========================================================
-- A) DATA INTEGRITY + DUPLICATES
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
sym_hygiene AS (
  SELECT 'balance_sheet' AS table_name, symbol,
         LENGTH(symbol) AS sym_len,
         REGEXP_CONTAINS(symbol, r'^\s|\s$') AS has_edge_whitespace
  FROM %s
  UNION ALL
  SELECT 'income_statement', symbol, LENGTH(symbol), REGEXP_CONTAINS(symbol, r'^\s|\s$')
  FROM %s
  UNION ALL
  SELECT 'market_cap', symbol, LENGTH(symbol), REGEXP_CONTAINS(symbol, r'^\s|\s$')
  FROM %s
),
dupes_sources AS (
  SELECT 'balance_sheet' AS table_name, symbol, fiscalDateEnding AS key_date, COUNT(*) AS cnt
  FROM %s
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT 'income_statement', symbol, fiscalDateEnding, COUNT(*) AS cnt
  FROM %s
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT 'market_cap', symbol, snapshotDate, COUNT(*) AS cnt
  FROM %s
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
),
dupes_outputs AS (
  SELECT 'market_magic_formula_values_no_exclusions' AS table_name,
         symbol, qdate AS key_date, COUNT(*) AS cnt
  FROM %s
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT 'market_magic_formula_values_with_exclusions',
         symbol, qdate, COUNT(*) AS cnt
  FROM %s
  GROUP BY 1,2,3
  HAVING COUNT(*) > 1
)
SELECT
  'symbol_hygiene' AS section,
  table_name,
  COUNT(*) AS rows_checked,
  COUNTIF(has_edge_whitespace) AS symbols_with_edge_whitespace,
  COUNTIF(sym_len = 0) AS empty_symbols,
  COUNTIF(sym_len > 10) AS unusually_long_symbols
FROM sym_hygiene
GROUP BY 1,2

UNION ALL

SELECT
  'duplicates_sources' AS section,
  table_name,
  COUNT(*) AS rows_checked,
  NULL AS symbols_with_edge_whitespace,
  NULL AS empty_symbols,
  NULL AS unusually_long_symbols
FROM dupes_sources
GROUP BY 1,2

UNION ALL

SELECT
  'duplicates_outputs' AS section,
  table_name,
  COUNT(*) AS rows_checked,
  NULL, NULL, NULL
FROM dupes_outputs
GROUP BY 1,2
ORDER BY section, table_name
""",
t_bs, t_is, t_mc,
t_bs, t_is, t_mc,
t_mkt, t_mkt_excl);

-- ==========================================================
-- B1) BS/IS OVERLAP + MISSING MATCHES (exact-date)
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
bs AS (SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde FROM %s),
isq AS (SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde FROM %s),
exact_overlap AS (
  SELECT
    COALESCE(bs.symbol, isq.symbol) AS symbol,
    COALESCE(bs.fde, isq.fde) AS fde,
    bs.fde IS NOT NULL AS has_bs,
    isq.fde IS NOT NULL AS has_is
  FROM bs
  FULL OUTER JOIN isq
  USING (symbol, fde)
)
SELECT
  EXTRACT(YEAR FROM fde) AS yr,
  EXTRACT(QUARTER FROM fde) AS qtr,
  COUNTIF(has_bs) AS bs_rows,
  COUNTIF(has_is) AS is_rows,
  COUNTIF(has_bs AND has_is) AS exact_overlap_rows,
  COUNTIF(has_bs AND NOT has_is) AS bs_only_rows,
  COUNTIF(has_is AND NOT has_bs) AS is_only_rows
FROM exact_overlap
GROUP BY 1,2
ORDER BY 1,2
""", t_bs, t_is);

-- ==========================================================
-- B2) BS -> nearest IS MISALIGNMENT (<=120 days), no correlation
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
bs AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS bs_fde
  FROM %s
),
isq AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS is_fde
  FROM %s
),
paired AS (
  SELECT
    b.symbol,
    b.bs_fde,
    i.is_fde,
    ABS(DATE_DIFF(i.is_fde, b.bs_fde, DAY)) AS abs_diff_days
  FROM bs b
  LEFT JOIN isq i
    ON i.symbol = b.symbol
   AND ABS(DATE_DIFF(i.is_fde, b.bs_fde, DAY)) <= 120
),
nearest AS (
  SELECT *
  FROM paired
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, bs_fde
    ORDER BY abs_diff_days
  ) = 1
)
SELECT
  EXTRACT(YEAR FROM bs_fde) AS yr,
  EXTRACT(QUARTER FROM bs_fde) AS qtr,
  COUNT(*) AS bs_rows_checked,
  COUNTIF(is_fde IS NULL) AS bs_with_no_is_within_120d,
  APPROX_QUANTILES(abs_diff_days, 5)[OFFSET(1)] AS p20_abs_diff_days,
  APPROX_QUANTILES(abs_diff_days, 5)[OFFSET(2)] AS p40_abs_diff_days,
  APPROX_QUANTILES(abs_diff_days, 5)[OFFSET(3)] AS p60_abs_diff_days,
  APPROX_QUANTILES(abs_diff_days, 5)[OFFSET(4)] AS p80_abs_diff_days
FROM nearest
GROUP BY 1,2
ORDER BY 1,2
""", t_bs, t_is);

-- ==========================================================
-- C1) FUNDAMENTAL EXPECTED vs ACTUAL (exact BS∩IS vs fund table)
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
bs AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde
  FROM %s
),
isq AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde
  FROM %s
),
expected AS (
  SELECT b.symbol, b.fde
  FROM bs b
  JOIN isq i USING (symbol, fde)
),
actual AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde
  FROM %s
)
SELECT
  EXTRACT(YEAR FROM e.fde) AS yr,
  EXTRACT(QUARTER FROM e.fde) AS qtr,
  COUNT(*) AS expected_rows,
  COUNTIF(a.symbol IS NOT NULL) AS actual_rows,
  COUNTIF(a.symbol IS NULL) AS missing_rows
FROM expected e
LEFT JOIN actual a USING (symbol, fde)
GROUP BY 1,2
ORDER BY 1,2
""", t_bs, t_is, t_fund);

-- ==========================================================
-- C2) FUNDAMENTAL MISSING ROOT CAUSES (no correlated EXISTS)
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
bs AS (
  SELECT
    TRIM(symbol) AS symbol,
    DATE(fiscalDateEnding) AS fde,
    CAST(NULLIF(commonStockSharesOutstanding, '') AS FLOAT64) AS shares_out
  FROM %s
),
isq AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde
  FROM %s
),
expected AS (
  SELECT b.symbol, b.fde
  FROM bs b
  JOIN isq i USING (symbol, fde)
),
actual AS (
  SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde
  FROM %s
),
missing AS (
  SELECT e.symbol, e.fde
  FROM expected e
  LEFT JOIN actual a USING (symbol, fde)
  WHERE a.symbol IS NULL
),
-- daily price existence on/before fde via join+agg
price_flag AS (
  SELECT
    m.symbol,
    m.fde,
    COUNT(dp.symbol) > 0 AS has_daily_price_on_or_before_fde
  FROM missing m
  LEFT JOIN %s dp
    ON TRIM(dp.symbol) = m.symbol
   AND DATE(dp.date) <= m.fde
  GROUP BY 1,2
),
shares_flag AS (
  SELECT
    m.symbol,
    m.fde,
    (b.shares_out IS NOT NULL AND b.shares_out > 0) AS has_shares
  FROM missing m
  LEFT JOIN bs b
    ON b.symbol = m.symbol AND b.fde = m.fde
)
SELECT
  EXTRACT(YEAR FROM m.fde) AS yr,
  EXTRACT(QUARTER FROM m.fde) AS qtr,
  COUNT(*) AS missing_rows,
  COUNTIF(NOT p.has_daily_price_on_or_before_fde) AS missing_due_to_no_daily_price,
  COUNTIF(NOT s.has_shares) AS missing_due_to_missing_shares,
  COUNTIF(p.has_daily_price_on_or_before_fde AND s.has_shares) AS missing_other_reasons
FROM missing m
JOIN price_flag p USING (symbol, fde)
JOIN shares_flag s USING (symbol, fde)
GROUP BY 1,2
ORDER BY 1,2
""", t_bs, t_is, t_fund, t_dp);

-- ==========================================================
-- D1) MARKET CAP EXACT-DATE ALIGNMENT RATE (MC.snapshotDate == BS.fde)
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
bs AS (SELECT TRIM(symbol) AS symbol, DATE(fiscalDateEnding) AS fde FROM %s),
mc AS (SELECT TRIM(symbol) AS symbol, DATE(snapshotDate) AS snap FROM %s)
SELECT
  EXTRACT(YEAR FROM b.fde) AS yr,
  EXTRACT(QUARTER FROM b.fde) AS qtr,
  COUNT(*) AS bs_rows,
  COUNTIF(m.snap IS NOT NULL) AS bs_rows_with_exact_mc_date,
  SAFE_DIVIDE(COUNTIF(m.snap IS NOT NULL), COUNT(*)) AS exact_match_rate
FROM bs b
LEFT JOIN mc m
  ON m.symbol = b.symbol AND m.snap = b.fde
GROUP BY 1,2
ORDER BY 1,2
""", t_bs, t_mc);

-- ==========================================================
-- D2) MARKET CAP MISSING/BAD IN FUNDAMENTAL TABLE
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
SELECT
  EXTRACT(YEAR FROM DATE(fiscalDateEnding)) AS yr,
  EXTRACT(QUARTER FROM DATE(fiscalDateEnding)) AS qtr,
  COUNT(*) AS fund_rows,
  COUNTIF(marketCap IS NULL OR CAST(marketCap AS FLOAT64) <= 0) AS fund_rows_missing_or_bad_mc
FROM %s
GROUP BY 1,2
ORDER BY 1,2
""", t_fund);

-- ==========================================================
-- E) MARKET MONTHLY EXPECTED vs ACTUAL + WHY MISSING (no correlation)
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
WITH
months AS (
  SELECT qdate
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2006-01-31', DATE '2025-10-31', INTERVAL 1 MONTH)) AS qdate
),
universe AS (
  SELECT DISTINCT TRIM(symbol) AS symbol
  FROM %s
),
snap AS (
  SELECT
    u.symbol,
    m.qdate,
    DATE_SUB(m.qdate, INTERVAL 60 DAY) AS cutoff_date
  FROM universe u
  CROSS JOIN months m
),
bs AS (
  SELECT
    TRIM(symbol) AS symbol,
    DATE(fiscalDateEnding) AS fde,
    CAST(totalCurrentAssets AS FLOAT64) AS totalCurrentAssets,
    CAST(cashAndCashEquivalentsAtCarryingValue AS FLOAT64) AS cash,
    CAST(totalCurrentLiabilities AS FLOAT64) AS totalCurrentLiabilities,
    CAST(propertyPlantEquipment AS FLOAT64) AS ppe,
    CAST(NULLIF(commonStockSharesOutstanding,'') AS FLOAT64) AS shares_out
  FROM %s
),
isq AS (
  SELECT
    TRIM(symbol) AS symbol,
    DATE(fiscalDateEnding) AS fde,
    CAST(COALESCE(NULLIF(ebit,''), NULLIF(operatingIncome,'')) AS FLOAT64) AS ebit
  FROM %s
),
mc AS (
  SELECT
    TRIM(symbol) AS symbol,
    DATE(snapshotDate) AS snap_date,
    CAST(marketCap AS FLOAT64) AS marketCap
  FROM %s
),

bs_pick AS (
  SELECT
    s.symbol, s.qdate,
    b.fde,
    b.totalCurrentAssets, b.cash, b.totalCurrentLiabilities, b.ppe, b.shares_out,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, s.qdate ORDER BY b.fde DESC) AS rn
  FROM snap s
  LEFT JOIN bs b
    ON b.symbol = s.symbol
   AND b.fde <= s.cutoff_date
),
bs_latest AS (
  SELECT * EXCEPT(rn) FROM bs_pick WHERE rn = 1
),

is_pick AS (
  SELECT
    s.symbol, s.qdate,
    i.fde AS is_fde,
    i.ebit,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, s.qdate ORDER BY i.fde DESC) AS rn
  FROM snap s
  LEFT JOIN isq i
    ON i.symbol = s.symbol
   AND i.fde <= s.cutoff_date
),
is_latest AS (
  SELECT * EXCEPT(rn) FROM is_pick WHERE rn = 1
),

mc_pick AS (
  SELECT
    s.symbol, s.qdate,
    m.snap_date,
    m.marketCap,
    ROW_NUMBER() OVER (PARTITION BY s.symbol, s.qdate ORDER BY m.snap_date DESC) AS rn
  FROM snap s
  LEFT JOIN mc m
    ON m.symbol = s.symbol
   AND m.snap_date <= s.qdate
),
mc_latest AS (
  SELECT * EXCEPT(rn) FROM mc_pick WHERE rn = 1
),

joined AS (
  SELECT
    s.symbol,
    s.qdate,

    b.fde AS bs_fde,
    i.is_fde,
    m.snap_date,

    -- flags
    (b.fde IS NOT NULL) AS has_bs,
    (i.is_fde IS NOT NULL) AS has_is,
    (m.snap_date IS NOT NULL) AS has_mc,

    (b.shares_out IS NOT NULL AND b.shares_out > 0) AS has_shares,
    (i.ebit IS NOT NULL) AS has_ebit,
    (m.marketCap IS NOT NULL AND m.marketCap > 0) AS has_marketCap,

    m.marketCap AS ev_proxy,
    (b.totalCurrentAssets - b.cash - b.totalCurrentLiabilities + b.ppe) AS capital_proxy
  FROM snap s
  LEFT JOIN bs_latest b USING (symbol, qdate)
  LEFT JOIN is_latest i USING (symbol, qdate)
  LEFT JOIN mc_latest m USING (symbol, qdate)
),

expected_logic AS (
  SELECT
    *,
    (has_bs AND has_is AND has_mc AND has_shares AND has_ebit AND has_marketCap) AS expected_prereqs,
    (ev_proxy IS NULL OR ev_proxy <= 0) AS bad_ev,
    (capital_proxy IS NULL OR capital_proxy <= 0) AS bad_capital
  FROM joined
),

actual AS (
  SELECT TRIM(symbol) AS symbol, DATE(qdate) AS qdate
  FROM %s
)

SELECT
  e.qdate,
  COUNT(*) AS universe_rows,
  COUNTIF(e.expected_prereqs) AS expected_rows,
  COUNTIF(a.symbol IS NOT NULL) AS actual_rows,
  COUNTIF(e.expected_prereqs AND a.symbol IS NULL) AS expected_but_missing_rows,

  COUNTIF(NOT e.has_bs) AS missing_bs,
  COUNTIF(NOT e.has_is) AS missing_is,
  COUNTIF(NOT e.has_mc) AS missing_market_cap_row,
  COUNTIF(e.has_bs AND NOT e.has_shares) AS missing_shares,
  COUNTIF(e.has_is AND NOT e.has_ebit) AS missing_ebit,
  COUNTIF(e.has_mc AND NOT e.has_marketCap) AS bad_marketCap_value,

  COUNTIF(e.expected_prereqs AND e.bad_ev) AS bad_ev_among_expected,
  COUNTIF(e.expected_prereqs AND e.bad_capital) AS bad_capital_among_expected
FROM expected_logic e
LEFT JOIN actual a USING (symbol, qdate)
GROUP BY 1
ORDER BY e.qdate
""", t_mc, t_bs, t_is, t_mc, t_mkt);

-- ==========================================================
-- F1) POST-EXCLUSION MONTHLY COVERAGE
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
SELECT
  DATE(qdate) AS qdate,
  COUNT(*) AS rows_post_exclusions,
  COUNT(DISTINCT TRIM(symbol)) AS symbols_post_exclusions
FROM %s
GROUP BY 1
ORDER BY qdate
""", t_mkt_excl);

-- ==========================================================
-- F2) POST-EXCLUSION ANNUAL DISTINCT SYMBOLS
-- ==========================================================
EXECUTE IMMEDIATE FORMAT(r"""
SELECT
  EXTRACT(YEAR FROM DATE(qdate)) AS yr,
  COUNT(DISTINCT TRIM(symbol)) AS distinct_symbols_post_exclusions
FROM %s
GROUP BY 1
ORDER BY yr
""", t_mkt_excl);
