# Magic Formula Project

SQL + notebook research pipeline for building and backtesting a Magic Formula investing strategy.

## What this project is

This project assembles fundamental and market data, computes Magic Formula factors (ROC and EY), builds investable universes, and runs monthly backtests against an S&P benchmark.

It is organized around:
- data ingestion/validation notebooks in `notebooks/`
- table-building SQL in `sql/tables/`
- backtest logic in `sql/backtesting/`
- diagnostics in `sql/diagnostics/`
- output charts/screenshots in `docs/images/backtests/`

## Features

- Builds both fundamentals-anchored and market-snapshot Magic Formula datasets.
- Supports exclusion and no-exclusion universes (size, valuation, sector/industry filters).
- Runs two backtest styles:
  - raw ranking flow
  - normalized/z-score ranking flow 
- Uses a no-rebuy holding rule with monthly replacement logic.
- Compares strategy NAV and returns vs S&P benchmark series.
- Includes diagnostics SQL for data integrity, alignment, and missingness analysis.

## Repository layout

```text
magic_formula_project/
├── notebooks/              # Data collection, validation, and analysis notebooks
├── sql/
│   ├── tables/             # Table creation and transformation SQL
│   ├── backtesting/        # Backtest/trade/NAV SQL scripts
│   ├── diagnostics/        # Data quality and coverage checks
│   ├── graphs/             # SQL used for plotting/reporting datasets
│   ├── queries/            # Investigation/search queries
│   └── yfinance/           # yfinance helper/supplement SQL
├── docs/images/backtests/  # Saved screenshots and matplotlib outputs
└── README.md
```

## Setup

### 1) Prerequisites

- Google Cloud Project with:
  - BigQuery enabled
  - Dataset: `magic_formula`
- Python environment (recommended):
  - Python `3.9+`
  - `pandas`, `matplotlib`, `google-cloud-bigquery`
- Alpha Vantage API key (for data ingestion)

### 2) Data sources

The pipeline expects the following core tables:

| Table | Description |
| --- | --- |
| `balance_sheet` | Assets, liabilities, debt, cash |
| `income_statement` | EBIT / operating income |
| `company_overview` | Sector, industry, metadata |
| `market_cap` | Market capitalization snapshots |
| `daily_price` | Adjusted OHLC price data |
| `benchmark_daily_price` | S&P 500 benchmark series |

### 3) Configure project IDs in SQL

Several scripts use `${PROJECT_ID}` placeholders. Replace or parameterize them before execution.

Some scripts currently include hardcoded project names. Standardize these to your project before running end-to-end.

### 4) Recommended execution order

1. Run ingestion notebooks in `notebooks/` (if raw data is not already loaded).
2. Build factor tables from `sql/tables/`.
3. Run diagnostics in `sql/diagnostics/Diagnostics.sql`.
4. Run backtests in `sql/backtesting/`.
5. Run graph/report queries in `sql/graphs/` and export visuals.

## Factor definitions

The Magic Formula is built using:

### Return on Capital (ROC)

`ROC = EBIT / (NWC + NFA)`

Where:
- `NWC (Net Working Capital) = Current Assets - Cash - (Current Liabilities - Debt)`
- `NFA (Net Fixed Assets) ~= PPE` or `(Total Assets - Current Assets - Goodwill)`

### Earnings Yield (EY)

`EY = EBIT / EnterpriseValue`

Where:
- `EV = MarketCap + Debt - Cash`

## Backtesting methodology

- Rebalance frequency: monthly
- Buy timing: next trading day after month-end
- Sell timing: ~1 year later (first available trading day)
- Portfolio construction:
  - equal weight
  - typically 2-3 stocks per month
- No-rebuy rule:
  - cannot re-enter a stock within 1 year of holding

## Expected tables built

### Universe and factor tables

- `magic_formula.fundamental_magic_formula_values`
- `magic_formula.market_magic_formula_values_no_exclusions`
- `magic_formula.market_magic_formula_values_with_exclusions`
- `magic_formula.sp500_monthly_returns`

### Raw backtest outputs

- `magic_formula.bt_monthly_picks_raw_norebuy`
- `magic_formula.bt_trades_raw_norebuy`
- `magic_formula.bt_nav_period_returns_raw_norebuy`
- `magic_formula.bt_portfolio_nav_raw_norebuy`

### Normalized backtest outputs

- `magic_formula.bt_monthly_picks_d3_norebuy`
- `magic_formula.bt_trades_d3_norebuy`
- `magic_formula.bt_cohort_returns_d3_norebuy`
- `magic_formula.bt_nav_period_returns_d3_norebuy`
- `magic_formula.bt_portfolio_nav_d3_norebuy`

### yfinance supplement tables

- `magic_formula.symbol_yf_map`
- `magic_formula.daily_price_yf`

### Possible additional/intermediate tables

> Placeholder: document any temporary or helper tables that your workflow persists.

## Raw vs normalized: what is the difference?

### Raw

- Ranks names directly on raw EY/ROC ordering by month.
- Optional 3rd monthly buy is based on closeness to rank #2 in both EY and ROC.
- Useful as a direct, low-transformation baseline.

### Normalized 

- Converts EY and ROC to monthly z-scores and combines into a single score.
- Optional 3rd monthly buy uses score proximity (`score3 >= score2 - delta`).
- Better for balancing factor scale differences across months.

## Results

This repo includes result artifacts for both approaches:

- `docs/images/backtests/raw/`
- `docs/images/backtests/normalized/`

### Performance summary

Period tested: `2006-03-01` to `2025-02-03`

| Metric | Raw | Normalized |
| --- | ---: | ---: |
| Start NAV | 1.0622 | 1.0757 |
| End NAV | 26.3665 | 35.3121 |
| Total return | 2382.33% | 3182.66% |
| CAGR | 18.24% | 19.98% |
| Annualized volatility | 22.80% | 21.72% |
| Sharpe | 0.868 | 0.971 |
| Max drawdown | -44.28% | -45.13% |
| Beta | 1.103 | 1.016 |
| Alpha (annualized) | 7.27% | 9.67% |
| Correlation vs S&P | 0.834 | 0.807 |
| R-squared vs S&P | 0.696 | 0.651 |
| CAGR outperformance vs S&P | 7.70%/yr | 9.43%/yr |
| Trade count | 468 | 409 |
| Win rate | 64.96% | 68.70% |
| Avg trade return | 21.26% | 21.60% |
| Median trade return | 12.39% | 12.98% |
| Best trade | 576.50% | 576.50% |
| Worst trade | -81.30% | -79.75% |

Benchmark context (same period): S&P total return `572.08%`, CAGR `10.55%`, Sharpe `0.611`.

Normalized vs Raw delta: `+1.74%/yr` CAGR, `-1.08%` volatility, `+0.103` Sharpe, and `+3.75%` win rate, with a slightly deeper max drawdown (`-0.86%`).

### Interpretation

- Both implementations materially outperform the benchmark over the full sample, suggesting the Magic Formula signal remains strong in this configuration.
- The normalized variant appears more efficient: higher CAGR and Sharpe with lower volatility and lower market beta than the raw variant.
- Drawdown remains severe in both approaches (about `-44%` to `-45%`), so the strategy still carries substantial equity-like tail risk despite strong long-run returns.
- Trade-level statistics (higher win rate and slightly better median return in normalized) indicate consistency improved, not just a few outlier winners.
- Because benchmark correlation is still high (`~0.81-0.83`), this is best interpreted as enhanced equity exposure rather than a market-neutral return stream.

## Graphs

### Raw backtest graphs

- Rolling 12M return
- Strategy NAV vs benchmark NAV
- Portfolio return over time
- Strategy drawdown vs benchmark drawdown
- Matplotlib cumulative NAV and return/drawdown charts

Files are under `docs/images/backtests/raw/`.

### Normalized backtest graphs

- Strategy NAV vs benchmark NAV
- Portfolio return over time
- Rolling 12M return
- Strategy drawdown vs benchmark drawdown
- Matplotlib cumulative NAV and return/drawdown charts

Files are under `docs/images/backtests/normalized/`.

## Limitations

- Data quality and symbol alignment can affect coverage and ranking stability.
- Quarterly fundamentals vs trading-date market snapshots require careful lag alignment.
- Missing price windows can impact trade simulation realism.
- Source/API constraints may limit completeness for specific symbols/time ranges.
- Alpha Vantage does not reliably retain history for delisted tickers, which can exclude failed names from the test universe and introduce survivorship bias in reported results.
- Some SQL currently mixes parameterized and hardcoded project references.

## Summary

This project delivers an end-to-end Magic Formula research and backtesting workflow, from data ingestion and factor construction to portfolio simulation and benchmark comparison. Both raw and normalized ranking methods outperform the S&P over the tested period, with normalized ranking showing stronger risk-adjusted performance. Interpret all results alongside the documented limitations, especially survivorship bias risk caused by incomplete delisted-ticker coverage in Alpha Vantage.


