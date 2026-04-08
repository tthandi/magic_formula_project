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
  - normalized/z-score ranking flow (D3 rule)
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

> Placeholder: update this section with your exact runtime environment if different.

### 1) Prerequisites

- Google BigQuery project + dataset (expected dataset name: `magic_formula`)
- Source tables loaded (examples referenced by SQL):
  - `balance_sheet`
  - `income_statement`
  - `company_overview`
  - `market_cap`
  - `daily_price`
  - `benchmark_daily_price`
- Optional: Python/Jupyter environment for notebooks and plotting

### 2) Configure project IDs in SQL

Several scripts use `${PROJECT_ID}` placeholders. Replace or parameterize them before execution.

Some scripts currently include hardcoded project names. Standardize these to your project before running end-to-end.

### 3) Recommended execution order

1. Run ingestion notebooks in `notebooks/` (if raw data is not already loaded).
2. Build factor tables from `sql/tables/`.
3. Run diagnostics in `sql/diagnostics/Diagnostics.sql`.
4. Run backtests in `sql/backtesting/`.
5. Run graph/report queries in `sql/graphs/` and export visuals.

## Expected tables built

The following tables are expected from core SQL scripts (names as defined in scripts):

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

### Normalized (D3) backtest outputs

- `magic_formula.bt_monthly_picks_d3_norebuy`
- `magic_formula.bt_trades_d3_norebuy`
- `magic_formula.bt_cohort_returns_d3_norebuy`
- `magic_formula.bt_nav_period_returns_d3_norebuy`
- `magic_formula.bt_portfolio_nav_d3_norebuy`

### Optional yfinance supplement tables

- `magic_formula.symbol_yf_map`
- `magic_formula.daily_price_yf`

### Possible additional/intermediate tables

> Placeholder: document any temporary or helper tables that your workflow persists.

## Raw vs normalized: what is the difference?

### Raw

- Ranks names directly on raw EY/ROC ordering by month.
- Optional 3rd monthly buy is based on closeness to rank #2 in both EY and ROC.
- Useful as a direct, low-transformation baseline.

### Normalized (D3)

- Converts EY and ROC to monthly z-scores and combines into a single score.
- Optional 3rd monthly buy uses score proximity (`score3 >= score2 - delta`).
- Better for balancing factor scale differences across months.

## Results

This repo includes result artifacts for both approaches:

- `docs/images/backtests/raw/`
- `docs/images/backtests/normalized/`

### Performance summary

> Placeholder: add key metrics here once finalized (CAGR, max drawdown, alpha/beta, Sharpe, hit rate, etc.).

Suggested template:
- Period tested: `TODO`
- Strategy CAGR: `TODO`
- Benchmark CAGR: `TODO`
- Max drawdown (strategy / benchmark): `TODO / TODO`
- Alpha / Beta: `TODO / TODO`

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
- Some SQL currently mixes parameterized and hardcoded project references.

### Known open items

> Placeholder: add confirmed known issues here.
- `TODO: Document exact AlphaVantage/API limitations and impact`
- `TODO: Document survivorship-bias handling (if any)`
- `TODO: Document transaction costs/slippage assumptions`
- `TODO: Document rebalancing and execution assumptions`

## Notes

- Keep notebooks in `notebooks/` and SQL in `sql/`.
- Use descriptive names for SQL files by feature/purpose.
- Add methodology notes and experiment logs under `docs/`.
