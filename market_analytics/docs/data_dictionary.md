# Gold Layer Data Dictionary

All tables live in `stock_market.gold`. This is the layer Power BI, Databricks SQL, and the AI agent
should query — not Bronze or Silver directly.

## Dimensions

### `dim_security`
One row per stock/ETF/index symbol, current state only (no history).

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker as used in price data (no `.NS` suffix for stocks/ETFs) |
| `name` | STRING | Company/fund/index name |
| `instrument_type` | STRING | `STOCK`, `ETF`, or `INDEX` |
| `sub_type` | STRING | Series (stocks) / asset sub-type (ETFs) / category (index) |
| `exchange` | STRING | `NSE` or `BSE` |
| `isin_number` | STRING | ISIN, stocks only — NULL for ETFs/index (not in source data) |

### `dim_scheme`
One row per mutual fund scheme, current state only.

| Column | Type | Description |
|---|---|---|
| `scheme_code` | STRING | mfapi.in scheme code |
| `scheme_name` | STRING | Full scheme name (growth plans only — dividend/IDCW filtered out) |
| `isin_growth` | STRING | ISIN for the growth option |
| `fund_house` | STRING | AMC name |
| `scheme_category` | STRING | e.g. "Equity Scheme - Large Cap Fund" |
| `scheme_type` | STRING | Open-ended / closed-ended |

### `dim_date`
Standard calendar dimension, 2000-01-01 through ~5 years ahead.

| Column | Type | Description |
|---|---|---|
| `date` | DATE | |
| `year`, `quarter`, `month`, `month_name` | | Calendar attributes |
| `day_of_week` | STRING | |
| `is_weekend` | BOOLEAN | |
| `fiscal_year` | INT | Indian FY convention (April–March) |

## Facts

### `fact_price_daily`
Daily OHLCV for stocks, ETFs, and index — all instrument types unified in one table.

| Column | Type | Description |
|---|---|---|
| `symbol`, `date`, `instrument_type` | | Grain |
| `open`, `high`, `low`, `close`, `volume` | DOUBLE/LONG | |
| `daily_return` | DOUBLE | % change vs. previous trading day |

### `fact_nav_daily`
Daily NAV for mutual fund schemes.

| Column | Type | Description |
|---|---|---|
| `scheme_code`, `date` | | Grain |
| `nav` | DOUBLE | |
| `daily_return` | DOUBLE | % change vs. previous NAV date |

## Decision-support snapshots

### `security_metrics`
One row per symbol, recomputed each pipeline run as of the latest available date.

| Column | Description |
|---|---|
| `as_of_date` | The date this entire row is calculated from |
| `latest_close` | |
| `return_1d` … `return_1y` | Simple % return over each period |
| `return_3y`, `return_5y` | Simple total % return (not annualized) |
| `cagr_3y`, `cagr_5y` | Annualized return — NULL if less than 3/5 years of history exists |
| `volatility_1y` | Annualized standard deviation of daily returns, trailing 1 year |
| `sharpe_1y` | (Annualized return − risk-free rate) / volatility, trailing 1 year |
| `max_drawdown_1y` | Largest peak-to-trough decline, trailing 1 year, as a % (negative) |

### `scheme_metrics`
Same structure as `security_metrics`, keyed by `scheme_code`/`latest_nav` instead of `symbol`/`latest_close`.

### `stock_vs_benchmark`
Beta/alpha/correlation of each stock/ETF vs. NIFTY 50, trailing 1 year.

| Column | Description |
|---|---|
| `symbol`, `benchmark_symbol` | |
| `beta_1y` | Sensitivity to benchmark moves (covariance / benchmark variance) |
| `alpha_1y` | CAPM alpha, annualized % — excess return not explained by beta |
| `correlation_1y` | Pearson correlation of daily returns, -1 to 1 |
| `tracking_error_1y` | Annualized std. dev. of (asset return − benchmark return) |

### `scheme_vs_benchmark`
Same metrics as `stock_vs_benchmark`, keyed by `scheme_code`, benchmark chosen per
`scheme_benchmark_map` (category-based mapping — see `gold_benchmark_map.py`).

### `scheme_benchmark_map`
Reference table: `scheme_code` → `benchmark_symbol`, derived from `scheme_category` text matching.
Edit `gold_benchmark_map.py` to change the mapping rules.

## Reference: what's NOT here

No stock fundamentals (PE, PB, dividend yield, market cap) — not ingested. No rolling time-series of
beta/alpha/volatility over time — only point-in-time snapshots. Both are documented as known gaps in
the main README rather than silently missing.
