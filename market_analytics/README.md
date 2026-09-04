# Stock Market & Mutual Fund Analytics Lakehouse

A production-style Databricks Lakehouse that incrementally ingests NSE-listed stocks, ETFs, market
indices, and mutual fund NAV history; processes it Bronze → Silver → Gold using Delta Lake and
Lakeflow Declarative Pipelines; applies data quality checks, SCD/CDC where it earns its keep, and
orchestration; and exposes the result to Power BI, Databricks SQL, and a natural-language AI agent.

Built on **Databricks Free Edition** (serverless-only), deployed via **Databricks Asset Bundles**.

![Architecture](docs/architecture.png)

## What this answers

The project exists to answer questions like:
- *"What was Reliance's return over the last year?"*
- *"Compare NIFTY 50 and NIFTY Bank."*
- *"How does this large-cap fund's beta and alpha compare to its benchmark?"*

## Data sources

| Source | What | Notes |
|---|---|---|
| [yfinance](https://pypi.org/project/yfinance/) | Daily OHLCV for stocks, ETFs, index | Batched, multi-ticker `yf.download()` calls |
| [mfapi.in](https://www.mfapi.in/) | Mutual fund scheme list + full NAV history | No incremental API — full history pulled per scheme, deduplicated downstream |
| NSE equity master (CSV) | Symbol, ISIN, listing date, series | Static reference file, user-supplied |
| ETF master (XLSX) | Symbol, underlying asset | Static reference file, user-supplied |
| Hand-maintained list | ~20 major NSE/BSE indices | NIFTY 50, Bank NIFTY, Sensex, India VIX, sector indices |

## Catalog structure

```
stock_market (Unity Catalog)
├── bronze   — raw Delta tables, one per source, append-only or full-overwrite depending on source shape
├── silver   — cleaned, conformed, deduplicated
└── gold     — analytics-ready: dimensions, facts, and decision-support metrics
```

See [docs/data_dictionary.md](docs/data_dictionary.md) for full column-level documentation of every
Gold table.

## Repo structure

```
.
├── databricks.yml                  # DAB root config
├── resources/                      # DAB job + pipeline definitions
│   ├── job.yml
│   └── pipeline.yml
├── 00_setup/                       # one-time catalog/schema/watermark setup
├── common/                         # shared config + watermark helpers used by ingestion notebooks
├── masters/                        # loads equity/ETF/index master reference data → Bronze
├── market_data/                    # incremental yfinance ingestion → Bronze
├── mutual_funds/                   # incremental mfapi.in ingestion → Bronze
├── 02_silver_gold_pipeline/        # Lakeflow Declarative Pipeline source (Silver + Gold)
│   ├── silver/
│   └── gold/
├── uc_functions.sql                # SQL functions exposed as AI agent tools
└── docs/
    ├── architecture.png
    └── data_dictionary.md
```

## How it works

1. **Bronze** — plain Python notebooks, run as Databricks Job tasks, call yfinance/mfapi.in and
   land raw data as Delta tables. Incremental loading is driven by a small **watermark control
   table** (`bronze.ingestion_watermark`) rather than scanning the full price history each run.
2. **Silver** — a single Lakeflow Declarative Pipeline (Free Edition allows one active pipeline per
   type, so Silver and Gold share one pipeline object with many tables/flows inside it). Price and
   NAV tables use genuine streaming reads + `AUTO CDC` upserts. Master dimensions are recomputed as
   plain materialized views — full SCD2 history was evaluated and deliberately dropped for these,
   given how small and rarely-changing they are relative to the complexity it added.
3. **Gold** — star schema (`dim_security`, `dim_scheme`, `dim_date`, `fact_price_daily`,
   `fact_nav_daily`) plus decision-support snapshot tables (`security_metrics`, `scheme_metrics`,
   `stock_vs_benchmark`, `scheme_vs_benchmark`) computed fresh each pipeline run.
4. **Orchestration** — one Databricks Job runs the Bronze ingestion tasks, then triggers the
   Silver/Gold pipeline once all sources are updated. Scheduled daily after NSE market close.
5. **Consumption** — Power BI (Import mode, scheduled refresh) for dashboards; Databricks SQL for
   ad-hoc analysis; a Genie space over the `gold` schema for natural-language Q&A, backed by two UC
   SQL functions (`fn_return`, `fn_compare_securities`) for reliable answers to common questions.

## Setup

1. Run `00_setup` once to create the catalog, schemas, and watermark table.
2. Upload the equity CSV and ETF XLSX master files to the `market_landing` volume.
3. Run the `masters/` notebooks, then `market_data/` and `mutual_funds/` notebooks once each to
   backfill history (resumable — safe to re-run if interrupted).
4. Create a Lakeflow pipeline pointed at `02_silver_gold_pipeline/`, run it.
5. Run `uc_functions.sql` once in a SQL editor.
6. Wire the above into a Databricks Job (see `resources/job.yml`) and schedule it.
7. Connect Power BI via the SQL warehouse connection details (Import mode).
8. Create a Genie space over `stock_market.gold`.

## Known limitations / deliberate simplifications

- **No stock fundamentals (PE ratio, PB ratio, etc.)** — not ingested. `yfinance`'s bulk API doesn't
  include these; would require a separate, slower, more fragile per-ticker `yf.Ticker().info` source.
- **`RISK_FREE_RATE` is a hardcoded constant** (`0.07`) used for Sharpe ratio and alpha calculations,
  not sourced from a real rate feed. Update as needed in `gold_security_metrics.py` /
  `gold_scheme_metrics.py` / the benchmark comparison files.
- **Benchmark mapping for funds is a simple category-text match** (`gold_benchmark_map.py`) —
  reasonable defaults, easily edited, not authoritative.
- **`exchange` field defaults to NSE** for all stocks/ETFs, since that's the source of both the
  master files and the price data (`.NS` tickers) — not a claim about where else a security may be
  dual-listed.
- **No rolling time-series of risk metrics** (e.g. beta trending over months) and **no fund
  ranking/percentile logic** — deliberately left to Power BI's DAX layer rather than baked into
  fixed Gold tables.

## Roadmap

- [ ] Stock fundamentals ingestion (PE, PB, dividend yield, market cap)
- [ ] `prod` DAB target with proper dev/prod catalog separation
- [ ] Data quality metrics dashboard from Lakeflow expectations
- [ ] Rate table for risk-free rate instead of hardcoded constant


