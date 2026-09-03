# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file.
#
# latest_date is attached as a COLUMN (unpartitioned window F.max), never
# pulled out via .collect() -- single continuous DataFrame lineage rooted at
# spark.read.table(...), which is what makes this reliable under Lakeflow's
# dependency tracking.
#
# return_Ny = simple total % change over N years. cagr_Ny = annualized
# equivalent. Both are included for 3y/5y since they answer different
# questions ("how much did it grow" vs "what rate did it grow at").
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "stock_market.silver"
GOLD = "stock_market.gold"

RISK_FREE_RATE = 0.07
PERIOD_DAYS = {"1d": 1, "1w": 7, "1m": 30, "3m": 91, "6m": 182, "1y": 365, "3y": 365 * 3, "5y": 365 * 5}
CAGR_YEARS = {"3y": 3, "5y": 5}


def _price_asof(prices_with_latest, out_col, days=None, ytd=False):
    if ytd:
        cutoff = F.date_sub(F.make_date(F.year(F.col("latest_date")), F.lit(1), F.lit(1)), 1)
    else:
        cutoff = F.date_sub(F.col("latest_date"), days)

    asof_dates = (
        prices_with_latest.filter(F.col("date") <= cutoff)
        .groupBy("symbol").agg(F.max("date").alias("asof_date"))
    )
    return (
        asof_dates.join(prices_with_latest, "symbol")
        .filter(F.col("date") == F.col("asof_date"))
        .select("symbol", F.col("close").alias(out_col))
    )


@dp.table(
    name=f"{GOLD}.security_metrics",
    comment="Per-symbol return/risk snapshot as of latest date. Stocks, ETFs, and index all in one table.",
)
def security_metrics():
    prices = spark.read.table(f"{SILVER}.prices_daily")
    prices = prices.withColumn("latest_date", F.max("date").over(Window.partitionBy()))

    result = (
        prices.filter(F.col("date") == F.col("latest_date"))
        .select("symbol", "instrument_type", F.col("date").alias("as_of_date"), F.col("close").alias("latest_close"))
    )

    for period, days in PERIOD_DAYS.items():
        result = result.join(_price_asof(prices, f"close_{period}", days=days), "symbol", "left")
    result = result.join(_price_asof(prices, "close_ytd", ytd=True), "symbol", "left")

    for period in list(PERIOD_DAYS.keys()) + ["ytd"]:
        result = result.withColumn(
            f"return_{period}",
            F.round((F.col("latest_close") / F.col(f"close_{period}") - 1) * 100, 2)
        )
    for period, years in CAGR_YEARS.items():
        result = result.withColumn(
            f"cagr_{period}",
            F.round((F.pow(F.col("latest_close") / F.col(f"close_{period}"), F.lit(1.0 / years)) - 1) * 100, 2)
        )

    w = Window.partitionBy("symbol").orderBy("date")
    daily_returns_1y = (
        prices.filter(F.col("date") > F.date_sub(F.col("latest_date"), 365))
        .withColumn("prev_close", F.lag("close").over(w))
        .withColumn("dr", F.col("close") / F.col("prev_close") - 1)
        .filter(F.col("dr").isNotNull())
    )
    risk = (
        daily_returns_1y.groupBy("symbol")
        .agg(
            (F.stddev("dr") * F.sqrt(F.lit(252))).alias("volatility_1y_raw"),
            (F.avg("dr") * F.lit(252)).alias("annualized_return_1y_raw"),
        )
        .withColumn("sharpe_1y", F.round((F.col("annualized_return_1y_raw") - F.lit(RISK_FREE_RATE)) / F.col("volatility_1y_raw"), 2))
        .withColumn("volatility_1y", F.round(F.col("volatility_1y_raw") * 100, 2))
        .select("symbol", "volatility_1y", "sharpe_1y")
    )
    result = result.join(risk, "symbol", "left")

    w_running_max = Window.partitionBy("symbol").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    mdd = (
        prices.filter(F.col("date") > F.date_sub(F.col("latest_date"), 365))
        .withColumn("running_max", F.max("close").over(w_running_max))
        .withColumn("drawdown", F.col("close") / F.col("running_max") - 1)
        .groupBy("symbol")
        .agg(F.round(F.min("drawdown") * 100, 2).alias("max_drawdown_1y"))
    )
    result = result.join(mdd, "symbol", "left")

    return result.select(
        "symbol", "instrument_type", "as_of_date", "latest_close",
        "return_1d", "return_1w", "return_1m", "return_3m", "return_6m", "return_ytd",
        "return_1y", "return_3y", "return_5y",
        "cagr_3y", "cagr_5y", "volatility_1y", "sharpe_1y", "max_drawdown_1y",
    )