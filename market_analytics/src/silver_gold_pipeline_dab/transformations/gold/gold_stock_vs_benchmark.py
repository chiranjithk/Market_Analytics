# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Same fixed pattern, applied to
# stocks/ETFs against a fixed benchmark (NIFTY 50). Trailing 1y only.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD = "stock_market.gold"
RISK_FREE_RATE = 0.07
DEFAULT_BENCHMARK = "^NSEI"


@dp.table(
    name=f"{GOLD}.stock_vs_benchmark",
    comment="Beta, alpha, correlation, tracking error of each stock/ETF vs NIFTY 50, trailing 1y.",
)
def stock_vs_benchmark():
    prices = spark.read.table(f"{GOLD}.fact_price_daily")
    prices = prices.withColumn("latest_date", F.max("date").over(Window.partitionBy()))

    asset_returns = prices.filter(F.col("instrument_type").isin(["STOCK", "ETF"])) \
        .select("symbol", "date", "daily_return", "latest_date")
    bench_returns = (
        prices.filter((F.col("instrument_type") == "INDEX") & (F.col("symbol") == DEFAULT_BENCHMARK))
        .select("date", F.col("daily_return").alias("benchmark_return"))
    )

    paired = (
        asset_returns.filter(F.col("date") > F.date_sub(F.col("latest_date"), 365))
        .withColumnRenamed("daily_return", "asset_return")
        .join(bench_returns, "date")
        .filter(F.col("asset_return").isNotNull() & F.col("benchmark_return").isNotNull())
    )

    result = (
        paired.groupBy("symbol")
        .agg(
            F.covar_samp("asset_return", "benchmark_return").alias("cov"),
            F.var_samp("benchmark_return").alias("bench_var"),
            F.round(F.corr("asset_return", "benchmark_return"), 2).alias("correlation_1y"),
            (F.avg("asset_return") * 252).alias("ann_asset_return"),
            (F.avg("benchmark_return") * 252).alias("ann_bench_return"),
            (F.stddev(F.col("asset_return") - F.col("benchmark_return")) * F.sqrt(F.lit(252))).alias("te_raw"),
        )
        .withColumn("beta_1y", F.round(F.col("cov") / F.col("bench_var"), 2))
        .withColumn(
            "alpha_1y",
            F.round((F.col("ann_asset_return") - (F.lit(RISK_FREE_RATE) + F.col("beta_1y") * (F.col("ann_bench_return") - F.lit(RISK_FREE_RATE)))) * 100, 2)
        )
        .withColumn("tracking_error_1y", F.round(F.col("te_raw") * 100, 2))
        .withColumn("benchmark_symbol", F.lit(DEFAULT_BENCHMARK))
        .select("symbol", "benchmark_symbol", "beta_1y", "alpha_1y", "correlation_1y", "tracking_error_1y")
    )

    return result