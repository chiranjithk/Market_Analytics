# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Beta, alpha, correlation, and
# tracking error of each scheme vs its mapped benchmark, trailing 1y only
# (trimmed from 1y+3y for reliability -- add 3y back later, same pattern,
# once this is confirmed stable).
#
# latest_date is attached as a column via an unpartitioned window, single
# continuous DataFrame lineage -- no .collect(), no disconnected small
# DataFrame joined back in.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD = "stock_market.gold"
RISK_FREE_RATE = 0.07


@dp.table(
    name=f"{GOLD}.scheme_vs_benchmark",
    comment="Beta, alpha, correlation, tracking error of each scheme vs its mapped benchmark, trailing 1y.",
)
def scheme_vs_benchmark():
    scheme_returns = spark.read.table(f"{GOLD}.fact_nav_daily").select("scheme_code", "date", "daily_return")
    scheme_returns = scheme_returns.withColumn("latest_date", F.max("date").over(Window.partitionBy()))

    bench_map = spark.read.table(f"{GOLD}.scheme_benchmark_map").select("scheme_code", "benchmark_symbol")
    index_returns = (
        spark.read.table(f"{GOLD}.fact_price_daily")
        .filter(F.col("instrument_type") == "INDEX")
        .select(F.col("symbol").alias("benchmark_symbol"), "date", F.col("daily_return").alias("benchmark_return"))
    )

    paired = (
        scheme_returns.filter(F.col("date") > F.date_sub(F.col("latest_date"), 365))
        .withColumnRenamed("daily_return", "asset_return")
        .join(bench_map, "scheme_code")
        .join(index_returns, ["benchmark_symbol", "date"])
        .filter(F.col("asset_return").isNotNull() & F.col("benchmark_return").isNotNull())
    )

    result = (
        paired.groupBy("scheme_code", "benchmark_symbol")
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
        .select("scheme_code", "benchmark_symbol", "beta_1y", "alpha_1y", "correlation_1y", "tracking_error_1y")
    )

    return result