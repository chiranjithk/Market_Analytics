# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Same pattern/additions as
# gold_security_metrics.py -- as_of_date column, full period set restored,
# both return_Ny (total) and cagr_Ny (annualized) for 3y/5y.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "stock_market.silver"
GOLD = "stock_market.gold"

RISK_FREE_RATE = 0.07
PERIOD_DAYS = {"1d": 1, "1w": 7, "1m": 30, "3m": 91, "6m": 182, "1y": 365, "3y": 365 * 3, "5y": 365 * 5}
CAGR_YEARS = {"3y": 3, "5y": 5}


def _nav_asof(nav_with_latest, out_col, days=None, ytd=False):
    if ytd:
        cutoff = F.date_sub(F.make_date(F.year(F.col("latest_date")), F.lit(1), F.lit(1)), 1)
    else:
        cutoff = F.date_sub(F.col("latest_date"), days)

    asof_dates = (
        nav_with_latest.filter(F.col("date") <= cutoff)
        .groupBy("scheme_code").agg(F.max("date").alias("asof_date"))
    )
    return (
        asof_dates.join(nav_with_latest, "scheme_code")
        .filter(F.col("date") == F.col("asof_date"))
        .select("scheme_code", F.col("nav").alias(out_col))
    )


@dp.table(
    name=f"{GOLD}.scheme_metrics",
    comment="Per-scheme return/risk snapshot as of latest date.",
)
def scheme_metrics():
    nav = spark.read.table(f"{SILVER}.nav_daily")
    nav = nav.withColumn("latest_date", F.max("date").over(Window.partitionBy()))

    result = (
        nav.filter(F.col("date") == F.col("latest_date"))
        .select("scheme_code", F.col("date").alias("as_of_date"), F.col("nav").alias("latest_nav"))
    )

    for period, days in PERIOD_DAYS.items():
        result = result.join(_nav_asof(nav, f"nav_{period}", days=days), "scheme_code", "left")
    result = result.join(_nav_asof(nav, "nav_ytd", ytd=True), "scheme_code", "left")

    for period in list(PERIOD_DAYS.keys()) + ["ytd"]:
        result = result.withColumn(
            f"return_{period}",
            F.round((F.col("latest_nav") / F.col(f"nav_{period}") - 1) * 100, 2)
        )
    for period, years in CAGR_YEARS.items():
        result = result.withColumn(
            f"cagr_{period}",
            F.round((F.pow(F.col("latest_nav") / F.col(f"nav_{period}"), F.lit(1.0 / years)) - 1) * 100, 2)
        )

    w = Window.partitionBy("scheme_code").orderBy("date")
    daily_returns_1y = (
        nav.filter(F.col("date") > F.date_sub(F.col("latest_date"), 365))
        .withColumn("prev_nav", F.lag("nav").over(w))
        .withColumn("dr", F.col("nav") / F.col("prev_nav") - 1)
        .filter(F.col("dr").isNotNull())
    )
    risk = (
        daily_returns_1y.groupBy("scheme_code")
        .agg(
            (F.stddev("dr") * F.sqrt(F.lit(252))).alias("volatility_1y_raw"),
            (F.avg("dr") * F.lit(252)).alias("annualized_return_1y_raw"),
        )
        .withColumn("sharpe_1y", F.round((F.col("annualized_return_1y_raw") - F.lit(RISK_FREE_RATE)) / F.col("volatility_1y_raw"), 2))
        .withColumn("volatility_1y", F.round(F.col("volatility_1y_raw") * 100, 2))
        .select("scheme_code", "volatility_1y", "sharpe_1y")
    )
    result = result.join(risk, "scheme_code", "left")

    w_running_max = Window.partitionBy("scheme_code").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    mdd = (
        nav.filter(F.col("date") > F.date_sub(F.col("latest_date"), 365))
        .withColumn("running_max", F.max("nav").over(w_running_max))
        .withColumn("drawdown", F.col("nav") / F.col("running_max") - 1)
        .groupBy("scheme_code")
        .agg(F.round(F.min("drawdown") * 100, 2).alias("max_drawdown_1y"))
    )
    result = result.join(mdd, "scheme_code", "left")

    return result.select(
        "scheme_code", "as_of_date", "latest_nav",
        "return_1d", "return_1w", "return_1m", "return_3m", "return_6m", "return_ytd",
        "return_1y", "return_3y", "return_5y",
        "cagr_3y", "cagr_5y", "volatility_1y", "sharpe_1y", "max_drawdown_1y",
    )