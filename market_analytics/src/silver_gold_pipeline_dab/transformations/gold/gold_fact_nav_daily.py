# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Same pattern for MF NAV.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "stock_market.silver"
GOLD = "stock_market.gold"


@dp.table(name=f"{GOLD}.fact_nav_daily", comment="Daily NAV + day-over-day return.")
def fact_nav_daily():
    nav = spark.read.table(f"{SILVER}.nav_daily")
    w = Window.partitionBy("scheme_code").orderBy("date")
    return (
        nav
        .withColumn("prev_nav", F.lag("nav").over(w))
        .withColumn("daily_return", F.round((F.col("nav") / F.col("prev_nav") - 1) * 100, 4))
        .drop("prev_nav")
    )