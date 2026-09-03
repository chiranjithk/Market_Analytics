# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Adds daily_return to the raw
# Silver prices — the building block both Power BI DAX measures and the
# security_metrics snapshot below use.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "stock_market.silver"
GOLD = "stock_market.gold"


@dp.table(name=f"{GOLD}.fact_price_daily", comment="Daily OHLCV + day-over-day return, all instrument types.")
def fact_price_daily():
    prices = spark.read.table(f"{SILVER}.prices_daily")
    w = Window.partitionBy("symbol", "instrument_type").orderBy("date")
    return (
        prices
        .withColumn("prev_close", F.lag("close").over(w))
        .withColumn("daily_return", F.round((F.col("close") / F.col("prev_close") - 1) * 100, 4))
        .drop("prev_close")
    )