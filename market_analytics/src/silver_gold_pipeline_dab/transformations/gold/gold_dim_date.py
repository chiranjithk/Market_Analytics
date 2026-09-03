# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file.
# Standard calendar dimension. Materialized (@dp.table, not @dp.view) since
# Power BI and the AI agent need to query it directly outside the pipeline.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

GOLD = "stock_market.gold"


@dp.table(name=f"{GOLD}.dim_date", comment="Calendar dimension, 2000-01-01 through 5 years ahead.")
def dim_date():
    return (
        spark.sql("SELECT explode(sequence(to_date('2000-01-01'), date_add(current_date(), 1825), interval 1 day)) AS date")
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day_of_week", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
        .withColumn("fiscal_year", F.when(F.col("month") >= 4, F.col("year") + 1).otherwise(F.col("year")))
    )