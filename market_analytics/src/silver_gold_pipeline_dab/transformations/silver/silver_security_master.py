# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file.
#
# Bronze master tables are OVERWRITTEN each load (current-state snapshot, no
# history). Silver mirrors that: a plain materialized view, fully recomputed
# from Bronze each pipeline run. No streaming, no CDC, no SCD2 history for
# these — deliberate simplification given how small/rarely-changing this data
# is. If per-symbol attribute history is needed later, this is the file to
# revisit, not something the rest of the project depends on.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from datetime import datetime

BRONZE = "stock_market.bronze"
SILVER = "stock_market.silver"


@dp.table(
    name=f"{SILVER}.security_master",
    comment="Conformed stock+ETF+index master, current state only (no history).",
)
@dp.expect_all_or_drop({"valid_symbol": "symbol IS NOT NULL", "valid_name": "name IS NOT NULL"})
def security_master():
    stock = (
        spark.read.table(f"{BRONZE}.stock_master_raw")
        .dropna(subset=["Symbol"]).dropDuplicates(["Symbol"])
        .withColumn("listing_date", F.to_date(F.col("Listing_date"), "dd-MMM-yy"))
        .withColumn(
            "listing_date",
            F.when(F.col("listing_date") > datetime.now(), F.col("listing_date") - F.expr("INTERVAL 100 YEAR"))
             .otherwise(F.col("listing_date"))
        )
        .select(
            F.trim(F.col("Symbol")).alias("symbol"),
            F.trim(F.col("Name")).alias("name"),
            F.lit("STOCK").alias("instrument_type"),
            F.trim(F.col("Series")).alias("sub_type"),
            F.lit("NSE").alias("exchange"),
            F.trim(F.col("isin_number")).alias("isin_number"),
        )
    )

    etf = (
        spark.read.table(f"{BRONZE}.etf_master_raw")
        .dropna(subset=["Symbol"]).dropDuplicates(["Symbol"])
        .select(
            F.trim(F.col("Symbol")).alias("symbol"),
            F.trim(F.col("Name")).alias("name"),
            F.lit("ETF").alias("instrument_type"),
            F.trim(F.col("Sub_Type")).alias("sub_type"),
            F.lit("NSE").alias("exchange"),
            F.lit(None).cast("string").alias("isin_number"),
        )
    )

    index = (
        spark.read.table(f"{BRONZE}.index_master_raw")
        .dropna(subset=["Symbol"]).dropDuplicates(["Symbol"])
        .select(
            F.trim(F.col("Symbol")).alias("symbol"),
            F.trim(F.col("Name")).alias("name"),
            F.lit("INDEX").alias("instrument_type"),
            F.trim(F.col("Asset_Type")).alias("sub_type"),
            F.trim(F.col("Exchange")).alias("exchange"),
            F.lit(None).cast("string").alias("isin_number"),
        )
    )

    return stock.unionByName(etf).unionByName(index)