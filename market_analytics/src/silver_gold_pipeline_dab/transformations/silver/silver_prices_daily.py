# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file — NOT a notebook you run directly.
# This gets picked up automatically once you point a pipeline's source folder
# at wherever this file lives in the workspace.
#
# Streaming read of Bronze (checkpoint-based — only processes new files since
# last pipeline run, no table scan) + AUTO CDC SCD1 upsert into Silver.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = "stock_market.bronze"
SILVER = "stock_market.silver"


def _clean(df, instrument_type: str, strip_ns_suffix: bool):
    df = (
        df.withColumn("date", F.to_date("date"))
        .withColumn("close", F.round(F.col("close").cast("double"), 4))
        .withColumn("high", F.round(F.col("high").cast("double"), 4))
        .withColumn("low", F.round(F.col("low").cast("double"), 4))
        .withColumn("open", F.round(F.col("open").cast("double"), 4))
        .withColumn("volume", F.col("volume").cast("long"))
        .withColumn("year", F.year("date"))
        .withColumn("ticker_symbol", F.col("symbol"))
        .withColumn("instrument_type", F.lit(instrument_type))
    )
    if strip_ns_suffix:
        df = df.withColumn("symbol", F.regexp_replace("ticker_symbol", r"\.NS$", ""))
    for c, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    return df.select("symbol", "date", "open", "high", "low", "close", "volume",
                      "ticker_symbol", "instrument_type", "year", "ingestion_timestamp")


@dp.view
def prices_union_stream():
    stock = spark.readStream.table(f"{BRONZE}.stock_price_raw")
    etf = spark.readStream.table(f"{BRONZE}.etf_price_raw")
    index = spark.readStream.table(f"{BRONZE}.index_price_raw")

    stock = _clean(stock, "STOCK", strip_ns_suffix=True)
    etf = _clean(etf, "ETF", strip_ns_suffix=True)
    index = _clean(index, "INDEX", strip_ns_suffix=False)

    return stock.unionByName(etf).unionByName(index)


dp.create_streaming_table(
    name=f"{SILVER}.prices_daily",
    expect_all_or_drop={
        "valid_price": "close > 0",
        "no_future_dates": "date <= current_date()",
    },
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.prices_daily",
    source="prices_union_stream",
    keys=["symbol", "date", "instrument_type"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1,
)