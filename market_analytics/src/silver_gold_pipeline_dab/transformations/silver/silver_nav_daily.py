# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Same streaming + AUTO CDC pattern
# as silver_prices_daily.py. mf_nav_raw is append-only, and since mfapi
# returns full scheme history on every call, overlapping/duplicate rows are
# expected — AUTO CDC's upsert-on-key (scheme_code, date) handles that,
# keeping the latest by ingestion_timestamp.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = "stock_market.bronze"
SILVER = "stock_market.silver"


@dp.view
def nav_stream():
    raw = spark.readStream.table(f"{BRONZE}.mf_nav_raw")
    return (
        raw.withColumnsRenamed({"schemeCode": "scheme_code"})
        .withColumn("date", F.to_date(F.col("date")))
        .withColumn("nav", F.round(F.col("nav").cast("double"), 4))
        .withColumn("year", F.year(F.col("date")))
        .select("scheme_code", "date", "nav", "year", "ingestion_timestamp")
    )


dp.create_streaming_table(
    name=f"{SILVER}.nav_daily",
    expect_all_or_drop={
        "valid_nav": "nav > 0",
        "no_future_dates": "date <= current_date()",
    },
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.nav_daily",
    source="nav_stream",
    keys=["scheme_code", "date"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1,
)