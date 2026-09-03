# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Same plain-recompute pattern as
# silver_security_master.py — no streaming, no CDC, current state only.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE = "stock_market.bronze"
SILVER = "stock_market.silver"


@dp.table(
    name=f"{SILVER}.scheme_master",
    comment="Conformed MF scheme master, current state only (no history).",
)
@dp.expect_all_or_drop({"valid_code": "scheme_code IS NOT NULL"})
def scheme_master():
    raw = spark.read.table(f"{BRONZE}.mf_scheme_master_raw")
    df = (
        raw.dropna(subset=["schemeCode"]).dropDuplicates(["schemeCode"])
        .withColumnsRenamed({
            "schemeCode": "scheme_code",
            "schemeName": "scheme_name",
            "isinGrowth": "isin_growth",
            "fundHouse": "fund_house",
            "schemeCategory": "scheme_category",
            "schemeType": "scheme_type",
            "isinDivReinvestment": "isin_div_reinvestment",
        })
        .select("scheme_code", "scheme_name", "isin_growth", "fund_house",
                "scheme_category", "scheme_type", "isin_div_reinvestment")
    )
    for c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df