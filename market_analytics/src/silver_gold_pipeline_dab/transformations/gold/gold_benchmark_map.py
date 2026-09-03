# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file.
# Maps each MF scheme to a benchmark index symbol based on scheme_category
# text matching. Deliberately simple/editable — adjust the rlike patterns or
# mappings below to match your view of correct benchmarks (e.g. you may want
# Large Cap -> NIFTY 100 instead of NIFTY 500 depending on convention).
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql import functions as F

GOLD = "stock_market.gold"


@dp.table(name=f"{GOLD}.scheme_benchmark_map", comment="Scheme -> benchmark index symbol, by category.")
def scheme_benchmark_map():
    scheme = spark.read.table(f"{GOLD}.dim_scheme")
    return scheme.select(
        "scheme_code",
        "scheme_category",
        F.when(F.col("scheme_category").rlike("(?i)large cap"), F.lit("^CRSLDX"))       # NIFTY 500
         .when(F.col("scheme_category").rlike("(?i)mid cap"), F.lit("^CNXMIDCAP"))      # NIFTY MIDCAP 100
         .when(F.col("scheme_category").rlike("(?i)small cap"), F.lit("^CNXSC"))        # NIFTY SMALLCAP 100
         .when(F.col("scheme_category").rlike("(?i)flexi cap|multi cap|elss"), F.lit("^CRSLDX"))
         .otherwise(F.lit("^NSEI"))                                                      # NIFTY 50 default
         .alias("benchmark_symbol")
    )