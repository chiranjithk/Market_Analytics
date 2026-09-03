# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Materialized pass-through.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp

SILVER = "stock_market.silver"
GOLD = "stock_market.gold"


@dp.table(name=f"{GOLD}.dim_scheme", comment="Mutual fund scheme dimension.")
def dim_scheme():
    return spark.read.table(f"{SILVER}.scheme_master")