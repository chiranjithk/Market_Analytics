# -----------------------------------------------------------------------------
# Lakeflow Declarative Pipeline source file. Materialized pass-through of
# silver.security_master — trivial transform, but must be a real table
# (@dp.table) not a pipeline-internal @dp.view, since Power BI/Genie need to
# query it directly.
# -----------------------------------------------------------------------------
from pyspark import pipelines as dp

SILVER = "stock_market.silver"
GOLD = "stock_market.gold"


@dp.table(name=f"{GOLD}.dim_security", comment="Conformed stock/ETF/index dimension.")
def dim_security():
    return spark.read.table(f"{SILVER}.security_master")