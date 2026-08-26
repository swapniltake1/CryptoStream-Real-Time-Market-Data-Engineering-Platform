from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold_top_losers",
    comment="Gold layer: top 10 cryptocurrency losers by 24h price change percentage",
)
def gold_top_losers():
    """
    Reads the gold market snapshot and ranks the top 10 coins
    by price_change_percentage_24h in ascending order.
    """
    snapshot = spark.read.table("gold_market_snapshot")

    return (
        snapshot
        .filter(col("price_change_percentage_24h").isNotNull())
        .orderBy(col("price_change_percentage_24h").asc())
        .limit(10)
        .select(
            row_number().over(
                Window.orderBy(col("price_change_percentage_24h").asc())
            ).alias("rank"),
            "coin_name",
            "symbol",
            "current_price",
            "price_change_percentage_24h",
            "total_volume",
            "market_cap",
        )
    )