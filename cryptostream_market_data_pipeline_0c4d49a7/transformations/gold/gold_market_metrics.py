from pyspark import pipelines as dp
from pyspark.sql.functions import *


@dp.materialized_view(
    name="gold_market_metrics",
    comment="Gold layer: overall cryptocurrency market metrics from latest snapshot",
)
def gold_market_metrics():
    """
    Reads the gold market snapshot and computes aggregate market metrics:
    total market cap, 24h volume, coins tracked, average/max/min price changes,
    and average volatility.
    """
    snapshot = spark.read.table("gold_market_snapshot")

    return (
        snapshot
        .agg(
            sum("market_cap").alias("total_market_cap"),
            sum("total_volume").alias("total_24h_volume"),
            count("coin_id").alias("total_coins_tracked"),
            avg("price_change_percentage_24h").alias("avg_price_change_24h"),
            max("price_change_percentage_24h").alias("max_price_change_24h"),
            min("price_change_percentage_24h").alias("min_price_change_24h"),
            avg("price_volatility_24h").alias("avg_volatility_24h"),
        )
        .withColumn("metric_timestamp", current_timestamp())
    )