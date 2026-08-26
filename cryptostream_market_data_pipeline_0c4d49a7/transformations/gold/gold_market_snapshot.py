from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(
    name="gold_market_snapshot",
    comment="Gold layer: latest market snapshot per cryptocurrency",
)
def gold_market_snapshot():
    """
    Reads silver data and creates the latest market snapshot per coin
    by selecting the most recent record (by market_timestamp) for each coin_id.
    """
    silver = spark.read.table("silver_crypto_market")

    window_spec = Window.partitionBy("coin_id").orderBy(col("market_timestamp").desc())

    return (
        silver
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .select(
            "coin_id",
            "symbol",
            "coin_name",
            "current_price",
            "market_cap",
            "market_cap_rank",
            "total_volume",
            "price_change_24h",
            "price_change_percentage_24h",
            "price_volatility_24h",
            "market_timestamp",
            current_timestamp().alias("snapshot_timestamp"),
        )
    )