from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.window import Window


@dp.materialized_view(
    name="silver_crypto_market",
    comment="Silver layer: validated, deduplicated, and transformed cryptocurrency market data",
)
@dp.expect("valid_coin_id", "coin_id IS NOT NULL")
@dp.expect("valid_symbol", "symbol IS NOT NULL")
@dp.expect("valid_price", "current_price IS NOT NULL AND current_price >= 0")
@dp.expect("valid_market_cap", "market_cap IS NOT NULL AND market_cap >= 0")
def silver_crypto_market():
    """
    Reads bronze data with batch read, applies data quality validation,
    deduplicates by (id, ingestion_date) keeping the latest ingestion_timestamp,
    and transforms columns for the silver layer.
    """
    bronze = spark.read.table("bronze_coingecko_market_data")

    # Data quality validation: filter out invalid records before deduplication
    df_validated = bronze.filter(
        (col("id").isNotNull())
        & (col("symbol").isNotNull())
        & (col("name").isNotNull())
        & (col("current_price").isNotNull())
        & (col("current_price") >= 0)
        & (col("market_cap").isNotNull())
        & (col("market_cap") >= 0)
    )

    # Deduplicate: keep latest record per coin per ingestion date
    window_spec = Window.partitionBy("id", "ingestion_date").orderBy(
        col("ingestion_timestamp").desc()
    )

    return (
        df_validated
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .select(
            col("id").alias("coin_id"),
            col("symbol"),
            col("name").alias("coin_name"),
            col("current_price"),
            col("market_cap"),
            col("market_cap_rank"),
            col("total_volume"),
            col("high_24h"),
            col("low_24h"),
            col("price_change_24h"),
            col("price_change_percentage_24h"),
            col("market_cap_change_24h"),
            col("market_cap_change_percentage_24h"),
            col("circulating_supply"),
            col("total_supply"),
            col("max_supply"),
            to_timestamp(col("last_updated")).alias("market_timestamp"),
            col("ingestion_timestamp"),
            col("ingestion_date"),
            col("source_system"),
        )
        .withColumn(
            "price_volatility_24h",
            (col("high_24h") - col("low_24h")) / col("low_24h") * 100,
        )
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("record_quality", lit("VALID"))
    )