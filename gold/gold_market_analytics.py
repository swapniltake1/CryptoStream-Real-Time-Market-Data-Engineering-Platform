# Databricks notebook source
# DBTITLE 1,Gold Layer - Market Analytics
# MAGIC %md
# MAGIC # Gold Layer: Market Analytics
# MAGIC
# MAGIC This notebook creates business-ready analytics datasets from silver layer data.
# MAGIC
# MAGIC ## Key Analytics:
# MAGIC * Market snapshots
# MAGIC * Price trends
# MAGIC * Top movers (gainers/losers)
# MAGIC * Market metrics
# MAGIC * Trading volume analysis

# COMMAND ----------

# DBTITLE 1,Read Silver Data
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Read silver table
silver_table = "main.crypto_silver.crypto_market"
df_silver = spark.table(silver_table)

print(f"✓ Read {df_silver.count()} records from silver layer")
print(f"Date range: {df_silver.agg(min('ingestion_date'), max('ingestion_date')).collect()[0]}")

# COMMAND ----------

# DBTITLE 1,Create Market Snapshot
# Create latest market snapshot
window_spec = Window.partitionBy("coin_id").orderBy(col("market_timestamp").desc())

df_market_snapshot = (
    df_silver
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
        current_timestamp().alias("snapshot_timestamp")
    )
)

print(f"✓ Market snapshot created: {df_market_snapshot.count()} coins")
display(df_market_snapshot.orderBy("market_cap_rank").limit(10))

# COMMAND ----------

# DBTITLE 1,Top Gainers and Losers
# Calculate Top Gainers
df_top_gainers = (
    df_market_snapshot
    .filter(col("price_change_percentage_24h").isNotNull())
    .orderBy(col("price_change_percentage_24h").desc())
    .limit(10)
    .select(
        row_number().over(Window.orderBy(col("price_change_percentage_24h").desc())).alias("rank"),
        "coin_name",
        "symbol",
        "current_price",
        "price_change_percentage_24h",
        "total_volume",
        "market_cap"
    )
)

print("✓ Top 10 Gainers (24h):")
display(df_top_gainers)

# Calculate Top Losers
df_top_losers = (
    df_market_snapshot
    .filter(col("price_change_percentage_24h").isNotNull())
    .orderBy(col("price_change_percentage_24h").asc())
    .limit(10)
    .select(
        row_number().over(Window.orderBy(col("price_change_percentage_24h").asc())).alias("rank"),
        "coin_name",
        "symbol",
        "current_price",
        "price_change_percentage_24h",
        "total_volume",
        "market_cap"
    )
)

print("✓ Top 10 Losers (24h):")
display(df_top_losers)

# COMMAND ----------

# DBTITLE 1,Market Metrics
# Calculate overall market metrics
df_market_metrics = df_market_snapshot.agg(
    sum("market_cap").alias("total_market_cap"),
    sum("total_volume").alias("total_24h_volume"),
    count("coin_id").alias("total_coins_tracked"),
    avg("price_change_percentage_24h").alias("avg_price_change_24h"),
    max("price_change_percentage_24h").alias("max_price_change_24h"),
    min("price_change_percentage_24h").alias("min_price_change_24h"),
    avg("price_volatility_24h").alias("avg_volatility_24h")
).withColumn("metric_timestamp", current_timestamp())

print("✓ Market Metrics:")
display(df_market_metrics)

# COMMAND ----------

# DBTITLE 1,Write Gold Tables
# Create gold schema
spark.sql("CREATE SCHEMA IF NOT EXISTS main.crypto_gold")

# Write market snapshot
gold_snapshot_table = "main.crypto_gold.market_snapshot"
df_market_snapshot.write.format("delta").mode("overwrite").saveAsTable(gold_snapshot_table)
print(f"✓ Wrote {df_market_snapshot.count()} records to {gold_snapshot_table}")

# Write top movers
gold_gainers_table = "main.crypto_gold.top_gainers"
df_top_gainers.write.format("delta").mode("overwrite").saveAsTable(gold_gainers_table)
print(f"✓ Wrote {df_top_gainers.count()} records to {gold_gainers_table}")

gold_losers_table = "main.crypto_gold.top_losers"
df_top_losers.write.format("delta").mode("overwrite").saveAsTable(gold_losers_table)
print(f"✓ Wrote {df_top_losers.count()} records to {gold_losers_table}")

# Write market metrics
gold_metrics_table = "main.crypto_gold.market_metrics"
df_market_metrics.write.format("delta").mode("append").saveAsTable(gold_metrics_table)
print(f"✓ Wrote market metrics to {gold_metrics_table}")

print("\n✓ Gold layer analytics complete!")

# COMMAND ----------

