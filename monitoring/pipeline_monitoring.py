# Databricks notebook source
# DBTITLE 1,Pipeline Monitoring and Observability
# MAGIC %md
# MAGIC # CryptoStream Pipeline Monitoring
# MAGIC
# MAGIC This notebook provides observability into the data pipeline health, data quality, and freshness.
# MAGIC
# MAGIC ## Monitoring Areas:
# MAGIC * Pipeline execution status
# MAGIC * Data freshness
# MAGIC * Record counts across layers
# MAGIC * Data quality metrics
# MAGIC * API ingestion health

# COMMAND ----------

# DBTITLE 1,Check Data Freshness
from pyspark.sql.functions import *
from datetime import datetime, timedelta

# Check Bronze layer freshness
bronze_table = "main.crypto_bronze.coingecko_market_data"
df_bronze = spark.table(bronze_table)

last_ingestion = df_bronze.agg(max("ingestion_timestamp")).collect()[0][0]
time_since_last = datetime.now() - last_ingestion

freshness_status = "FRESH" if time_since_last < timedelta(hours=1) else \
                   "STALE" if time_since_last < timedelta(hours=24) else "CRITICAL"

print(f"\n{'='*60}")
print(f"BRONZE LAYER FRESHNESS")
print(f"{'='*60}")
print(f"Last Ingestion: {last_ingestion}")
print(f"Time Since: {time_since_last}")
print(f"Status: {freshness_status}")
print(f"{'='*60}\n")

# COMMAND ----------

# DBTITLE 1,Record Counts Across Layers
# Count records across all layers
print(f"\n{'='*60}")
print(f"RECORD COUNTS BY LAYER")
print(f"{'='*60}")

try:
    bronze_count = spark.table("main.crypto_bronze.coingecko_market_data").count()
    print(f"Bronze Layer: {bronze_count:,} records")
except:
    print(f"Bronze Layer: Table not found")

try:
    silver_count = spark.table("main.crypto_silver.crypto_market").count()
    print(f"Silver Layer: {silver_count:,} records")
except:
    print(f"Silver Layer: Table not found")

try:
    gold_count = spark.table("main.crypto_gold.market_snapshot").count()
    print(f"Gold Layer: {gold_count:,} records")
except:
    print(f"Gold Layer: Table not found")

print(f"{'='*60}\n")

# COMMAND ----------

# DBTITLE 1,Data Quality Metrics
# Check data quality in silver layer
try:
    df_silver = spark.table("main.crypto_silver.crypto_market")
    
    quality_metrics = df_silver.agg(
        count("*").alias("total_records"),
        count(when(col("current_price").isNull(), 1)).alias("null_prices"),
        count(when(col("market_cap").isNull(), 1)).alias("null_market_caps"),
        count(when(col("current_price") < 0, 1)).alias("negative_prices"),
        avg("price_change_percentage_24h").alias("avg_price_change_24h"),
        max("price_volatility_24h").alias("max_volatility_24h")
    ).collect()[0]
    
    print(f"\n{'='*60}")
    print(f"DATA QUALITY METRICS (SILVER LAYER)")
    print(f"{'='*60}")
    print(f"Total Records: {quality_metrics['total_records']:,}")
    print(f"Null Prices: {quality_metrics['null_prices']:,}")
    print(f"Null Market Caps: {quality_metrics['null_market_caps']:,}")
    print(f"Negative Prices: {quality_metrics['negative_prices']:,}")
    print(f"Avg 24h Change: {quality_metrics['avg_price_change_24h']:.2f}%")
    print(f"Max Volatility: {quality_metrics['max_volatility_24h']:.2f}%")
    print(f"{'='*60}\n")
except Exception as e:
    print(f"Silver layer not available: {e}")

# COMMAND ----------

