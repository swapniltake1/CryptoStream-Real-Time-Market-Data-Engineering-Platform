# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Silver Layer - Data Transformation and Quality
# MAGIC %md
# MAGIC # Silver Layer: Data Transformation and Quality
# MAGIC
# MAGIC This notebook transforms bronze raw data into clean, validated silver layer data.
# MAGIC
# MAGIC ## Key Responsibilities:
# MAGIC * Parse JSON structures
# MAGIC * Enforce schema
# MAGIC * Standardize data types
# MAGIC * Remove duplicates
# MAGIC * Validate data quality
# MAGIC * Add business transformations

# COMMAND ----------

# DBTITLE 1,Read Bronze data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# Read bronze table
bronze_table = "main.crypto_bronze.coingecko_market_data"
df_bronze = spark.table(bronze_table)

print(f"✓ Read {df_bronze.count()} records from bronze layer")
print(f"Latest ingestion: {df_bronze.agg(max('ingestion_timestamp')).collect()[0][0]}")

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
# Apply data quality rules
df_validated = df_bronze.filter(
    (col("id").isNotNull()) &
    (col("symbol").isNotNull()) &
    (col("name").isNotNull()) &
    (col("current_price").isNotNull()) &
    (col("current_price") >= 0) &
    (col("market_cap").isNotNull()) &
    (col("market_cap") >= 0)
)

rejected_count = df_bronze.count() - df_validated.count()
print(f"✓ Validation complete")
print(f"Valid records: {df_validated.count()}")
print(f"Rejected records: {rejected_count}")

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
# Remove duplicates based on coin_id and ingestion date
from pyspark.sql.window import Window

# Create window to get latest record per coin per day
window_spec = Window.partitionBy("id", "ingestion_date").orderBy(col("ingestion_timestamp").desc())

df_deduped = (
    df_validated
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

dup_count = df_validated.count() - df_deduped.count()
print(f"✓ Deduplication complete")
print(f"Unique records: {df_deduped.count()}")
print(f"Duplicates removed: {dup_count}")

# COMMAND ----------

# DBTITLE 1,Standardize and Transform
# Standardize columns and add business transformations
df_silver = (
    df_deduped
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
        col("source_system")
    )
    .withColumn("price_volatility_24h", 
                (col("high_24h") - col("low_24h")) / col("low_24h") * 100)
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("record_quality", lit("VALID"))
)

print(f"✓ Silver transformation complete")
print(f"Total columns: {len(df_silver.columns)}")
display(df_silver.limit(10))

# COMMAND ----------

# DBTITLE 1,Write to Silver Table
# Create silver schema if not exists
spark.sql("CREATE SCHEMA IF NOT EXISTS main.crypto_silver")

# Define silver table
silver_table = "main.crypto_silver.crypto_market"

# Write to Delta table with MERGE (upsert logic)
df_silver.createOrReplaceTempView("silver_updates")

# Merge logic to handle updates
merge_query = f"""
MERGE INTO {silver_table} AS target
USING silver_updates AS source
ON target.coin_id = source.coin_id 
   AND target.ingestion_date = source.ingestion_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

# Create table if it doesn't exist, otherwise merge
try:
    spark.sql(merge_query)
    print(f"✓ Merged records into {silver_table}")
except Exception as e:
    if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
        df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        print(f"✓ Created and populated {silver_table}")
    else:
        raise e

record_count = df_silver.count()
print(f"Records processed: {record_count}")

# COMMAND ----------

