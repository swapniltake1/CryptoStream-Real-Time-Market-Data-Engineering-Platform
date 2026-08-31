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

# DBTITLE 1,Setup Logger
import logging
import sys
from datetime import datetime

# Configure logger
class CustomFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels"""
    
    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    FORMATS = {
        logging.DEBUG: grey + "%(asctime)s - %(name)s - %(levelname)s - %(message)s" + reset,
        logging.INFO: blue + "%(asctime)s - %(name)s - %(levelname)s - %(message)s" + reset,
        logging.WARNING: yellow + "%(asctime)s - %(name)s - %(levelname)s - %(message)s" + reset,
        logging.ERROR: red + "%(asctime)s - %(name)s - %(levelname)s - %(message)s" + reset,
        logging.CRITICAL: bold_red + "%(asctime)s - %(name)s - %(levelname)s - %(message)s" + reset
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

# Create logger
logger = logging.getLogger('SilverTransformation')
logger.setLevel(logging.DEBUG)

# Remove existing handlers
if logger.handlers:
    logger.handlers.clear()

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(CustomFormatter())

# Add handler to logger
logger.addHandler(console_handler)

# Log pipeline start
logger.info("="*70)
logger.info("SILVER LAYER TRANSFORMATION - DATA QUALITY & VALIDATION")
logger.info("="*70)
logger.info(f"Pipeline started at: {datetime.now().isoformat()}")
logger.info("Logger initialized successfully")

print("\n✓ Logger configured and ready")

# COMMAND ----------

# DBTITLE 1,Read Bronze data
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

logger.info("STEP 1: Reading Bronze layer data")
logger.info("-" * 70)

try:
    # Read bronze table
    bronze_table = "main.crypto_bronze.coingecko_market_data"
    logger.info(f"Reading from Bronze table: {bronze_table}")
    
    df_bronze = spark.table(bronze_table)
    
    logger.info("Calculating bronze layer statistics...")
    bronze_count = df_bronze.count()
    latest_ingestion = df_bronze.agg(max('bronze_ingestion_timestamp')).collect()[0][0]
    
    logger.info(f"Bronze records loaded: {bronze_count:,}")
    logger.info(f"Latest ingestion timestamp: {latest_ingestion}")
    logger.debug(f"DataFrame schema: {df_bronze.schema}")
    
    print(f"✓ Read {bronze_count:,} records from bronze layer")
    print(f"Latest ingestion: {latest_ingestion}")
    
except Exception as e:
    logger.error(f"Error reading Bronze table: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
logger.info("STEP 2: Applying data quality validation rules")
logger.info("-" * 70)

try:
    logger.info("Defining validation rules...")
    logger.debug("Rules: Non-null ID/symbol/name/price/market_cap, positive prices/caps")
    
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
    
    logger.info("Calculating validation statistics...")
    valid_count = df_validated.count()
    rejected_count = bronze_count - valid_count
    rejection_rate = (rejected_count / bronze_count * 100) if bronze_count > 0 else 0
    
    logger.info(f"Valid records: {valid_count:,}")
    logger.info(f"Rejected records: {rejected_count:,}")
    logger.info(f"Rejection rate: {rejection_rate:.2f}%")
    
    if rejection_rate > 10:
        logger.warning(f"High rejection rate detected: {rejection_rate:.2f}%")
    
    print(f"✓ Validation complete")
    print(f"Valid records: {valid_count:,}")
    print(f"Rejected records: {rejected_count:,}")
    
except Exception as e:
    logger.error(f"Error during validation: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
logger.info("STEP 3: Removing duplicate records")
logger.info("-" * 70)

try:
    # Remove duplicates based on coin_id and ingestion date
    from pyspark.sql.window import Window
    
    logger.info("Creating deduplication window...")
    logger.debug("Partition by: id, ingestion_date | Order by: bronze_ingestion_timestamp DESC")
    
    # Create window to get latest record per coin per day
    window_spec = Window.partitionBy("id", "ingestion_date").orderBy(col("bronze_ingestion_timestamp").desc())
    
    logger.info("Applying deduplication logic...")
    df_deduped = (
        df_validated
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    logger.info("Calculating deduplication statistics...")
    unique_count = df_deduped.count()
    dup_count = valid_count - unique_count
    dup_rate = (dup_count / valid_count * 100) if valid_count > 0 else 0
    
    logger.info(f"Unique records: {unique_count:,}")
    logger.info(f"Duplicates removed: {dup_count:,}")
    logger.info(f"Duplication rate: {dup_rate:.2f}%")
    
    print(f"✓ Deduplication complete")
    print(f"Unique records: {unique_count:,}")
    print(f"Duplicates removed: {dup_count:,}")
    
except Exception as e:
    logger.error(f"Error during deduplication: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Standardize and Transform
logger.info("STEP 4: Standardizing columns and applying business transformations")
logger.info("-" * 70)

try:
    logger.info("Selecting and renaming columns...")
    logger.debug("Standardizing column names: id->coin_id, name->coin_name")
    
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
            col("bronze_ingestion_timestamp").alias("ingestion_timestamp"),
            col("ingestion_date"),
            col("source_system")
        )
        .withColumn("price_volatility_24h", 
                    (col("high_24h") - col("low_24h")) / col("low_24h") * 100)
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("record_quality", lit("VALID"))
    )
    
    silver_count = df_silver.count()
    column_count = len(df_silver.columns)
    
    logger.info("Business transformations applied:")
    logger.debug("  - Calculated price_volatility_24h metric")
    logger.debug("  - Added processing_timestamp")
    logger.debug("  - Added record_quality flag")
    logger.info(f"Silver DataFrame: {silver_count:,} rows, {column_count} columns")
    logger.debug(f"Schema: {df_silver.schema}")
    
    print(f"✓ Silver transformation complete")
    print(f"Total columns: {column_count}")
    display(df_silver.limit(10))
    
except Exception as e:
    logger.error(f"Error during transformation: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Write to Silver Table
logger.info("STEP 5: Writing to Silver Delta table")
logger.info("-" * 70)

try:
    # Create silver schema if not exists
    logger.info("Ensuring Silver schema exists...")
    spark.sql("CREATE SCHEMA IF NOT EXISTS main.crypto_silver")
    logger.debug("Schema main.crypto_silver ready")
    
    # Define silver table
    silver_table = "main.crypto_silver.crypto_market"
    logger.info(f"Target table: {silver_table}")
    
    # Write to Delta table with MERGE (upsert logic)
    logger.info("Creating temporary view for merge operation...")
    df_silver.createOrReplaceTempView("silver_updates")
    logger.debug("Temp view 'silver_updates' created")
    
    # Merge logic to handle updates
    logger.info("Preparing MERGE operation...")
    logger.debug("Merge key: coin_id + ingestion_date")
    
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
        logger.info("Executing MERGE operation...")
        spark.sql(merge_query)
        logger.info(f"Successfully merged {silver_count:,} records into {silver_table}")
        print(f"✓ Merged records into {silver_table}")
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            logger.warning(f"Table {silver_table} not found, creating new table...")
            df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)
            logger.info(f"Created and populated {silver_table} with {silver_count:,} records")
            print(f"✓ Created and populated {silver_table}")
        else:
            logger.error(f"MERGE operation failed: {str(e)}")
            raise e
    
    logger.info(f"Silver layer processing complete: {silver_count:,} records")
    print(f"Records processed: {silver_count:,}")
    
except Exception as e:
    logger.error(f"Error writing to Silver table: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Summary
