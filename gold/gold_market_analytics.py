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
logger = logging.getLogger('GoldAnalytics')
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
logger.info("GOLD LAYER ANALYTICS - BUSINESS INSIGHTS & METRICS")
logger.info("="*70)
logger.info(f"Pipeline started at: {datetime.now().isoformat()}")
logger.info("Logger initialized successfully")

print("\n✓ Logger configured and ready")

# COMMAND ----------

# DBTITLE 1,Read Silver Data
from pyspark.sql.functions import *
from pyspark.sql.window import Window

logger.info("STEP 1: Reading Silver layer data")
logger.info("-" * 70)

try:
    # Read silver table
    silver_table = "main.crypto_silver.crypto_market"
    logger.info(f"Reading from Silver table: {silver_table}")
    
    df_silver = spark.table(silver_table)
    
    logger.info("Calculating silver layer statistics...")
    silver_count = df_silver.count()
    date_range = df_silver.agg(min('ingestion_date'), max('ingestion_date')).collect()[0]
    min_date, max_date = date_range[0], date_range[1]
    
    logger.info(f"Silver records loaded: {silver_count:,}")
    logger.info(f"Date range: {min_date} to {max_date}")
    logger.debug(f"DataFrame schema: {df_silver.schema}")
    
    print(f"✓ Read {silver_count:,} records from silver layer")
    print(f"Date range: {min_date} to {max_date}")
    
except Exception as e:
    logger.error(f"Error reading Silver table: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Create Market Snapshot
logger.info("STEP 2: Creating latest market snapshot")
logger.info("-" * 70)

try:
    logger.info("Building window specification for latest records...")
    logger.debug("Partition by: coin_id | Order by: market_timestamp DESC")
    
    # Create latest market snapshot
    window_spec = Window.partitionBy("coin_id").orderBy(col("market_timestamp").desc())
    
    logger.info("Extracting latest market snapshot per coin...")
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
    
    snapshot_count = df_market_snapshot.count()
    logger.info(f"Market snapshot created: {snapshot_count:,} unique coins")
    logger.debug(f"Columns in snapshot: {len(df_market_snapshot.columns)}")
    
    print(f"✓ Market snapshot created: {snapshot_count:,} coins")
    display(df_market_snapshot.orderBy("market_cap_rank").limit(10))
    
except Exception as e:
    logger.error(f"Error creating market snapshot: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Top Gainers and Losers
logger.info("STEP 3: Calculating top gainers and losers")
logger.info("-" * 70)

try:
    # Calculate Top Gainers
    logger.info("Identifying top 10 gainers (24h)...")
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
    
    gainers_count = df_top_gainers.count()
    top_gainer = df_top_gainers.first()
    if top_gainer:
        logger.info(f"Top gainer: {top_gainer['coin_name']} (+{top_gainer['price_change_percentage_24h']:.2f}%)")
    logger.debug(f"Gainers DataFrame: {gainers_count} records")
    
    print("✓ Top 10 Gainers (24h):")
    display(df_top_gainers)
    
    # Calculate Top Losers
    logger.info("Identifying top 10 losers (24h)...")
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
    
    losers_count = df_top_losers.count()
    top_loser = df_top_losers.first()
    if top_loser:
        logger.info(f"Top loser: {top_loser['coin_name']} ({top_loser['price_change_percentage_24h']:.2f}%)")
    logger.debug(f"Losers DataFrame: {losers_count} records")
    
    print("✓ Top 10 Losers (24h):")
    display(df_top_losers)
    
except Exception as e:
    logger.error(f"Error calculating top movers: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Market Metrics
logger.info("STEP 4: Calculating overall market metrics")
logger.info("-" * 70)

try:
    logger.info("Aggregating market-wide statistics...")
    logger.debug("Metrics: total market cap, volume, price changes, volatility")
    
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
    
    # Log key metrics
    metrics = df_market_metrics.first()
    if metrics:
        logger.info(f"Total market cap: ${metrics['total_market_cap']:,.0f}")
        logger.info(f"Total 24h volume: ${metrics['total_24h_volume']:,.0f}")
        logger.info(f"Coins tracked: {metrics['total_coins_tracked']}")
        logger.info(f"Avg price change (24h): {metrics['avg_price_change_24h']:.2f}%")
        logger.debug(f"Max price change: {metrics['max_price_change_24h']:.2f}%")
        logger.debug(f"Min price change: {metrics['min_price_change_24h']:.2f}%")
        logger.debug(f"Avg volatility: {metrics['avg_volatility_24h']:.2f}%")
    
    print("✓ Market Metrics:")
    display(df_market_metrics)
    
except Exception as e:
    logger.error(f"Error calculating market metrics: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Write Gold Tables
logger.info("STEP 5: Writing to Gold Delta tables")
logger.info("-" * 70)

try:
    # Create gold schema
    logger.info("Ensuring Gold schema exists...")
    spark.sql("CREATE SCHEMA IF NOT EXISTS main.crypto_gold")
    logger.debug("Schema main.crypto_gold ready")
    
    # Write market snapshot
    gold_snapshot_table = "main.crypto_gold.market_snapshot"
    logger.info(f"Writing market snapshot to {gold_snapshot_table}...")
    logger.debug("Write mode: overwrite (replace entire snapshot)")
    
    df_market_snapshot.write.format("delta").mode("overwrite").saveAsTable(gold_snapshot_table)
    logger.info(f"Wrote {snapshot_count:,} records to {gold_snapshot_table}")
    print(f"✓ Wrote {snapshot_count:,} records to {gold_snapshot_table}")
    
    # Write top movers
    gold_gainers_table = "main.crypto_gold.top_gainers"
    logger.info(f"Writing top gainers to {gold_gainers_table}...")
    df_top_gainers.write.format("delta").mode("overwrite").saveAsTable(gold_gainers_table)
    logger.info(f"Wrote {gainers_count} records to {gold_gainers_table}")
    print(f"✓ Wrote {gainers_count} records to {gold_gainers_table}")
    
    gold_losers_table = "main.crypto_gold.top_losers"
    logger.info(f"Writing top losers to {gold_losers_table}...")
    df_top_losers.write.format("delta").mode("overwrite").saveAsTable(gold_losers_table)
    logger.info(f"Wrote {losers_count} records to {gold_losers_table}")
    print(f"✓ Wrote {losers_count} records to {gold_losers_table}")
    
    # Write market metrics
    gold_metrics_table = "main.crypto_gold.market_metrics"
    logger.info(f"Appending market metrics to {gold_metrics_table}...")
    logger.debug("Write mode: append (historical metrics tracking)")
    df_market_metrics.write.format("delta").mode("append").saveAsTable(gold_metrics_table)
    logger.info(f"Appended market metrics to {gold_metrics_table}")
    print(f"✓ Wrote market metrics to {gold_metrics_table}")
    
    logger.info("All Gold tables written successfully")
    print("\n✓ Gold layer analytics complete!")
    
except Exception as e:
    logger.error(f"Error writing to Gold tables: {str(e)}", exc_info=True)
    raise

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Summary
