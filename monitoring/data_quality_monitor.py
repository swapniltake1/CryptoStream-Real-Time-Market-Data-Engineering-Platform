# Databricks notebook source
# DBTITLE 1,Data Quality Monitoring Dashboard
# MAGIC %md
# MAGIC # CryptoStream Data Quality Monitoring
# MAGIC
# MAGIC ## Purpose
# MAGIC Monitor data quality across bronze, silver, and gold layers.
# MAGIC
# MAGIC ## Checks:
# MAGIC * **Completeness**: Missing required fields
# MAGIC * **Validity**: Data within acceptable ranges
# MAGIC * **Freshness**: Data age and staleness
# MAGIC * **Consistency**: Cross-layer validation
# MAGIC * **Anomalies**: Unusual patterns or outliers
# MAGIC
# MAGIC ## Alert Thresholds:
# MAGIC * 🔴 Critical: > 5% invalid records
# MAGIC * 🟡 Warning: 1-5% invalid records
# MAGIC * 🟢 Healthy: < 1% invalid records

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import sys

# Add config path
sys.path.append("/Workspace/Users/swapniltake1@outlook.com/CryptoStream-Real-Time-Market-Data-Engineering-Platform/config")

try:
    from crypto_config import get_data_quality_config, get_pipeline_config
    dq_config = get_data_quality_config()
    pipeline_config = get_pipeline_config()
    print("✓ Configurations loaded")
except:
    print("⚠ Using default configuration")
    pipeline_config = type('obj', (object,), {
        'CATALOG_NAME': 'main',
        'BRONZE_SCHEMA': 'crypto_bronze',
        'SILVER_SCHEMA': 'crypto_silver',
        'GOLD_SCHEMA': 'crypto_gold',
        'BRONZE_TABLE': 'coingecko_market_data',
        'SILVER_TABLE': 'crypto_market'
    })

# Define table references
bronze_table = f"{pipeline_config.CATALOG_NAME}.{pipeline_config.BRONZE_SCHEMA}.{pipeline_config.BRONZE_TABLE}"
silver_table = f"{pipeline_config.CATALOG_NAME}.{pipeline_config.SILVER_SCHEMA}.{pipeline_config.SILVER_TABLE}"

print(f"\nMonitoring tables:")
print(f"  Bronze: {bronze_table}")
print(f"  Silver: {silver_table}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer Quality Checks
print("=" * 70)
print("BRONZE LAYER DATA QUALITY")
print("=" * 70)

try:
    df_bronze = spark.table(bronze_table)
    
    # Overall statistics
    total_records = df_bronze.count()
    latest_ingestion = df_bronze.agg(max("ingestion_timestamp")).collect()[0][0]
    
    print(f"\n📊 Overall Statistics:")
    print(f"  Total records: {total_records:,}")
    print(f"  Latest ingestion: {latest_ingestion}")
    
    # Check for null values in critical fields
    critical_fields = ["id", "symbol", "name", "current_price", "market_cap", "last_updated"]
    
    null_counts = df_bronze.select(
        *[sum(when(col(field).isNull(), 1).otherwise(0)).alias(field) for field in critical_fields]
    ).collect()[0].asDict()
    
    print(f"\n🔍 Null Value Check:")
    has_nulls = False
    for field, count in null_counts.items():
        pct = (count / total_records * 100) if total_records > 0 else 0
        status = "✗" if count > 0 else "✓"
        print(f"  {status} {field}: {count} nulls ({pct:.2f}%)")
        if count > 0:
            has_nulls = True
    
    if not has_nulls:
        print("  🎉 No nulls detected in critical fields")
    
    # Data freshness check
    if latest_ingestion:
        age_hours = (datetime.now() - latest_ingestion.replace(tzinfo=None)).total_seconds() / 3600
        print(f"\n⏰ Data Freshness:")
        print(f"  Latest data age: {age_hours:.1f} hours")
        
        if age_hours > 24:
            print("  ⚠ WARNING: Data is stale (> 24 hours old)")
        elif age_hours > 6:
            print("  🟡 CAUTION: Data aging (> 6 hours old)")
        else:
            print("  ✓ Data is fresh")
    
    # Recent ingestion summary
    print(f"\n📊 Recent Ingestions (Last 7 days):")
    display(
        df_bronze
        .filter(col("ingestion_date") >= date_sub(current_date(), 7))
        .groupBy("ingestion_date", "data_source_type")
        .agg(
            count("*").alias("record_count"),
            countDistinct("id").alias("unique_coins")
        )
        .orderBy(col("ingestion_date").desc())
    )
    
except Exception as e:
    print(f"✗ Bronze layer check failed: {e}")

# COMMAND ----------

# DBTITLE 1,Silver Layer Quality Checks
print("\n" + "=" * 70)
print("SILVER LAYER DATA QUALITY")
print("=" * 70)

try:
    df_silver = spark.table(silver_table)
    
    # Overall statistics
    total_records = df_silver.count()
    print(f"\n📊 Overall Statistics:")
    print(f"  Total records: {total_records:,}")
    
    # Validation checks
    invalid_price = df_silver.filter(
        (col("current_price").isNull()) | (col("current_price") < 0)
    ).count()
    
    invalid_market_cap = df_silver.filter(
        (col("market_cap").isNull()) | (col("market_cap") < 0)
    ).count()
    
    invalid_records = df_silver.filter(
        (col("current_price").isNull()) | (col("current_price") < 0) |
        (col("market_cap").isNull()) | (col("market_cap") < 0)
    ).count()
    
    valid_pct = ((total_records - invalid_records) / total_records * 100) if total_records > 0 else 0
    invalid_pct = (invalid_records / total_records * 100) if total_records > 0 else 0
    
    print(f"\n🔍 Validation Results:")
    print(f"  Valid records: {total_records - invalid_records:,} ({valid_pct:.2f}%)")
    print(f"  Invalid price: {invalid_price} ({(invalid_price/total_records*100):.2f}%)")
    print(f"  Invalid market cap: {invalid_market_cap} ({(invalid_market_cap/total_records*100):.2f}%)")
    
    # Quality status
    if invalid_pct > 5:
        print("\n  🔴 CRITICAL: High invalid record rate")
    elif invalid_pct > 1:
        print("\n  🟡 WARNING: Elevated invalid record rate")
    else:
        print("\n  🟢 HEALTHY: Low invalid record rate")
    
    # Anomaly detection - Price changes > 50%
    print(f"\n🔍 Anomaly Detection:")
    
    large_price_changes = df_silver.filter(
        abs(col("price_change_percentage_24h")) > 50
    ).count()
    
    print(f"  Large price changes (>50%): {large_price_changes}")
    
    if large_price_changes > 0:
        print("\n  🔺 Top Price Movers (Potential Anomalies):")
        display(
            df_silver
            .filter(abs(col("price_change_percentage_24h")) > 50)
            .select(
                "coin_name", "symbol", "current_price",
                "price_change_percentage_24h", "market_timestamp"
            )
            .orderBy(abs(col("price_change_percentage_24h")).desc())
            .limit(10)
        )
    
    # Quality metrics by coin
    print(f"\n📊 Quality Metrics by Coin:")
    display(
        df_silver
        .groupBy("coin_name")
        .agg(
            count("*").alias("total_records"),
            sum(when(col("record_quality") == "VALID", 1).otherwise(0)).alias("valid_records"),
            max("market_timestamp").alias("latest_update"),
            avg("current_price").alias("avg_price"),
            stddev("price_change_percentage_24h").alias("price_volatility")
        )
        .withColumn("quality_pct", (col("valid_records") / col("total_records") * 100))
        .orderBy(col("total_records").desc())
    )
    
except Exception as e:
    print(f"✗ Silver layer check failed: {e}")

# COMMAND ----------

# DBTITLE 1,Cross-Layer Consistency Check
print("\n" + "=" * 70)
print("CROSS-LAYER CONSISTENCY")
print("=" * 70)

try:
    # Compare record counts
    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    
    print(f"\n🔄 Record Count Comparison:")
    print(f"  Bronze layer: {bronze_count:,} records")
    print(f"  Silver layer: {silver_count:,} records")
    
    if bronze_count > silver_count:
        rejected = bronze_count - silver_count
        rejection_rate = (rejected / bronze_count * 100)
        print(f"  Rejected: {rejected:,} records ({rejection_rate:.2f}%)")
        
        if rejection_rate > 10:
            print("  ⚠ WARNING: High rejection rate (> 10%)")
    
    # Check data flow timing
    latest_bronze = spark.table(bronze_table).agg(max("ingestion_timestamp")).collect()[0][0]
    latest_silver = spark.table(silver_table).agg(max("ingestion_timestamp")).collect()[0][0]
    
    if latest_bronze and latest_silver:
        lag_seconds = (latest_bronze - latest_silver).total_seconds()
        print(f"\n⏱️ Processing Lag:")
        print(f"  Latest bronze: {latest_bronze}")
        print(f"  Latest silver: {latest_silver}")
        print(f"  Lag: {lag_seconds:.0f} seconds")
        
        if lag_seconds > 3600:  # 1 hour
            print("  ⚠ WARNING: Silver layer is significantly behind bronze")
        else:
            print("  ✓ Processing lag is acceptable")
    
except Exception as e:
    print(f"✗ Consistency check failed: {e}")

# COMMAND ----------

# DBTITLE 1,Summary and Recommendations
print("\n" + "=" * 70)
print("SUMMARY & RECOMMENDATIONS")
print("=" * 70)

try:
    bronze_count = spark.table(bronze_table).count()
    silver_count = spark.table(silver_table).count()
    
    invalid_silver = spark.table(silver_table).filter(
        (col("current_price").isNull()) | (col("current_price") < 0) |
        (col("market_cap").isNull()) | (col("market_cap") < 0)
    ).count()
    
    quality_score = ((silver_count - invalid_silver) / silver_count * 100) if silver_count > 0 else 0
    
    print(f"\n🎯 Overall Data Quality Score: {quality_score:.1f}%")
    
    print("\n📝 Recommendations:")
    
    if quality_score < 95:
        print("  ⚠ Investigate and fix data quality issues in silver layer")
    else:
        print("  ✓ Data quality is excellent")
    
    if bronze_count == 0:
        print("  ⚠ No data in bronze layer - check ingestion pipeline")
    
    latest_bronze = spark.table(bronze_table).agg(max("ingestion_timestamp")).collect()[0][0]
    if latest_bronze:
        age_hours = (datetime.now() - latest_bronze.replace(tzinfo=None)).total_seconds() / 3600
        if age_hours > 24:
            print("  ⚠ Data is stale - verify ingestion schedule is running")
    
    print("\n✓ Data quality monitoring complete")
    
except Exception as e:
    print(f"✗ Summary generation failed: {e}")

print("\n" + "=" * 70)

# COMMAND ----------

