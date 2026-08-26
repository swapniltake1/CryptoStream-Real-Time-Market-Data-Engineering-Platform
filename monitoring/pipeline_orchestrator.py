# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,CryptoStream Pipeline Orchestrator
# MAGIC %md
# MAGIC # CryptoStream Pipeline Orchestrator
# MAGIC
# MAGIC This notebook orchestrates the complete CryptoStream data pipeline execution in the correct sequence:
# MAGIC
# MAGIC ## Pipeline Execution Order:
# MAGIC 1. **Bronze Layer**: Ingest data from uploaded files into Bronze Delta table
# MAGIC 2. **Silver Layer**: Transform and cleanse data into Silver layer
# MAGIC 3. **Gold Layer**: Create aggregated analytics in Gold layer
# MAGIC 4. **Monitoring**: Run pipeline monitoring and data quality checks
# MAGIC
# MAGIC ## Usage:
# MAGIC Run all cells to execute the complete pipeline end-to-end.

# COMMAND ----------

# DBTITLE 1,Setup and Configuration
from datetime import datetime
import time

# Notebook paths
NOTEBOOKS = [
    {
        "name": "Bronze Ingestion",
        "path": "/Users/swapniltake1@outlook.com/CryptoStream-Real-Time-Market-Data-Engineering-Platform/bronze/bronze_from_external_ingestion",
        "timeout": 600  # 10 minutes
    },
    {
        "name": "Silver Transformation",
        "path": "/Users/swapniltake1@outlook.com/CryptoStream-Real-Time-Market-Data-Engineering-Platform/silver/silver_transformation",
        "timeout": 600
    },
    {
        "name": "Gold Analytics",
        "path": "/Users/swapniltake1@outlook.com/CryptoStream-Real-Time-Market-Data-Engineering-Platform/gold/gold_market_analytics",
        "timeout": 600
    },
    {
        "name": "Pipeline Monitoring",
        "path": "/Users/swapniltake1@outlook.com/CryptoStream-Real-Time-Market-Data-Engineering-Platform/monitoring/pipeline_monitoring",
        "timeout": 300  # 5 minutes
    }
]

print(f"Pipeline Orchestrator initialized at {datetime.now()}")
print(f"Total notebooks to execute: {len(NOTEBOOKS)}")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Execute Pipeline Notebooks
# Execute each notebook in sequence
results = []
start_time = time.time()

for i, notebook in enumerate(NOTEBOOKS, 1):
    print(f"\n{'='*80}")
    print(f"[{i}/{len(NOTEBOOKS)}] Executing: {notebook['name']}")
    print(f"Path: {notebook['path']}")
    print(f"Started at: {datetime.now()}")
    print(f"{'='*80}")
    
    try:
        notebook_start = time.time()
        
        # Run the notebook
        result = dbutils.notebook.run(
            notebook['path'],
            timeout_seconds=notebook['timeout']
        )
        
        notebook_duration = time.time() - notebook_start
        
        results.append({
            "name": notebook['name'],
            "status": "SUCCESS",
            "duration": notebook_duration,
            "result": result
        })
        
        print(f"\n✓ {notebook['name']} completed successfully")
        print(f"  Duration: {notebook_duration:.2f} seconds")
        
    except Exception as e:
        notebook_duration = time.time() - notebook_start
        error_msg = str(e)
        
        results.append({
            "name": notebook['name'],
            "status": "FAILED",
            "duration": notebook_duration,
            "error": error_msg
        })
        
        print(f"\n✗ {notebook['name']} failed")
        print(f"  Duration: {notebook_duration:.2f} seconds")
        print(f"  Error: {error_msg}")
        print(f"\n⚠ Pipeline execution stopped due to failure")
        break

total_duration = time.time() - start_time
print(f"\n{'='*80}")
print(f"Pipeline execution completed at {datetime.now()}")
print(f"Total duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Pipeline Execution Summary
# Display execution summary
print(f"\n{'='*80}")
print("PIPELINE EXECUTION SUMMARY")
print(f"{'='*80}\n")

success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
failed_count = sum(1 for r in results if r['status'] == 'FAILED')

for result in results:
    status_icon = "✓" if result['status'] == 'SUCCESS' else "✗"
    print(f"{status_icon} {result['name']:30} | {result['status']:10} | {result['duration']:.2f}s")
    if result['status'] == 'FAILED':
        print(f"  Error: {result['error']}")

print(f"\n{'='*80}")
print(f"Success: {success_count}/{len(NOTEBOOKS)} notebooks")
print(f"Failed:  {failed_count}/{len(NOTEBOOKS)} notebooks")

if failed_count == 0:
    print(f"\n🎉 All pipeline stages completed successfully!")
else:
    print(f"\n⚠ Pipeline completed with {failed_count} failure(s)")
    
print(f"{'='*80}")

# COMMAND ----------

