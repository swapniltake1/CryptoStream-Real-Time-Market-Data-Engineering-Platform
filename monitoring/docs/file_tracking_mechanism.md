# File Tracking Mechanism

## Overview

The CryptoStream pipeline implements a **file tracking mechanism** to ensure each data file is processed **exactly once**, preventing duplicate records in the Bronze Delta table.

## Problem Solved

Without file tracking:
- ❌ The same file gets processed every time the pipeline runs
- ❌ Creates duplicate records in Delta tables
- ❌ Wastes compute resources
- ❌ Corrupts analytics and reporting

## Solution: Control Table

### Control Table Structure

**Table**: `main.crypto_bronze.processed_files_control`

```sql
CREATE TABLE main.crypto_bronze.processed_files_control (
    file_name STRING,                    -- Name of the file (e.g., coingecko_market_20260826_130827.json)
    file_path STRING,                    -- Full path to the file
    file_size BIGINT,                    -- File size in bytes
    file_modification_time TIMESTAMP,    -- When the file was last modified
    processing_timestamp TIMESTAMP,      -- When we processed this file
    batch_id STRING,                     -- Batch ID from the file metadata
    record_count INT,                    -- Number of records processed from this file
    status STRING                        -- Processing status: SUCCESS or FAILED
)
USING DELTA
```

### How It Works

#### Step 1: List Available Files
```python
# List all JSON files in the data folder
files = dbutils.fs.ls(data_folder)
json_files = [f for f in files if f.name.endswith('.json')]
```

#### Step 2: Check Control Table
```python
# Get list of already processed files
processed_files_df = spark.table(control_table)
    .filter(col("status") == "SUCCESS")
processed_filenames = set([row.file_name for row in processed_files_df.collect()])
```

#### Step 3: Filter New Files
```python
# Filter out already processed files
new_files = [f for f in json_files if f.name not in processed_filenames]

if not new_files:
    print("No new files to process")
    dbutils.notebook.exit("No new files to process")
```

#### Step 4: Process Files in Order
```python
# Sort by modification time and get the oldest unprocessed file
# (Process files in chronological order)
sorted_files = sorted(new_files, key=lambda f: f.modificationTime)
file_to_process = sorted_files[0]
```

#### Step 5: Record Processed File
```python
# After successful processing, record the file
control_record = [{
    "file_name": file_name,
    "file_path": file_path,
    "file_size": file_size,
    "file_modification_time": file_mod_time,
    "processing_timestamp": datetime.now(),
    "batch_id": metadata['batch_id'],
    "record_count": len(data_records),
    "status": "SUCCESS"
}]

control_df = spark.createDataFrame(control_record, schema=control_schema)
control_df.write.format("delta").mode("append").saveAsTable(control_table)
```

## Benefits

✅ **Idempotent Processing**: Running the pipeline multiple times is safe
✅ **No Duplicates**: Each file processed exactly once
✅ **Auditability**: Complete history of all processed files
✅ **Chronological Order**: Files processed in order of creation
✅ **Error Recovery**: Can retry failed files without reprocessing successful ones
✅ **Monitoring**: Easy to see which files have been processed and when

## Usage Example

### First Run (New File)
```
Looking for data files in: .../external_ingestion/data/
============================================================
✓ Found 2 JSON file(s)

📊 Already processed: 0 file(s)
🆕 New files to process: 2
============================================================

New files to process:
  📄 coingecko_market_20260826_130827.json
     Size: 7,553 bytes
     Modified: 2026-08-26 13:11:06

  📄 coingecko_market_20260826_140000.json
     Size: 8,123 bytes
     Modified: 2026-08-26 14:00:00

============================================================
📌 Processing file: coingecko_market_20260826_130827.json
   (Processing oldest new file first)
============================================================
```

### Second Run (Same Files)
```
Looking for data files in: .../external_ingestion/data/
============================================================
✓ Found 2 JSON file(s)

📊 Already processed: 1 file(s)
🆕 New files to process: 1
============================================================

✓ No new files to process. All files have been ingested.

Already processed files:
  ✓ coingecko_market_20260826_130827.json
```

### Third Run (New File Added)
```
Looking for data files in: .../external_ingestion/data/
============================================================
✓ Found 3 JSON file(s)

📊 Already processed: 2 file(s)
🆕 New files to process: 1
============================================================

New files to process:
  📄 coingecko_market_20260826_150000.json
     Size: 7,891 bytes
     Modified: 2026-08-26 15:00:00

============================================================
📌 Processing file: coingecko_market_20260826_150000.json
   (Processing oldest new file first)
============================================================
```

## Querying the Control Table

### View All Processed Files
```sql
SELECT 
    file_name,
    processing_timestamp,
    batch_id,
    record_count,
    status
FROM main.crypto_bronze.processed_files_control
ORDER BY processing_timestamp DESC;
```

### Check Processing Statistics
```sql
SELECT 
    COUNT(*) as total_files_processed,
    SUM(record_count) as total_records_ingested,
    MIN(processing_timestamp) as first_processed,
    MAX(processing_timestamp) as last_processed
FROM main.crypto_bronze.processed_files_control
WHERE status = 'SUCCESS';
```

### Find Unprocessed Files
```python
# List files in the data folder
all_files = set([f.name for f in dbutils.fs.ls(data_folder) if f.name.endswith('.json')])

# Get processed files
processed = set([row.file_name for row in spark.table(control_table).collect()])

# Find difference
unprocessed = all_files - processed
print(f"Unprocessed files: {unprocessed}")
```

## Error Handling

If a file processing fails:
1. The error is caught and logged
2. The file is **not** recorded in the control table
3. Next run will retry the same file
4. Successfully processed files remain marked as processed

## Cleanup Strategy

### Archive Old Files (Optional)
```python
# Move processed files to archive folder after X days
archive_path = "/Workspace/.../external_ingestion/archive/"
processed_files = spark.table(control_table)
    .filter(col("processing_timestamp") < date_sub(current_date(), 30))
    .collect()

for row in processed_files:
    dbutils.fs.mv(row.file_path, f"{archive_path}/{row.file_name}")
```

### Purge Old Control Records (Optional)
```sql
-- Keep only last 90 days of control records
DELETE FROM main.crypto_bronze.processed_files_control
WHERE processing_timestamp < current_date() - INTERVAL 90 DAYS;
```

## Integration with Pipeline Orchestrator

The [pipeline_orchestrator](../pipeline_orchestrator) notebook runs all stages sequentially:

1. **Bronze Ingestion**: Processes only new files
2. **Silver Transformation**: Transforms all Bronze data
3. **Gold Analytics**: Creates aggregated analytics
4. **Monitoring**: Validates data quality

Running the orchestrator multiple times is safe - Bronze will skip already-processed files automatically.

## Summary

| Feature | Without Tracking | With Tracking |
|---------|------------------|---------------|
| Duplicate Prevention | ❌ No | ✅ Yes |
| Idempotent | ❌ No | ✅ Yes |
| Processing Order | ❌ Random | ✅ Chronological |
| Audit Trail | ❌ No | ✅ Yes |
| Error Recovery | ❌ Difficult | ✅ Easy |
| Resource Efficiency | ❌ Wasteful | ✅ Optimized |

---

**Next Steps**:
1. Upload new JSON files to `external_ingestion/data/`
2. Run the pipeline orchestrator
3. Check the control table to verify file tracking
4. Monitor for any duplicate records (there should be none!)
