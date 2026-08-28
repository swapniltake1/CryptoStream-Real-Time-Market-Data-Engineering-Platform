# CryptoStream Troubleshooting Runbook

## 1. API Connection Failure

### Symptoms

- connection error
- timeout
- HTTP failure

### Actions

1. Verify local internet connectivity.
2. Check CoinGecko availability.
3. Verify endpoint and parameters.
4. Check rate limits.
5. Retry after an appropriate delay.
6. Review ingestion logs.

## 2. JSON File Not Found in Databricks

### Actions

1. Verify the local script generated the file.
2. Confirm the file was uploaded/copied to the expected landing path.
3. Confirm filename extension is `.json`.
4. Verify Databricks path configuration.
5. Check file permissions.

## 3. Bronze File Already Processed

### Expected behavior

The Bronze process should skip a file whose control-table status is SUCCESS.

### Actions

Check:

```sql
SELECT *
FROM main.crypto_bronze.processed_files_control
ORDER BY processed_timestamp DESC;
```

## 4. Bronze Failure

Check:
- JSON structure
- metadata/data keys
- source file
- schema
- control-table status
- error message

A failed file should not be marked SUCCESS.

## 5. Silver Rejections

Check:
- null coin identifiers
- null price
- negative price
- null market cap
- malformed timestamps

Review the rejected-record count and source payload.

## 6. Duplicate Records

Check the expected grain.

For time-series processing:

```text
coin_id + market_timestamp
```

For daily processing, inspect the current coin/date deduplication behavior.

## 7. Gold Empty

Check:

```text
Silver row count
Silver latest timestamps
Gold notebook execution
Column names
```

Gold depends on valid Silver data.

## 8. Freshness Critical

Check:
- last successful ingestion
- latest Bronze ingestion timestamp
- API availability
- landing file arrival
- orchestrator status

## 9. Orchestrator Failure

Identify the first failed notebook. Fix the upstream issue before rerunning downstream stages.

## 10. Known Code Review Item

The current Bronze implementation should be reviewed for the `source_file` assignment where the intended processed-file variable is `file_to_process`. Correct this before production use.

## 11. Escalation Information

When reporting an incident include:

```text
pipeline_name
stage
batch_id
source_file
execution_time
error_message
records_read
records_processed
last_successful_run
```
