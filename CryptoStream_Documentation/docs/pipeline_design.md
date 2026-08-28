# CryptoStream Pipeline Design

## 1. End-to-End Flow

```text
1. Read source configuration
2. Call CoinGecko API
3. Validate HTTP response
4. Create batch metadata
5. Write timestamped JSON
6. Discover new landing files
7. Check processed-files control table
8. Load Bronze
9. Validate and deduplicate
10. Merge/write Silver
11. Build Gold datasets
12. Run monitoring
13. Record pipeline result
```

## 2. Pipeline Contracts

### External Ingestion Contract

Input: CoinGecko REST API.

Output: JSON envelope containing metadata and data.

Required metadata:
- project name
- source system
- endpoint
- target currency
- ingestion timestamp
- batch ID
- record count

### Bronze Contract

Input: New JSON landing file.

Output: Delta records plus lineage metadata.

Guarantee: A file is marked SUCCESS only after a successful Bronze write.

### Silver Contract

Input: Bronze Delta.

Output: Canonical, validated cryptocurrency market records.

Quality:
- required identity fields are present
- price is non-null and non-negative
- market cap is non-null and non-negative
- duplicates are controlled

### Gold Contract

Input: Silver.

Output:
- current market snapshot
- top gainers
- top losers
- market metrics

## 3. Incremental Processing

Bronze uses the processed-files control table to identify files that have already completed successfully.

```text
Landing Files
     |
     v
Read SUCCESS filenames
     |
     v
Exclude successful files
     |
     v
Process oldest remaining file
     |
     v
Write Bronze
     |
     v
Record SUCCESS
```

## 4. Idempotency

At file level, successful filenames are not reprocessed.

At record level, Silver uses Delta MERGE semantics around a defined logical grain. For true high-frequency time-series data, the recommended business key is:

`coin_id + market_timestamp`

## 5. Failure Handling

Failures should:
- be visible in logs
- not be marked SUCCESS
- preserve enough metadata for diagnosis
- be eligible for retry on the next run

## 6. Orchestration

Recommended dependency order:

```text
Bronze -> Silver -> Gold -> Monitoring
```

A downstream stage should not run when an upstream stage fails.

## 7. Recommended Future Controls

- exponential backoff for API failures
- 429 rate-limit handling
- schema-drift detection
- audit Delta table
- quarantine table for rejected records
- automated tests
- CI/CD
- Auto Loader/Structured Streaming
