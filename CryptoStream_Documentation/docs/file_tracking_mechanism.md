# File Tracking Mechanism

## Objective

Prevent duplicate file ingestion, preserve processing history, process files chronologically and allow failed files to be retried.

## Control Table

`main.crypto_bronze.processed_files_control`

Recommended fields:

```text
source_file
source_file_path
file_size
file_modified_time
processed_timestamp
batch_id
record_count
status
error_message
```

## Processing Pattern

```text
Landing files
   |
   v
Read SUCCESS filenames
   |
   v
Exclude successful files
   |
   v
Sort by modification time
   |
   v
Process oldest file
   |
   v
Write Bronze
   |
   v
Record SUCCESS
```

A failed file must not be marked SUCCESS. This allows a future run to retry it.

## Operational Principle

The control table provides file-level idempotency and auditability. For true streaming, evolve this pattern toward checkpoint-based processing and event-time-aware state management.
