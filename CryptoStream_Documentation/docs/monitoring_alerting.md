# CryptoStream Monitoring and Alerting Guide

## 1. Monitoring Objectives

Monitor data freshness, pipeline execution, data volume and data quality.

## 2. Core Metrics

| Metric | Purpose |
|---|---|
| Freshness lag | Detect stale market data |
| Pipeline duration | Detect performance degradation |
| Input record count | Detect source-volume anomalies |
| Silver valid count | Detect quality changes |
| Rejected count | Detect malformed data |
| Duplicate count | Detect repeated observations |
| API failure count | Detect source availability issues |
| Last successful run | Detect pipeline outages |

## 3. Freshness

Recommended conceptual metric:

```text
current_time - max(ingestion_timestamp)
```

Suggested states:

```text
FRESH
STALE
CRITICAL
```

Thresholds should be configurable.

## 4. Alerts

Potential alerts:

- pipeline failed
- no successful run within SLA
- freshness exceeded threshold
- rejection rate exceeded threshold
- duplicate rate exceeded threshold
- schema changed unexpectedly
- API rate limit reached

## 5. Operational Dashboard

Recommended panels:

- Pipeline status
- Last successful run
- Data freshness
- Records ingested
- Records rejected
- Duplicate rate
- API errors
- Stage duration

## 6. Incident Response

When a pipeline fails:

1. Identify failed stage.
2. Check batch/file ID.
3. Inspect error message.
4. Verify source availability.
5. Check landing file.
6. Validate control-table status.
7. Retry failed processing.
8. Reconcile Bronze/Silver/Gold counts.
9. Document root cause.

## 7. Future Enhancement

Persist monitoring results into a Delta audit table and integrate alerting with enterprise notification channels.
