# CryptoStream Data Quality and Validation

## 1. Objective

The Data Quality framework ensures that invalid or incomplete source records do not silently become trusted analytical data.

## 2. Validation Categories

### Completeness

Required fields:

```text
coin_id
symbol
coin_name/name
current_price
market_cap
market_timestamp
```

### Numeric Validity

```text
current_price >= 0
market_cap >= 0
total_volume >= 0
```

### Range/Business Rules

- 24-hour high should be greater than or equal to 24-hour low when both are present.
- Percentage fields should be numeric.
- Market rank should be positive when present.
- Timestamps should be parseable.

### Uniqueness

For time-series processing, evaluate:

```text
coin_id + market_timestamp
```

For the current daily Silver implementation, the repository uses a coin/date-oriented deduplication strategy.

## 3. Quality Outcomes

Recommended classifications:

```text
VALID
INVALID
QUARANTINED
```

Accepted records continue to Silver/Gold. Invalid records should be persisted to a quarantine dataset in a future enhancement.

## 4. Reconciliation

For each batch compare:

```text
API records
    vs
Bronze records
    vs
Silver valid + rejected
    vs
Gold records
```

Any unexplained difference should trigger investigation.

## 5. Quality KPIs

- Null rate
- Duplicate rate
- Rejection rate
- Freshness lag
- Schema mismatch count
- Source-to-Bronze reconciliation
- Bronze-to-Silver reconciliation

## 6. Data Quality Gate

A future production implementation should fail a pipeline when critical thresholds are exceeded, for example:

```text
Critical null rate > 5%
Duplicate rate > 2%
Freshness lag > SLA
Unexpected schema change = true
```

Thresholds should be configurable rather than hardcoded.
