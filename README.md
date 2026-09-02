# CryptoStream

[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/) [![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/docs/latest/api/python)

[![Docs](https://img.shields.io/badge/Docs-CryptoStream_Documentation-blue)](https://github.com/swapniltake1/CryptoStream-Real-Time-Market-Data-Engineering-Platform/blob/main/CryptoStream_Documentation/docs/README.md)

Professional, production-oriented data engineering platform for ingesting, validating, processing, and analyzing cryptocurrency market data using the CoinGecko API and a Databricks Lakehouse architecture.

---

Table of Contents
- Project Overview
- What's Changed
- Architecture & Flow diagrams
- Key Features
- Data Layers (Medallion)
- Time-Series & Analytics
- Incremental & Idempotent Processing
- Observability & Audit
- Getting Started
- Configuration
- Testing & CI
- Documentation
- Contributing
- License & Author

---

## Project Overview

CryptoStream is an enterprise-style, metadata-driven data engineering platform that converts continuous cryptocurrency market feeds into reliable, analytics-ready datasets. It is designed for scalable, testable, and production-grade deployments on Databricks / Delta Lake or similar Spark-compatible lakehouses.

Primary goals:
- Ingest market data from CoinGecko reliably
- Preserve raw source payloads for traceability
- Enforce schema and validation rules
- Provide idempotent, incremental updates into Delta Lake
- Expose business-ready Gold tables for analytics and dashboards

---

## What's Changed

Recent code changes have improved ingestion robustness, made the transformation pipelines more idempotent, and added observability metadata to every pipeline run. Key updates:
- Improved API client: better retry/backoff and rate-limit handling
- Standardized ingestion metadata (batch_id, request_id, ingestion_timestamp)
- Enhanced Silver transformations: stricter schema checks and deterministic business keys
- MERGE-based upserts for Silver->Gold with transactional guarantees
- Additional operational metrics emitted for monitoring (records_read, records_processed, pipeline_duration)

These changes are reflected in the updated pipeline code, tests, and configuration files. Update your `config/` entries and CI if you customized the previous behavior.

---

## Architecture & Flow diagrams

Below are two diagrams to help understand the end-to-end flow and the component architecture. They are provided as mermaid diagrams so you can render them in markdown-aware viewers; if your renderer does not support mermaid, the textual explanation following each diagram describes the same flow.

Architecture flow (high-level):

```mermaid
flowchart LR
  A[CoinGecko API] -->|HTTP Poll / Batch| B[Ingestion Service]
  B --> C[Bronze (Delta) - raw_payloads]
  C --> D[Silver - parsed & validated]
  D --> E[Gold - analytics & aggregates]
  E --> F[Databricks SQL / Dashboards]
  B -->|metrics & logs| G[Monitoring & Alerting]
  D --> H[Quarantine / Reprocess Queue]
  style A fill:#f9f,stroke:#333,stroke-width:1px
  style B fill:#bbf,stroke:#333,stroke-width:1px
  style C fill:#eee,stroke:#333,stroke-width:1px
  style D fill:#ffd,stroke:#333,stroke-width:1px
  style E fill:#dfd,stroke:#333,stroke-width:1px
  style G fill:#fdd,stroke:#333,stroke-width:1px
  style H fill:#fcc,stroke:#333,stroke-width:1px
```

Textual explanation:
- CoinGecko API: source of market and coin-level JSON payloads.
- Ingestion Service: Python-based client with configurable retry/backoff, batching, and request auditing. Writes raw responses and ingestion metadata into Bronze.
- Bronze: Delta tables that preserve raw_payload and ingestion metadata (batch_id, request_timestamp, request_id).
- Silver: Parsing, schema enforcement, validation rules, and deterministic deduplication using business key (e.g., coin_id + market_timestamp). Invalid or suspicious records are routed to a quarantine table or reprocess queue.
- Gold: Aggregated, business-ready tables (snapshots, trends, top movers) optimized for Databricks SQL and dashboards.
- Monitoring & Alerting: captures pipeline metrics and emits alerts for repeated failures or schema drift.


Component architecture diagram (detailed):

```mermaid
flowchart TD
  subgraph Source
    API[CoinGecko API]
  end

  subgraph Ingest
    Client[Ingestion Service (Python)]
    Notebook[Optional: Notebook Runner]
  end

  subgraph Storage
    Bronze[Bronze (Delta) - raw]
    Silver[Silver (Delta) - curated]
    Gold[Gold (Delta) - analytics]
  end

  subgraph Processing
    ETL[Batch ETL / Jobs]
    Streaming[Structured Streaming (future)]
  end

  subgraph Ops
    Monitoring[Prometheus / Datadog / Azure Monitor]
    Alerts[Alerting (PagerDuty / Email)]
    CI[CI / Tests]
  end

  API --> Client --> Bronze --> ETL --> Silver --> ETL --> Gold -->|served by| Dash[Databricks SQL / BI]
  Client --> Monitoring
  ETL --> Monitoring
  ETL --> Alerts
  CI --> ETL
  Streaming --> Silver
  Notebook --> Client
  Dash -->|queries| Gold
  style Bronze fill:#f3f4f6
  style Silver fill:#fff7ed
  style Gold fill:#ecfdf5
  style Monitoring fill:#fffbeb
  style CI fill:#eef2ff
```

Diagram notes:
- ETL represents scheduled Jobs or Databricks Jobs that perform Silver transformations and Gold aggregations.
- Streaming is a planned enhancement; current implementation is batch-based but the codebase is organized to allow Structured Streaming in the future.
- Monitoring integrates with emitted metrics and pipeline logs; alerts are triggered for failed runs or high rejection rates.

---

## Key Features

- Dynamic, metadata-driven ingestion for many assets
- Delta Lake-backed medallion architecture (Bronze / Silver / Gold)
- Idempotent MERGE-based upserts using deterministic business keys
- Configurable retry and rate-limit handling
- Record-level data quality classifications (VALID, INVALID, QUARANTINED)
- Operational metadata & audit logs for every pipeline run
- Time-series analytics and trend generation

---

## Medallion Layers

Bronze (raw):
- Purpose: Preserve raw API responses with ingestion metadata
- Example tables: `bronze.coingecko_market_raw`, `bronze.coingecko_coin_raw`
- Typical metadata: `batch_id`, `api_endpoint`, `request_timestamp`, `ingestion_timestamp`, `raw_payload`

Silver (curated):
- Purpose: Parse JSON, enforce schema, standardize types, validate and deduplicate
- Example table: `silver.crypto_market`
- Representative columns:
  - `coin_id`, `symbol`, `name`
  - `current_price`, `market_cap`, `total_volume`
  - `market_cap_rank`, `price_change_percentage_24h`
  - `last_updated`, `ingestion_timestamp`

Gold (analytics):
- Purpose: Business-ready datasets optimized for dashboards and analytics
- Example tables: `gold.crypto_market_snapshot`, `gold.crypto_price_trends`, `gold.crypto_top_movers`

---

## Time-Series Analytics & Metrics

Supported analytics:
- Current price, 24h/7d/30d change
- Moving averages (e.g., 7-day, 30-day)
- Volatility metrics and historical comparisons
- Top gainers / losers by period
- Market capitalization and volume aggregations

---

## Incremental Processing & Idempotency

- Incremental keys: `last_updated`, `ingestion_timestamp`, `batch_id`, or `market_timestamp`
- Business key recommendation: `coin_id + market_timestamp`
- Use Delta Lake `MERGE` to perform upserts and maintain idempotency
- Pipelines should detect and process only changed or new records when possible

---

## Data Quality and Validation

Validation rules are applied at Silver stage and include:
- Completeness: required fields are not null (e.g., `coin_id`, `current_price`, `market_timestamp`)
- Validity: numeric ranges (e.g., `current_price >= 0`)
- Duplicate detection: business-key uniqueness
- Classification: `VALID`, `INVALID`, `QUARANTINED`

Invalid records are isolated for investigation rather than contaminating Gold datasets.

---

## Observability, Audit & Monitoring

Operational metadata captured per run:
- `pipeline_name`, `batch_id`, `start_timestamp`, `end_timestamp`
- `records_read`, `records_processed`, `records_inserted`, `records_updated`, `records_rejected`
- `pipeline_status`, `error_message`

Key monitoring metrics:
- Pipeline status and duration
- Records processed / rejected
- API response time and data freshness
- Last successful run timestamp and failure counts

---

## Error Handling & Rate Limits

- Configurable retry policy (count, delay) and exponential backoff
- Handling for HTTP errors, timeouts, network failures, and rate limiting
- Logging and alerting for repeated failures or schema changes

Retry flow (conceptual): API Request -> Success? -> Process : Retry -> On retry exhaustion log failure and emit alert

---

## Getting Started (Quick Start)

Prerequisites:
- Python 3.8+ and pip
- PySpark and Spark-compatible environment (Databricks recommended)
- Delta Lake or Databricks Lakehouse

1. Clone repository

   git clone https://github.com/swapniltake1/CryptoStream-Real-Time-Market-Data-Engineering-Platform.git

2. Install dependencies

   pip install -r requirements.txt

3. Configure credentials and environment variables (do NOT commit secrets)

4. Run the ingestion notebook or pipeline from `notebooks/01_ingestion/coin_market_ingestion`

---

## Configuration

Configurations are metadata-driven and live in `config/`:
- `api_config` — API endpoints, retry policy, rate limits
- `crypto_config` — list of tracked assets and currencies
- `pipeline_config` — batch sizes, delta paths, environment overrides

Add or enable assets by updating `crypto_config` rather than changing code.

---

## Testing & CI

Suggested test categories:
- Unit tests for parsing and transformation logic (`tests/test_transformations`)
- Integration tests for end-to-end Bronze -> Silver -> Gold (`tests/test_ingestion`)
- Data quality tests for schema, nulls, duplicates (`tests/test_data_quality`)

Include CI pipelines to run tests and linters on PRs.

---

## Project Layout

```
CryptoStream/
├── README.md
├── LICENSE
├── config/
├── notebooks/
├── sql/
├── tests/
├── docs/
└── CryptoStream_Documentation/
```

---

## Contribution

Contributions are welcome. Follow these guidelines:
- Open an issue for major changes or feature requests
- Create a branch for your work and open a PR against the default branch
- Include tests and update documentation for new features

---

## Future Enhancements

Planned improvements:
- Real-time ingestion (Structured Streaming / WebSocket)
- Databricks Auto Loader / Unity Catalog integration
- ML-based anomaly detection and alerting
- Enhanced data observability and lineage
- CI/CD for deployments and automated testing

---

## Author

Swapnil Take — Data Engineer
https://github.com/swapniltake1

---

## License

This project is licensed under the MIT License. See `LICENSE` for details.

---

Disclaimer: This project is for educational and engineering demonstration purposes only. It is not financial or investment advice.
