# CryptoStream

[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/) [![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/docs/latest/api/python/) [![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red.svg)](https://www.databricks.com/) [![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-blue.svg)](https://delta.io/) [![Source: CoinGecko](https://img.shields.io/badge/Source-CoinGecko-green.svg)](https://www.coingecko.com/) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Professional, production-oriented data engineering platform for ingesting, validating, processing, and analyzing cryptocurrency market data using the CoinGecko API and a Databricks Lakehouse architecture.

---

Table of Contents
- Project Overview
- Architecture
- Key Features
- Data Layers (Medallion)
- Time-Series & Analytics
- Incremental & Idempotent Processing
- Observability & Audit
- Getting Started
- Configuration
- Testing & CI
- Contributing
- License & Author

---

## Project Overview

CryptoStream is an enterprise-style, metadata-driven data engineering platform that converts continuous cryptocurrency market feeds into reliable, analytics-ready datasets. It is designed for scalability, maintainability, data quality, and operational observability.

Primary goals:
- Ingest market data from CoinGecko reliably
- Preserve raw source payloads for traceability
- Enforce schema and validation rules
- Provide idempotent, incremental updates into Delta Lake
- Expose business-ready Gold tables for analytics and dashboards

---

## Architecture (High level)

CoinGecko API -> Ingestion Layer (Python) -> Bronze (raw) -> Silver (cleansed) -> Gold (analytics) -> Databricks SQL / Dashboards

Key responsibilities by layer:
- Ingestion: API clients, retries, rate-limit handling, request auditing
- Bronze: raw payloads, ingestion metadata, batch traceability
- Silver: schema enforcement, parsing, validation, deduplication
- Gold: aggregated metrics, trend tables, top movers

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
└── docs/
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
