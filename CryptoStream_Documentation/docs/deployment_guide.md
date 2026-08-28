# CryptoStream Deployment and Setup Guide

## 1. Prerequisites

### Local

- Python 3.8+
- pip
- Git
- Internet connectivity for CoinGecko API ingestion

### Databricks

- Databricks workspace/Free Edition
- Spark/PySpark runtime
- Permission to create schemas and Delta tables required by the implementation

## 2. Clone Repository

```bash
git clone https://github.com/swapniltake1/CryptoStream-Real-Time-Market-Data-Engineering-Platform.git
cd CryptoStream-Real-Time-Market-Data-Engineering-Platform
```

## 3. Install Python Dependency

```bash
pip install requests
```

## 4. Run External Ingestion

```bash
python external_ingestion/coingecko_ingestion.py
```

The script generates a timestamped JSON file under the configured local data locations.

## 5. Move/Land Data in Databricks

Because outbound internet/API access may not be available in Databricks Free Edition, use the local Python process as the external ingestion boundary and upload/copy the generated JSON to the Databricks landing location expected by Bronze.

## 6. Create Databricks Schemas

Use a controlled setup process to create:

```text
main.crypto_bronze
main.crypto_silver
main.crypto_gold
```

## 7. Run Pipeline

Recommended sequence:

```text
bronze/bronze_from_external_ingestion.py
        |
        v
silver/silver_transformation.py
        |
        v
gold/gold_market_analytics.py
        |
        v
monitoring/pipeline_monitoring.py
```

Or execute the orchestrator:

```text
monitoring/pipeline_orchestrator.py
```

## 8. Validate Tables

```sql
SELECT COUNT(*) FROM main.crypto_bronze.coingecko_market_data;
SELECT COUNT(*) FROM main.crypto_silver.crypto_market;
SELECT COUNT(*) FROM main.crypto_gold.market_snapshot;
```

## 9. Production Deployment Direction

For enterprise deployment:

- parameterize environment paths
- move secrets to managed secret storage
- use Databricks Workflows
- use Git-backed source control
- add automated tests
- add CI/CD
- add monitoring/alerts
- adopt Unity Catalog
- use Auto Loader/Structured Streaming where appropriate

## 10. Environment Separation

Recommended environments:

```text
DEV
TEST
PROD
```

Environment-specific configuration should control paths, catalogs, schemas, API settings and operational thresholds.
