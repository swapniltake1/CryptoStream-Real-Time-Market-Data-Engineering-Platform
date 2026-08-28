# CryptoStream Architecture

## 1. Purpose

CryptoStream is an enterprise-style cryptocurrency market data engineering platform. It ingests CoinGecko market data, lands source payloads as JSON, processes data through Bronze, Silver and Gold Delta layers, and exposes operational monitoring and analytical outputs.

## 2. Architecture

```text
CoinGecko API
     |
     v
External Python Ingestion
     |
     v
Timestamped JSON Landing
     |
     v
Bronze Delta
     |
     +--> Processed Files Control Table
     |
     v
Silver Delta
     |
     +--> Data Quality / Deduplication
     |
     v
Gold Delta
     |
     +--> Market Snapshot
     +--> Top Gainers
     +--> Top Losers
     +--> Market Metrics
     |
     +--------------------+
                          |
                    Monitoring
                          |
                    Analytics / SQL
```

## 3. Architectural Principles

- Medallion Architecture
- Separation of ingestion and transformation
- Source traceability
- Incremental file processing
- Idempotent processing
- Explicit data-quality controls
- Operational observability
- Modular notebooks and Python components
- Configuration-driven evolution

## 4. Components

### External ingestion

`external_ingestion/coingecko_ingestion.py` calls the CoinGecko markets endpoint, adds project/source/batch metadata, and creates timestamped JSON files.

### Bronze

`bronze/bronze_from_external_ingestion.py` discovers unprocessed JSON files, reads the source payload, attaches Bronze metadata and writes Delta data.

### Silver

`silver/silver_transformation.py` validates required fields and numeric ranges, deduplicates observations, standardizes the schema and derives `price_volatility_24h`.

### Gold

`gold/gold_market_analytics.py` creates latest market snapshots, top movers and market-level metrics.

### Monitoring

`monitoring/pipeline_monitoring.py` checks freshness, layer counts, basic quality indicators and market-level signals.

### Orchestration

`monitoring/pipeline_orchestrator.py` executes Bronze, Silver, Gold and monitoring in sequence and records stage status/duration.

## 5. Target Data Layers

```text
main.crypto_bronze.coingecko_market_data
main.crypto_bronze.processed_files_control
main.crypto_silver.crypto_market
main.crypto_gold.market_snapshot
main.crypto_gold.top_gainers
main.crypto_gold.top_losers
main.crypto_gold.market_metrics
```

## 6. Scalability Path

For higher scale, the architecture can evolve to Databricks Auto Loader, Structured Streaming, Unity Catalog governance, checkpointing, event-time processing and automated CI/CD.

## 7. Current State

The current implementation is a file-based batch/micro-batch style pipeline. The repository roadmap describes real-time streaming as a future enhancement. This distinction should be preserved in technical documentation and interviews.
