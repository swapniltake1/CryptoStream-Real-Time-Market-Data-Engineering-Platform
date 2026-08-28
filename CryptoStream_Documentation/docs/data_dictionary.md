# CryptoStream Data Dictionary

## 1. Bronze Dataset

### `main.crypto_bronze.coingecko_market_data`

| Column | Type | Description |
|---|---|---|
| id | STRING | CoinGecko cryptocurrency identifier |
| symbol | STRING | Cryptocurrency market symbol |
| name | STRING | Cryptocurrency name |
| current_price | DOUBLE | Current quoted price |
| market_cap | LONG/DOUBLE | Market capitalization |
| market_cap_rank | INT | Market capitalization rank |
| total_volume | LONG/DOUBLE | 24-hour trading volume |
| high_24h | DOUBLE | 24-hour high |
| low_24h | DOUBLE | 24-hour low |
| price_change_24h | DOUBLE | Absolute 24-hour price change |
| price_change_percentage_24h | DOUBLE | Percentage 24-hour price change |
| last_updated | TIMESTAMP/STRING | Source update timestamp |
| ingestion_timestamp | TIMESTAMP | Platform ingestion timestamp |
| source_system | STRING | Source identifier |
| source_endpoint | STRING | Source API endpoint |
| batch_id | STRING | Unique ingestion batch |
| ingestion_date | DATE | Processing date |
| source_file | STRING | Landing filename |
| source_file_modified_time | TIMESTAMP | Landing file modification time |

## 2. Silver Dataset

### `main.crypto_silver.crypto_market`

| Column | Type | Description |
|---|---|---|
| coin_id | STRING | Canonical cryptocurrency identifier |
| symbol | STRING | Cryptocurrency symbol |
| coin_name | STRING | Canonical cryptocurrency name |
| current_price | DOUBLE | Current price |
| market_cap | DOUBLE | Market capitalization |
| market_cap_rank | INT | Market rank |
| total_volume | DOUBLE | 24-hour trading volume |
| high_24h | DOUBLE | 24-hour high |
| low_24h | DOUBLE | 24-hour low |
| price_change_24h | DOUBLE | Absolute price movement |
| price_change_percentage_24h | DOUBLE | Percentage price movement |
| market_timestamp | TIMESTAMP | Source market observation time |
| ingestion_timestamp | TIMESTAMP | Platform ingestion time |
| source_system | STRING | Source system |
| batch_id | STRING | Batch identifier |
| ingestion_date | DATE | Processing date |
| price_volatility_24h | DOUBLE | `(high_24h-low_24h)/low_24h*100` |
| record_quality | STRING | Current accepted-record quality status |

## 3. Gold Datasets

### Market Snapshot

Latest observation per cryptocurrency for current-state analytics.

### Top Gainers

Top 10 cryptocurrencies ordered by positive 24-hour percentage movement.

### Top Losers

Top 10 cryptocurrencies ordered by lowest 24-hour percentage movement.

### Market Metrics

Aggregated metrics including total market capitalization, total volume, tracked asset count, average price change, maximum/minimum price change and average volatility.

## 4. Control Table

### `main.crypto_bronze.processed_files_control`

| Column | Description |
|---|---|
| source_file | File basename |
| source_file_path | Full landing path |
| file_size | File size |
| file_modified_time | File modification timestamp |
| processed_timestamp | Bronze processing timestamp |
| batch_id | Batch identifier |
| record_count | Records loaded |
| status | SUCCESS/FAILED |
| error_message | Error information when applicable |

## 5. Modeling Principles

- Keep event/market time separate from ingestion time.
- Preserve source identifiers.
- Use deterministic business keys.
- Carry lineage metadata into curated layers.
- Avoid exposing raw source payloads directly to business users.
