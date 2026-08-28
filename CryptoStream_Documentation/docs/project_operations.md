# CryptoStream Project Operations Guide

## Daily/Per-Run Checklist

- [ ] Source API request completed
- [ ] JSON file generated
- [ ] File landed in Databricks
- [ ] Bronze completed
- [ ] Control table updated
- [ ] Silver completed
- [ ] Data-quality checks passed
- [ ] Gold completed
- [ ] Monitoring completed
- [ ] Freshness is within SLA
- [ ] Counts reconciled

## Release Checklist

- [ ] Unit tests passed
- [ ] Integration tests passed
- [ ] Data-quality tests passed
- [ ] No credentials committed
- [ ] Configuration reviewed
- [ ] Schema changes reviewed
- [ ] README updated
- [ ] Architecture documentation updated
- [ ] Rollback plan confirmed

## Portfolio Presentation Checklist

Be able to explain:

1. Why CoinGecko?
2. Why JSON landing?
3. Why Bronze/Silver/Gold?
4. How is incremental processing achieved?
5. How is idempotency achieved?
6. What happens when a file fails?
7. How are bad records handled?
8. Why Delta Lake?
9. How would you make it real-time?
10. What would you change for production scale?

## Recommended Project Positioning

CryptoStream should currently be presented as an enterprise-style Lakehouse data engineering project with a file-based batch/micro-batch ingestion architecture and a roadmap toward real-time streaming. Do not describe the current implementation as fully real-time unless Structured Streaming/Auto Loader or another true streaming mechanism has been implemented.
