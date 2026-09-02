# Engineering Backlog

This document captures planned engineering improvements for the CryptoStream real-time market data platform.

## Priority Order

1. Remove driver-side `collect()` from Bronze ingestion
2. Process all unprocessed ingestion files in a single pipeline run
3. Centralize shared pipeline logging configuration
4. Add automated data quality validation across Bronze, Silver, and Gold
5. Add automated testing for transformation and ingestion logic
6. Add schema evolution and drift handling
7. Add pipeline performance metrics and execution monitoring
8. Improve secrets and configuration management
9. Add CI validation for Python/notebook code
10. Add documentation for architecture and operational runbooks
