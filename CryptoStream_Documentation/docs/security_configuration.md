# CryptoStream Security and Configuration Guide

## 1. Security Principles

- Never commit credentials.
- Separate configuration from code.
- Use secret management for API keys when required.
- Restrict write access to Bronze/Silver/Gold schemas.
- Use least-privilege permissions.
- Avoid personal workspace paths in reusable production code.

## 2. API Credentials

The current CoinGecko public endpoint does not require an API key for the basic ingestion demonstrated by the project. If authenticated endpoints are introduced, credentials must be stored in secure secret management.

Do not use:

```python
API_KEY = "my-secret-key"
```

in source control.

## 3. Configuration

Recommended configuration areas:

```text
source endpoint
target currency
tracked assets
landing path
catalog
schemas
quality thresholds
retry settings
environment
```

## 4. Environment Model

```text
DEV
TEST
PROD
```

Use environment-specific values rather than editing notebook logic.

## 5. Data Access

Recommended access model:

```text
Ingestion Service -> Bronze Write
Transformation Job -> Silver Write
Analytics Job -> Gold Write
Analysts -> Gold Read
```

## 6. Sensitive Data

Cryptocurrency market data itself is generally public market information. Operational credentials, tokens, connection strings and infrastructure details should still be treated as sensitive.

## 7. Governance Roadmap

- Unity Catalog
- centralized permissions
- table ownership
- lineage
- audit logging
- environment separation
- secret management
