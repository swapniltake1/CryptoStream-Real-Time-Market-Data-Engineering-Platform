# CryptoStream Testing Strategy

## 1. Testing Objectives

Validate ingestion reliability, transformation correctness, data quality, idempotency and end-to-end reconciliation.

## 2. Unit Tests

Test:
- batch ID generation
- API response parsing
- metadata construction
- numeric validation
- volatility calculation
- top gainers/losers logic

## 3. Data Quality Tests

Test:
- required columns
- null constraints
- numeric ranges
- duplicate keys
- timestamp parsing
- schema compatibility

## 4. Integration Tests

Validate:

```text
JSON -> Bronze -> Silver -> Gold
```

Expected:
- Bronze count matches source payload
- Silver accepted + rejected reconciles with Bronze
- Gold snapshot contains latest record per tracked coin

## 5. Idempotency Test

Run Bronze twice against the same file.

Expected:

```text
First run  -> SUCCESS + data written
Second run -> file skipped
```

Run Silver twice for the same logical grain.

Expected: no unintended duplicate business records.

## 6. Failure Tests

Simulate:
- API timeout
- connection failure
- malformed JSON
- missing required fields
- negative price
- missing market cap
- duplicate records
- missing landing file

## 7. Regression Testing

Any change to source schema, transformation logic or Gold business rules should run the full test suite before deployment.

## 8. CI/CD Direction

Future CI should execute:

```text
Lint
  ->
Unit Tests
  ->
Data Quality Tests
  ->
Integration Tests
  ->
Build/Package
  ->
Deploy
```
