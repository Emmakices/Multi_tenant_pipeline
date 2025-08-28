# Pipeline Testing with Real Data

This directory contains test data and scripts to validate the multi-tenant pipeline with actual data processing instead of relying only on unit tests.

## Test Data Files

- `tenant_a_sample.csv` - Sample e-commerce data for tenant A (us-west1 region)
- `tenant_b_sample.csv` - Sample e-commerce data for tenant B (europe-west1 region)
- `shared_sample.csv` - Sample shared e-commerce data (us-central1 region)

## Test Scripts

### 1. Health Check (`test_pipeline_health.py`)

Tests if all processing engines are deployed and responding:

```bash
python test_pipeline_health.py
```

This script:

- Tests root endpoints of all engines
- Tests health endpoints of all engines
- Returns exit code 0 if all healthy, 1 if any issues

### 2. Full Pipeline Test (`upload_test_data.py`)

Uploads test data to GCS and tests full processing pipeline:

```bash
# Requires Google Cloud credentials
python upload_test_data.py
```

This script:

- Uploads test CSV files to Google Cloud Storage
- Tests each engine (pandas, polars, dask) with each dataset
- Validates processing results and data quality
- Tests multi-tenant isolation (different regions/tenants)

## Data Schema

All test files follow the expected e-commerce schema:

```
event_time (datetime): Timestamp of the event
event_type (string): Type of event (view, cart, purchase)
product_id (int64): Unique product identifier
category_id (int64): Product category ID
category_code (string): Human-readable category
brand (string): Product brand name
price (float64): Product price in USD
user_id (int64): Unique user identifier
user_session (string): User session ID
```

## Expected Behavior

**Tenant A Data** (`tenants/tenant-a/us-west1/`):

- Should be processed and stored in `Tenants_us_west1` dataset
- Should create ML features for recommendation engine
- Should validate data quality and handle any bad records

**Tenant B Data** (`tenants/tenant-b/europe-west1/`):

- Should be processed and stored in `Tenants_europe_west1` dataset
- Should handle different regional settings
- Should maintain tenant isolation

**Shared Data** (`shared/us-central1/`):

- Should be processed and stored with appropriate shared tenant handling
- Should be accessible by analytics across tenants

## Troubleshooting

If tests fail:

1. **Authentication Issues**: Ensure Google Cloud credentials are configured
2. **Engine Deployment**: Check Cloud Run deployments are active
3. **Storage Permissions**: Verify bucket access permissions
4. **Data Format**: Ensure CSV files match expected schema
5. **Regional Settings**: Check BigQuery datasets exist in correct regions

## Usage in CI/CD

These scripts can replace or supplement unit tests in CI/CD pipelines:

```yaml
# In .gitlab-ci.yml
integration-test-real-data:
  stage: integration
  script:
    - python test_pipeline_health.py
    - python upload_test_data.py
  only:
    - main
    - develop
```

This approach tests the actual deployed infrastructure rather than just the code in isolation.
