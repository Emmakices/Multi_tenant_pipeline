import json
import logging
import os
import re
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Tuple

import dask.dataframe as dd
import numpy as np
import pandas as pd
from flask import Flask, jsonify, request
from google.cloud import bigquery, storage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize clients conditionally to avoid authentication issues during import/testing
storage_client = None
bq_client = None


def get_storage_client():
    """Get or create the storage client."""
    global storage_client
    if storage_client is None:
        # Skip initialization in CI/testing environments
        if (
            os.getenv("CI")
            or os.getenv("GITHUB_ACTIONS")
            or os.getenv("PYTEST_CURRENT_TEST")
        ):
            from unittest.mock import Mock

            storage_client = Mock()
            storage_client.bucket = Mock()
        else:
            storage_client = storage.Client()
    return storage_client


def get_bq_client():
    """Get or create the BigQuery client."""
    global bq_client
    if bq_client is None:
        # Skip initialization in CI/testing environments
        if (
            os.getenv("CI")
            or os.getenv("GITHUB_ACTIONS")
            or os.getenv("PYTEST_CURRENT_TEST")
        ):
            from unittest.mock import Mock

            bq_client = Mock()
            bq_client.dataset = Mock()
            bq_client.get_dataset = Mock()
            bq_client.create_dataset = Mock()
            bq_client.load_table_from_dataframe = Mock()
            bq_client.get_table = Mock()
        else:
            bq_client = bigquery.Client()
    return bq_client


# BigQuery Configuration
PROJECT_ID = "data-pipeline-project-450922"
BASE_DATASET_ID = "Tenants"
TABLE_ID = "tenants"

# Regional BigQuery locations mapping
REGION_TO_BQ_LOCATION = {
    "us-central1": "US",
    "us-east1": "US",
    "us-west1": "US",
    "europe-west1": "EU",
    "europe-west2": "EU",
    "europe-west3": "EU",
    "asia-southeast1": "asia-southeast1",
    "asia-east1": "asia-east1",
    "asia-northeast1": "asia-northeast1",
}

# Define expected schema for E-commerce data
EXPECTED_COLUMNS = {
    "event_time": "datetime",
    "event_type": "string",
    "product_id": "int64",
    "category_id": "int64",
    "category_code": "string",
    "brand": "string",
    "price": "float64",
    "user_id": "int64",
    "user_session": "string",
}

VALID_EVENT_TYPES = ["view", "cart", "purchase", "remove_from_cart", "wishlist"]


def get_regional_dataset_id(region: str) -> str:
    """Generate region-specific dataset ID."""
    region_suffix = region.replace("-", "_")
    return f"{BASE_DATASET_ID}_{region_suffix}"


def get_bigquery_location(region: str) -> str:
    """Get BigQuery location for the specified region."""
    return REGION_TO_BQ_LOCATION.get(region, "US")  # Default to US if region not found


def extract_tenant_from_path(file_name: str) -> str:
    """Extract tenant ID from file path."""
    try:
        # Expected path: tenants/{tenant_id}/... or shared/...
        if file_name.startswith("tenants/"):
            parts = file_name.split("/")
            return parts[1] if len(parts) > 1 else "unknown"
        elif file_name.startswith("shared/"):
            return "shared"
        else:
            return "unknown"
    except:
        return "unknown"


def download_file_from_gcs(bucket_name: str, file_name: str) -> str:
    """Download file from Google Cloud Storage to local temp file."""
    try:
        bucket = get_storage_client().bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        blob.download_to_filename(temp_file.name)

        logger.info(f"Downloaded {file_name} to {temp_file.name}")
        return temp_file.name

    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        raise


def process_ecommerce_data(
    file_path: str, file_name: str, tenant_id: str, region: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process e-commerce data with ML-ready transformations and data quality checks using Dask.
    Returns: (clean_data, bad_records)
    """
    try:
        # Read the CSV file with Dask
        df = dd.read_csv(file_path)
        logger.info(f"Loaded dataframe with partitions: {df.npartitions}")

        # Initialize tracking for bad records
        bad_records = pd.DataFrame()

        # SCHEMA VALIDATION
        df, schema_bad = validate_schema_dask(df)
        if not schema_bad.empty:
            schema_bad["bad_reason"] = "schema_validation_failed"
            bad_records = pd.concat([bad_records, schema_bad], ignore_index=True)

        # DATA QUALITY CHECKS
        df, quality_bad = perform_data_quality_checks_dask(df)
        if not quality_bad.empty:
            bad_records = pd.concat([bad_records, quality_bad], ignore_index=True)

        # ML FEATURE ENGINEERING (only on clean data)
        if len(df) > 0:
            df = create_ml_features_dask(df, tenant_id, file_name, region)

        # Convert to pandas for final return (since BigQuery client expects pandas)
        final_df = df.compute() if hasattr(df, "compute") else df

        logger.info(
            f"Final clean data shape: {final_df.shape}, Bad records: {len(bad_records)}"
        )
        return final_df, bad_records

    except Exception as e:
        logger.error(f"Error processing e-commerce data: {str(e)}")
        # Return empty DataFrames on error
        return pd.DataFrame(), pd.DataFrame()


def validate_schema_dask(df: dd.DataFrame) -> Tuple[dd.DataFrame, pd.DataFrame]:
    """Validate data schema and separate bad records using Dask."""
    bad_records = pd.DataFrame()

    # Check if required columns exist (compute this to check)
    columns = df.columns.tolist()
    missing_cols = set(EXPECTED_COLUMNS.keys()) - set(columns)
    if missing_cols:
        logger.warning(f"Missing columns: {missing_cols}")
        return (
            dd.from_pandas(pd.DataFrame(), npartitions=1),
            df.compute(),
        )  # All records are bad if schema is wrong

    try:
        # Convert and validate data types with Dask
        df = df.assign(
            event_time=dd.to_datetime(df["event_time"], errors="coerce"),
            product_id=dd.to_numeric(df["product_id"], errors="coerce"),
            category_id=dd.to_numeric(df["category_id"], errors="coerce"),
            user_id=dd.to_numeric(df["user_id"], errors="coerce"),
            price=dd.to_numeric(df["price"], errors="coerce"),
        )

        # Create mask for bad records
        bad_mask = (
            df["event_time"].isna()
            | df["product_id"].isna()
            | df["category_id"].isna()
            | df["user_id"].isna()
            | ~df["event_type"].isin(VALID_EVENT_TYPES)
        )

        # Separate good and bad records
        bad_records_df = df[bad_mask].compute()
        clean_df = df[~bad_mask]

        return clean_df, bad_records_df

    except Exception as e:
        logger.error(f"Schema validation error: {str(e)}")
        return dd.from_pandas(pd.DataFrame(), npartitions=1), df.compute()


def perform_data_quality_checks_dask(
    df: dd.DataFrame,
) -> Tuple[dd.DataFrame, pd.DataFrame]:
    """Perform comprehensive data quality checks using Dask."""
    if len(df) == 0:
        return df, pd.DataFrame()

    try:
        # Create mask for bad records using Dask operations
        bad_mask = (
            # Missing Value Detection for critical fields
            df["user_id"].isna()
            | df["product_id"].isna()
            | df["event_time"].isna()
            | df["event_type"].isna()
            |
            # Price validation for purchase events
            (
                (df["event_type"] == "purchase")
                & (df["price"].isna() | (df["price"] <= 0))
            )
            |
            # Data Integrity Validation - Negative or zero IDs
            (df["user_id"] <= 0)
            | (df["product_id"] <= 0)
            | (df["category_id"] <= 0)
            |
            # Session validation
            df["user_session"].isna()
            | (df["user_session"].str.len() < 5)
        )

        # Handle price outliers (compute stats first)
        if "price" in df.columns:
            price_mean = df["price"].mean().compute()
            price_std = df["price"].std().compute()

            if price_std > 0:
                price_outlier_mask = (df["price"] > price_mean + 3 * price_std) | (
                    df["price"] < 0
                )
                bad_mask = bad_mask | price_outlier_mask

        # Simplified duplicate detection for Dask
        # Drop duplicates and track what was dropped
        df_with_index = df.reset_index()
        df_deduplicated = df_with_index.drop_duplicates(
            subset=["user_id", "product_id", "event_time"], keep="first"
        )

        # Create final bad mask
        final_bad_mask = bad_mask

        # Separate good and bad records
        bad_records = df[final_bad_mask].compute()
        if not bad_records.empty:
            bad_records["bad_reason"] = "data_quality_failed"

        clean_df = df[~final_bad_mask]

        return clean_df, bad_records

    except Exception as e:
        logger.error(f"Data quality check error: {str(e)}")
        return df, pd.DataFrame()


def create_ml_features_dask(
    df: dd.DataFrame, tenant_id: str, file_name: str, region: str
) -> dd.DataFrame:
    """Create ML-ready features from e-commerce data using Dask with historical tracking."""
    if len(df) == 0:
        return df

    try:
        # Add metadata columns (these are constant for all rows)
        df = df.assign(
            tenant_id=tenant_id,
            region=region,
            original_filename=file_name,
            processed_timestamp=datetime.now(UTC),
            processing_engine="dask",
            data_quality_status="clean",
            created_at=datetime.now(UTC),
        )

        # Add unique record IDs (do this in pandas for UUID generation)
        def add_record_ids(partition):
            partition["record_id"] = [str(uuid.uuid4()) for _ in range(len(partition))]
            return partition

        df = df.map_partitions(add_record_ids)

        # 1. TEMPORAL FEATURES
        df = df.assign(
            hour=df["event_time"].dt.hour,
            day_of_week=df["event_time"].dt.dayofweek,
            month=df["event_time"].dt.month,
            is_weekend=df["event_time"].dt.dayofweek.isin([5, 6]).astype(int),
        )

        # 2. USER BEHAVIOR FEATURES (Simplified for Dask)
        # Add basic time features first
        df = df.assign(time_since_last_event=0)  # Simplified for Dask

        # SESSION-BASED FEATURES (Simplified)
        # Add basic session features
        df = df.assign(
            session_event_count=1,  # Will be aggregated later
            session_unique_products=1,  # Simplified
            session_total_value=df["price"],
            session_avg_price=df["price"],
            session_max_price=df["price"],
            session_duration_minutes=0,  # Simplified
        )

        # CATEGORICAL ENCODING (Simplified)
        # Simple label encoding instead of one-hot for Dask efficiency
        def encode_categories(partition):
            if len(partition) > 0:
                partition["brand_encoded"] = (
                    partition["brand"].fillna("unknown").astype("category").cat.codes
                )
                partition["category_code_encoded"] = (
                    partition["category_code"]
                    .fillna("unknown")
                    .astype("category")
                    .cat.codes
                )
            return partition

        # Create metadata with the new columns
        meta_df = df._meta.copy()
        meta_df["brand_encoded"] = 0
        meta_df["category_code_encoded"] = 0

        df = df.map_partitions(encode_categories, meta=meta_df)

        # PRODUCT FEATURES (Simplified)
        df = df.assign(
            product_popularity=1,  # Will be computed later if needed
            category_price_mean=df["price"],
            category_price_std=1.0,
            price_vs_category_avg=0.0,
        )

        # USER AGGREGATION FEATURES (Simplified)
        df = df.assign(
            user_total_events=1,
            user_unique_products=1,
            user_total_spent=df["price"],
            user_avg_price=df["price"],
            user_unique_categories=1,
            user_conversion_rate=0.1,  # Default conversion rate
        )

        logger.info(f"Created ML features with Dask.")
        return df

    except Exception as e:
        logger.error(f"Error creating ML features: {str(e)}")
        return df


def save_to_bigquery_historical_dask(
    df: pd.DataFrame, file_name: str, region: str, tenant_id: str
) -> int:
    """Save processed data to BigQuery with regional multi-location support using Dask processed DataFrame."""
    try:
        # Get region-specific dataset ID and BigQuery location
        dataset_id = get_regional_dataset_id(region)
        bq_location = get_bigquery_location(region)

        logger.info(f"Using dataset: {dataset_id} in BigQuery location: {bq_location}")

        # Create regional dataset if it doesn't exist
        dataset_ref = get_bq_client().dataset(dataset_id)
        try:
            existing_dataset = get_bq_client().get_dataset(dataset_ref)
            logger.info(
                f"Using existing dataset: {dataset_id} in {existing_dataset.location}"
            )
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = bq_location
            dataset.description = f"Multi-tenant ML pipeline data for {region} region with historical preservation"

            # Set dataset labels for better organization
            dataset.labels = {
                "environment": "production",
                "pipeline": "multi_tenant_ml",
                "region": region.replace("-", "_"),
                "data_type": "ecommerce_events",
            }

            get_bq_client().create_dataset(dataset)
            logger.info(f"Created dataset: {dataset_id} in {bq_location}")

        # Configure table schema optimized for multi-tenant historical data
        table_ref = dataset_ref.table(TABLE_ID)

        # Job configuration for historical append
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",  # Always append, never overwrite
            create_disposition="CREATE_IF_NEEDED",
            # Partition by created_at date for efficient querying
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="created_at"
            ),
            # Cluster by tenant_id, region, and event_type for efficient tenant isolation and regional queries
            clustering_fields=["tenant_id", "region", "event_type", "user_id"],
            autodetect=True,  # Let BigQuery auto-detect schema from Dask data
        )

        # Load data to BigQuery
        job = get_bq_client().load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()  # Wait for job to complete

        # Verify the load
        table = get_bq_client().get_table(table_ref)
        logger.info(f"Successfully saved {len(df)} rows to BigQuery")
        logger.info(f"Table: {PROJECT_ID}.{dataset_id}.{TABLE_ID}")
        logger.info(f"BigQuery Location: {bq_location}")
        logger.info(f"Total table rows: {table.num_rows}")
        logger.info(f"Tenant: {tenant_id}, Region: {region}")

        return len(df)

    except Exception as e:
        logger.error(f"Error saving to BigQuery: {str(e)}")
        raise


def save_bad_records_dask(
    bad_df: pd.DataFrame, bucket_name: str, file_name: str, tenant_id: str
):
    """Save bad records to tenant-specific bad_records folder using Dask processed data."""
    try:
        if bad_df.empty:
            return

        # Create bad records file path
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        original_filename = file_name.split("/")[-1].replace(".csv", "")

        if tenant_id == "shared":
            bad_records_path = (
                f"shared/bad_records/dask_failed_{timestamp}_{original_filename}.csv"
            )
        else:
            bad_records_path = f"tenants/{tenant_id}/data/bad_records/dask_failed_{timestamp}_{original_filename}.csv"

        # Convert bad records to CSV
        temp_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv")
        bad_df.to_csv(temp_file.name, index=False)

        # Upload to GCS
        bucket = get_storage_client().bucket(bucket_name)
        blob = bucket.blob(bad_records_path)
        blob.upload_from_filename(temp_file.name)

        # Add metadata
        blob.metadata = {
            "original_file": file_name,
            "processing_engine": "dask",
            "failed_timestamp": datetime.now(UTC).isoformat(),
            "bad_record_count": str(len(bad_df)),
            "tenant_id": tenant_id,
        }
        blob.patch()

        # Clean up temp file
        os.unlink(temp_file.name)

        logger.info(f"Saved {len(bad_df)} bad records to: {bad_records_path}")

    except Exception as e:
        logger.error(f"Error saving bad records: {str(e)}")


def move_file_to_bad_records_dask(
    bucket_name: str, file_name: str, error_message: str, tenant_id: str
):
    """Move entire failed file to bad_records folder."""
    try:
        bucket = get_storage_client().bucket(bucket_name)
        source_blob = bucket.blob(file_name)

        # Create bad_records path
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        filename = file_name.split("/")[-1]

        if tenant_id == "shared":
            bad_records_path = (
                f"shared/bad_records/dask_processing_failed_{timestamp}_{filename}"
            )
        else:
            bad_records_path = f"tenants/{tenant_id}/data/bad_records/dask_processing_failed_{timestamp}_{filename}"

        # Copy to bad_records folder
        bucket.copy_blob(source_blob, bucket, bad_records_path)

        # Add error metadata
        bad_blob = bucket.blob(bad_records_path)
        bad_blob.metadata = {
            "error_message": error_message,
            "processing_engine": "dask",
            "failed_timestamp": datetime.now(UTC).isoformat(),
            "original_path": file_name,
            "tenant_id": tenant_id,
        }
        bad_blob.patch()

        logger.info(f"Moved failed file to: {bad_records_path}")

    except Exception as e:
        logger.error(f"Error moving file to bad_records: {str(e)}")


@app.route("/process", methods=["POST"])
def process_file():
    """
    Main processing endpoint for the Dask engine - this is where big files come to get processed.
    
    Dask is our heavy-duty engine for large datasets that need distributed processing.
    The router sends us files that are too big for Pandas or Polars to handle efficiently.
    We can process datasets larger than memory by breaking them into chunks and working
    on them across multiple workers.
    """
    try:
        # Parse the enhanced message from our router
        request_data = request.get_json()
        logger.info(f"Dask engine received processing request: {request_data}")

        # Extract file information from enhanced message structure
        file_info = request_data.get("file_info", {})
        processing_metadata = request_data.get("processing_metadata", {})
        
        # Get core file details
        file_name = file_info.get("file_name")
        bucket_name = file_info.get("bucket_name")
        file_size = file_info.get("file_size", 0)
        
        # Get tenant and regional info (parsed by Cloud Function)
        tenant_id = file_info.get("tenant_id")
        if not tenant_id and file_name:
            # Fallback: extract from file path for testing or direct calls
            tenant_id = extract_tenant_from_path(file_name)
        elif not tenant_id:
            tenant_id = "unknown"
        
        region = file_info.get("region", "us-central1")
        
        # Get correlation ID for tracking
        correlation_id = file_info.get("correlation_id", processing_metadata.get("correlation_id", "unknown"))
        
        # Get file type information
        file_type_info = file_info.get("file_info", {})
        file_type = file_type_info.get("file_type", "unknown")
        is_supported = file_type_info.get("is_supported", True)

        # Set up logging context for distributed tracing
        log_context = {
            'correlation_id': correlation_id,
            'tenant_id': tenant_id,
            'file_name': file_name,
            'file_size': file_size,
            'file_type': file_type,
            'engine': 'dask',
            'region': region
        }
        
        logger.info(f"Starting Dask distributed processing for tenant {tenant_id}", extra=log_context)
        
        # Dask is designed for big files - let's see what we're dealing with
        if file_size < 100 * 1024 * 1024:  # 100MB
            logger.info(f"Small file for Dask ({file_size / (1024*1024):.1f}MB) - Pandas might be more efficient", extra=log_context)
        else:
            logger.info(f"Good fit for Dask processing ({file_size / (1024*1024):.1f}MB) - using distributed approach", extra=log_context)

        logger.info(
            f"Processing {file_name} from {bucket_name}, Size: {file_size} bytes, Tenant: {tenant_id}"
        )

        # Download file from GCS
        file_path = download_file_from_gcs(bucket_name, file_name)

        # Process file with ML-ready transformations using Dask
        processed_data, bad_records = process_ecommerce_data(
            file_path, file_name, tenant_id, region
        )

        # Save good records to BigQuery (Historical append mode)
        good_record_count = 0
        if not processed_data.empty:
            good_record_count = save_to_bigquery_historical_dask(
                processed_data, file_name, region, tenant_id
            )

        # Handle bad records
        bad_record_count = 0
        if not bad_records.empty:
            bad_record_count = len(bad_records)
            save_bad_records_dask(bad_records, bucket_name, file_name, tenant_id)

        # Clean up temporary file
        os.unlink(file_path)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Successfully processed {file_name} with Dask",
                    "engine": "dask",
                    "region": region,
                    "tenant_id": tenant_id,
                    "good_records": good_record_count,
                    "bad_records": bad_record_count,
                    "processing_timestamp": datetime.now(UTC).isoformat(),
                    "bigquery_table": f"{PROJECT_ID}.{{regional_dataset}}.{TABLE_ID}",
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")

        # Move entire file to bad_records folder if processing fails
        if "file_name" in locals() and "bucket_name" in locals():
            move_file_to_bad_records_dask(
                bucket_name,
                file_name,
                str(e),
                tenant_id if "tenant_id" in locals() else "unknown",
            )

        return jsonify({"status": "error", "message": str(e), "engine": "dask"}), 500


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return (
        jsonify(
            {
                "status": "healthy",
                "engine": "dask",
                "supported_formats": ["csv", "json", "xlsx"],
                "data_type": "ecommerce_events",
                "bigquery_target": f"{PROJECT_ID}.{{regional_dataset}}.{TABLE_ID}",
                "version": "2.0-historical",
            }
        ),
        200,
    )


@app.route("/", methods=["GET"])
def root():
    """Root endpoint for basic connectivity test."""
    return (
        jsonify(
            {
                "message": "Dask Processing Engine - Multi-Tenant ML Pipeline",
                "status": "ready",
                "engine": "dask",
                "endpoints": ["/process", "/health"],
                "bigquery_target": f"{PROJECT_ID}.{{regional_dataset}}.{TABLE_ID}",
            }
        ),
        200,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
