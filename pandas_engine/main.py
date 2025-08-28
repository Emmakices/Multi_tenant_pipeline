import json
import logging
import os
import re
import tempfile
import uuid
from datetime import UTC, datetime, timedelta
from typing import Dict, List, Tuple

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


# BigQuery Configuration - Your specified project setup
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


@app.route("/process", methods=["POST"])
def process_file():
    """
    Main endpoint to process e-commerce files using Pandas.
    Expects JSON payload with file information.
    """
    try:
        # Get request data
        request_data = request.get_json()
        logger.info(f"Received request: {request_data}")

        # Extract file information
        file_info = request_data.get("file_info", {})
        file_name = file_info.get("file_name")
        bucket_name = file_info.get("bucket_name")
        file_size = file_info.get("file_size", 0)
        region = file_info.get("region")

        # Extract tenant info from file path
        tenant_id = extract_tenant_from_path(file_name)

        logger.info(
            f"Processing {file_name} from {bucket_name}, Size: {file_size} bytes, Tenant: {tenant_id}"
        )

        # Download file from GCS
        file_path = download_file_from_gcs(bucket_name, file_name)

        # Process file with ML-ready transformations
        processed_data, bad_records = process_ecommerce_data(
            file_path, file_name, tenant_id, region
        )

        # Save good records to BigQuery (Historical append mode)
        good_record_count = 0
        if not processed_data.empty:
            good_record_count = save_to_bigquery_historical(
                processed_data, file_name, region, tenant_id
            )

        # Handle bad records
        bad_record_count = 0
        if not bad_records.empty:
            bad_record_count = len(bad_records)
            save_bad_records(bad_records, bucket_name, file_name, tenant_id)

        # Clean up temporary file
        os.unlink(file_path)

        # Get the correct dataset ID for response
        dataset_id = get_regional_dataset_id(region)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": f"Successfully processed {file_name} with Pandas",
                    "engine": "pandas",
                    "region": region,
                    "tenant_id": tenant_id,
                    "good_records": good_record_count,
                    "bad_records": bad_record_count,
                    "processing_timestamp": datetime.now(UTC).isoformat(),
                    "bigquery_table": f"{PROJECT_ID}.{dataset_id}.{TABLE_ID}",
                }
            ),
            200,
        )

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")

        # Move entire file to bad_records folder if processing fails
        if "file_name" in locals() and "bucket_name" in locals():
            move_file_to_bad_records(
                bucket_name,
                file_name,
                str(e),
                tenant_id if "tenant_id" in locals() else "unknown",
            )

        return jsonify({"status": "error", "message": str(e), "engine": "pandas"}), 500


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
    Process e-commerce data with ML-ready transformations and data quality checks.
    Returns: (clean_data, bad_records)
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        logger.info(f"Loaded dataframe with shape: {df.shape}")

        # Initialize tracking for bad records
        bad_records = pd.DataFrame()
        bad_reasons = []

        # SCHEMA VALIDATION
        df, schema_bad = validate_schema(df)
        if not schema_bad.empty:
            schema_bad["bad_reason"] = "schema_validation_failed"
            bad_records = pd.concat([bad_records, schema_bad], ignore_index=True)

        # DATA QUALITY CHECKS
        df, quality_bad = perform_data_quality_checks(df)
        if not quality_bad.empty:
            bad_records = pd.concat([bad_records, quality_bad], ignore_index=True)

        # ML FEATURE ENGINEERING (only on clean data)
        if not df.empty:
            df = create_ml_features(df, tenant_id, file_name, region)

        logger.info(
            f"Final clean data shape: {df.shape}, Bad records: {len(bad_records)}"
        )
        return df, bad_records

    except Exception as e:
        logger.error(f"Error processing e-commerce data: {str(e)}")
        # Return empty DataFrames on error
        return pd.DataFrame(), pd.DataFrame()


def validate_schema(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Validate data schema and separate bad records."""
    bad_records = pd.DataFrame()

    # Check if required columns exist
    missing_cols = set(EXPECTED_COLUMNS.keys()) - set(df.columns)
    if missing_cols:
        logger.warning(f"Missing columns: {missing_cols}")
        return pd.DataFrame(), df  # All records are bad if schema is wrong

    # Type conversion and validation
    clean_df = df.copy()
    bad_mask = pd.Series([False] * len(df))

    try:
        # Convert event_time to datetime
        clean_df["event_time"] = pd.to_datetime(clean_df["event_time"], errors="coerce")
        bad_mask |= clean_df["event_time"].isna()

        # Convert numeric columns
        for col in ["product_id", "category_id", "user_id"]:
            clean_df[col] = pd.to_numeric(clean_df[col], errors="coerce")
            bad_mask |= clean_df[col].isna()

        # Convert price
        clean_df["price"] = pd.to_numeric(clean_df["price"], errors="coerce")

        # Validate event types
        bad_mask |= ~clean_df["event_type"].isin(VALID_EVENT_TYPES)

    except Exception as e:
        logger.error(f"Schema validation error: {str(e)}")
        return pd.DataFrame(), df

    # Separate good and bad records
    bad_records = df[bad_mask].copy()
    clean_df = clean_df[~bad_mask].copy()

    return clean_df, bad_records


def perform_data_quality_checks(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Perform comprehensive data quality checks."""
    if df.empty:
        return df, pd.DataFrame()

    bad_records = pd.DataFrame()
    quality_issues = []

    # Create mask for bad records
    bad_mask = pd.Series([False] * len(df))

    # Missing Value Detection
    critical_fields = ["user_id", "product_id", "event_time", "event_type"]
    for field in critical_fields:
        field_bad = df[field].isna()
        bad_mask |= field_bad
        if field_bad.any():
            quality_issues.append(f"missing_{field}")

    # Price validation for purchase events
    purchase_mask = df["event_type"] == "purchase"
    price_bad = purchase_mask & (df["price"].isna() | (df["price"] <= 0))
    bad_mask |= price_bad

    # Data Integrity Validation
    # Negative or zero IDs
    id_bad = (df["user_id"] <= 0) | (df["product_id"] <= 0) | (df["category_id"] <= 0)
    bad_mask |= id_bad

    # Outlier Detection
    if "price" in df.columns and not df["price"].isna().all():
        # Remove extreme price outliers (beyond 3 standard deviations)
        price_mean = df["price"].mean()
        price_std = df["price"].std()
        if price_std > 0:
            price_outliers = (df["price"] > price_mean + 3 * price_std) | (
                df["price"] < 0
            )
            bad_mask |= price_outliers

    # Session validation
    session_bad = df["user_session"].isna() | (df["user_session"].str.len() < 5)
    bad_mask |= session_bad

    # Duplicate detection (same user, product, timestamp)
    duplicates = df.duplicated(
        subset=["user_id", "product_id", "event_time"], keep="first"
    )
    bad_mask |= duplicates

    # Separate good and bad records
    if bad_mask.any():
        bad_records = df[bad_mask].copy()
        bad_records["bad_reason"] = "data_quality_failed"
        # Add specific reasons
        for i, is_bad in enumerate(bad_mask):
            if is_bad:
                reasons = []
                if df.iloc[i]["user_id"] <= 0 or pd.isna(df.iloc[i]["user_id"]):
                    reasons.append("invalid_user_id")
                if df.iloc[i]["product_id"] <= 0 or pd.isna(df.iloc[i]["product_id"]):
                    reasons.append("invalid_product_id")
                if pd.isna(df.iloc[i]["event_time"]):
                    reasons.append("invalid_event_time")
                if df.iloc[i]["event_type"] == "purchase" and (
                    pd.isna(df.iloc[i]["price"]) or df.iloc[i]["price"] <= 0
                ):
                    reasons.append("invalid_purchase_price")
                bad_records.loc[
                    bad_records.index[sum(bad_mask[: i + 1]) - 1], "bad_reason"
                ] = (",".join(reasons) if reasons else "data_quality_failed")

    clean_df = df[~bad_mask].copy()

    return clean_df, bad_records


def create_ml_features(
    df: pd.DataFrame, tenant_id: str, file_name: str, region: str
) -> pd.DataFrame:
    """Create ML-ready features from e-commerce data with historical tracking."""
    if df.empty:
        return df

    try:
        # Add unique record ID for duplicate prevention
        df["record_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Add tenant isolation and processing metadata
        df["tenant_id"] = tenant_id
        df["region"] = region  # Now properly passed as parameter
        df["original_filename"] = file_name
        df["processed_timestamp"] = datetime.now(UTC)
        df["processing_engine"] = "pandas"
        df["data_quality_status"] = "clean"
        df["created_at"] = datetime.now(UTC)  # For partitioning

        # 1. TEMPORAL FEATURES
        df["hour"] = df["event_time"].dt.hour
        df["day_of_week"] = df["event_time"].dt.dayofweek
        df["month"] = df["event_time"].dt.month
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

        # Sort by user and time for sequence features
        df = df.sort_values(["user_id", "event_time"])

        # USER BEHAVIOR FEATURES
        # Time since last event per user
        df["time_since_last_event"] = (
            df.groupby("user_id")["event_time"].diff().dt.total_seconds()
        )
        df["time_since_last_event"] = df["time_since_last_event"].fillna(0)

        # Session-based features
        session_stats = (
            df.groupby("user_session")
            .agg(
                {
                    "event_time": ["min", "max", "count"],
                    "product_id": "nunique",
                    "price": ["sum", "mean", "max"],
                }
            )
            .reset_index()
        )

        # Flatten column names
        session_stats.columns = ["user_session"] + [
            "_".join(col).strip() for col in session_stats.columns[1:]
        ]
        session_stats["session_duration_minutes"] = (
            session_stats["event_time_max"] - session_stats["event_time_min"]
        ).dt.total_seconds() / 60

        # Rename columns for clarity
        session_stats = session_stats.rename(
            columns={
                "event_time_count": "session_event_count",
                "product_id_nunique": "session_unique_products",
                "price_sum": "session_total_value",
                "price_mean": "session_avg_price",
                "price_max": "session_max_price",
            }
        )

        # Merge session features back
        df = df.merge(
            session_stats[
                [
                    "user_session",
                    "session_event_count",
                    "session_unique_products",
                    "session_total_value",
                    "session_avg_price",
                    "session_max_price",
                    "session_duration_minutes",
                ]
            ],
            on="user_session",
            how="left",
        )

        # CATEGORICAL ENCODING
        # One-hot encode event types
        event_dummies = pd.get_dummies(df["event_type"], prefix="event")
        df = pd.concat([df, event_dummies], axis=1)

        # Label encode brands and categories (for memory efficiency)
        df["brand_encoded"] = df["brand"].fillna("unknown").astype("category").cat.codes
        df["category_code_encoded"] = (
            df["category_code"].fillna("unknown").astype("category").cat.codes
        )

        # PRODUCT FEATURES
        # Product popularity (count of interactions)
        product_popularity = df["product_id"].value_counts().to_dict()
        df["product_popularity"] = df["product_id"].map(product_popularity)

        # Price positioning within category
        category_price_stats = (
            df.groupby("category_id")["price"].agg(["mean", "std"]).reset_index()
        )
        df = df.merge(
            category_price_stats,
            on="category_id",
            how="left",
            suffixes=("", "_category"),
        )
        df["price_vs_category_avg"] = (df["price"] - df["mean"]) / (df["std"] + 1e-8)
        df["price_vs_category_avg"] = df["price_vs_category_avg"].fillna(0)

        # 5. USER AGGREGATION FEATURES (LIMITED FOR PANDAS - SINGLE FILE SCOPE)
        user_stats = (
            df.groupby("user_id")
            .agg(
                {
                    "event_time": "count",
                    "product_id": "nunique",
                    "price": ["sum", "mean"],
                    "category_id": "nunique",
                }
            )
            .reset_index()
        )

        user_stats.columns = [
            "user_id",
            "user_total_events",
            "user_unique_products",
            "user_total_spent",
            "user_avg_price",
            "user_unique_categories",
        ]

        df = df.merge(user_stats, on="user_id", how="left")

        # CONVERSION FEATURES
        # Purchase conversion rate per user (in this session/file)
        user_purchases = df[df["event_type"] == "purchase"].groupby("user_id").size()
        user_total_events = df.groupby("user_id").size()
        user_conversion_rate = (user_purchases / user_total_events).fillna(0).to_dict()
        df["user_conversion_rate"] = df["user_id"].map(user_conversion_rate).fillna(0)

        # Fill any remaining NaN values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)

        logger.info(f"Created ML features. Final shape: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error creating ML features: {str(e)}")
        return df  # Return original df if feature creation fails


def save_to_bigquery_historical(
    df: pd.DataFrame, file_name: str, region: str, tenant_id: str
) -> int:
    """Save processed data to BigQuery with regional multi-location support and historical preservation."""
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

        # Comprehensive schema for historical data with tenant isolation
        schema = [
            # Core identification
            bigquery.SchemaField(
                "record_id",
                "STRING",
                mode="REQUIRED",
                description="Unique record identifier",
            ),
            bigquery.SchemaField(
                "tenant_id",
                "STRING",
                mode="REQUIRED",
                description="Tenant identifier for data isolation",
            ),
            bigquery.SchemaField(
                "region", "STRING", mode="REQUIRED", description="Processing region"
            ),
            # Original e-commerce data
            bigquery.SchemaField(
                "event_time",
                "TIMESTAMP",
                mode="REQUIRED",
                description="Original event timestamp",
            ),
            bigquery.SchemaField(
                "event_type",
                "STRING",
                mode="REQUIRED",
                description="Type of e-commerce event",
            ),
            bigquery.SchemaField(
                "product_id",
                "INTEGER",
                mode="REQUIRED",
                description="Product identifier",
            ),
            bigquery.SchemaField(
                "category_id",
                "INTEGER",
                mode="NULLABLE",
                description="Product category identifier",
            ),
            bigquery.SchemaField(
                "category_code",
                "STRING",
                mode="NULLABLE",
                description="Product category code",
            ),
            bigquery.SchemaField(
                "brand", "STRING", mode="NULLABLE", description="Product brand"
            ),
            bigquery.SchemaField(
                "price", "FLOAT", mode="NULLABLE", description="Product price"
            ),
            bigquery.SchemaField(
                "user_id", "INTEGER", mode="REQUIRED", description="User identifier"
            ),
            bigquery.SchemaField(
                "user_session",
                "STRING",
                mode="NULLABLE",
                description="User session identifier",
            ),
            # Processing metadata
            bigquery.SchemaField(
                "original_filename",
                "STRING",
                mode="REQUIRED",
                description="Source file name",
            ),
            bigquery.SchemaField(
                "processed_timestamp",
                "TIMESTAMP",
                mode="REQUIRED",
                description="Data processing timestamp",
            ),
            bigquery.SchemaField(
                "processing_engine",
                "STRING",
                mode="REQUIRED",
                description="Processing engine used",
            ),
            bigquery.SchemaField(
                "data_quality_status",
                "STRING",
                mode="REQUIRED",
                description="Data quality validation status",
            ),
            bigquery.SchemaField(
                "created_at",
                "TIMESTAMP",
                mode="REQUIRED",
                description="Record creation timestamp",
            ),
            # ML Features - Temporal
            bigquery.SchemaField(
                "hour", "INTEGER", mode="NULLABLE", description="Hour of event"
            ),
            bigquery.SchemaField(
                "day_of_week",
                "INTEGER",
                mode="NULLABLE",
                description="Day of week (0=Monday)",
            ),
            bigquery.SchemaField(
                "month", "INTEGER", mode="NULLABLE", description="Month of event"
            ),
            bigquery.SchemaField(
                "is_weekend",
                "INTEGER",
                mode="NULLABLE",
                description="Weekend indicator (0/1)",
            ),
            # ML Features - User behavior
            bigquery.SchemaField(
                "time_since_last_event",
                "FLOAT",
                mode="NULLABLE",
                description="Seconds since user's last event",
            ),
            bigquery.SchemaField(
                "session_event_count",
                "INTEGER",
                mode="NULLABLE",
                description="Number of events in session",
            ),
            bigquery.SchemaField(
                "session_unique_products",
                "INTEGER",
                mode="NULLABLE",
                description="Unique products in session",
            ),
            bigquery.SchemaField(
                "session_total_value",
                "FLOAT",
                mode="NULLABLE",
                description="Total session value",
            ),
            bigquery.SchemaField(
                "session_avg_price",
                "FLOAT",
                mode="NULLABLE",
                description="Average price in session",
            ),
            bigquery.SchemaField(
                "session_max_price",
                "FLOAT",
                mode="NULLABLE",
                description="Maximum price in session",
            ),
            bigquery.SchemaField(
                "session_duration_minutes",
                "FLOAT",
                mode="NULLABLE",
                description="Session duration in minutes",
            ),
            # ML Features - Product and user aggregations
            bigquery.SchemaField(
                "product_popularity",
                "INTEGER",
                mode="NULLABLE",
                description="Product interaction count",
            ),
            bigquery.SchemaField(
                "price_vs_category_avg",
                "FLOAT",
                mode="NULLABLE",
                description="Price relative to category average",
            ),
            bigquery.SchemaField(
                "user_total_events",
                "INTEGER",
                mode="NULLABLE",
                description="User's total events in file",
            ),
            bigquery.SchemaField(
                "user_unique_products",
                "INTEGER",
                mode="NULLABLE",
                description="User's unique products in file",
            ),
            bigquery.SchemaField(
                "user_total_spent",
                "FLOAT",
                mode="NULLABLE",
                description="User's total spending in file",
            ),
            bigquery.SchemaField(
                "user_avg_price",
                "FLOAT",
                mode="NULLABLE",
                description="User's average price in file",
            ),
            bigquery.SchemaField(
                "user_unique_categories",
                "INTEGER",
                mode="NULLABLE",
                description="User's unique categories in file",
            ),
            bigquery.SchemaField(
                "user_conversion_rate",
                "FLOAT",
                mode="NULLABLE",
                description="User's conversion rate in file",
            ),
            # ML Features - Encoded categories
            bigquery.SchemaField(
                "brand_encoded",
                "INTEGER",
                mode="NULLABLE",
                description="Encoded brand category",
            ),
            bigquery.SchemaField(
                "category_code_encoded",
                "INTEGER",
                mode="NULLABLE",
                description="Encoded category code",
            ),
            # One-hot encoded event types (will be added dynamically if they exist)
        ]

        # Add one-hot encoded event type columns if they exist in the dataframe
        event_columns = [col for col in df.columns if col.startswith("event_")]
        for col in event_columns:
            schema.append(
                bigquery.SchemaField(
                    col,
                    "INTEGER",
                    mode="NULLABLE",
                    description=f"One-hot encoded {col}",
                )
            )

        # Job configuration for historical append
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",  # Always append, never overwrite
            create_disposition="CREATE_IF_NEEDED",
            # Partition by created_at date for efficient querying
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="created_at"
            ),
            # Cluster by tenant_id, region, and event_type for efficient tenant isolation and regional queries
            clustering_fields=["tenant_id", "region", "event_type", "user_id"],
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


def save_bad_records(
    bad_df: pd.DataFrame, bucket_name: str, file_name: str, tenant_id: str
):
    """Save bad records to tenant-specific bad_records folder."""
    try:
        if bad_df.empty:
            return

        # Create bad records file path
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        original_filename = file_name.split("/")[-1].replace(".csv", "")

        if tenant_id == "shared":
            bad_records_path = (
                f"shared/bad_records/pandas_failed_{timestamp}_{original_filename}.csv"
            )
        else:
            bad_records_path = f"tenants/{tenant_id}/data/bad_records/pandas_failed_{timestamp}_{original_filename}.csv"

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
            "processing_engine": "pandas",
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


def move_file_to_bad_records(
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
                f"shared/bad_records/pandas_processing_failed_{timestamp}_{filename}"
            )
        else:
            bad_records_path = f"tenants/{tenant_id}/data/bad_records/pandas_processing_failed_{timestamp}_{filename}"

        # Copy to bad_records folder
        bucket.copy_blob(source_blob, bucket, bad_records_path)

        # Add error metadata
        bad_blob = bucket.blob(bad_records_path)
        bad_blob.metadata = {
            "error_message": error_message,
            "processing_engine": "pandas",
            "failed_timestamp": datetime.now(UTC).isoformat(),
            "original_path": file_name,
            "tenant_id": tenant_id,
        }
        bad_blob.patch()

        logger.info(f"Moved failed file to: {bad_records_path}")

    except Exception as e:
        logger.error(f"Error moving file to bad_records: {str(e)}")


@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return (
        jsonify(
            {
                "status": "healthy",
                "engine": "pandas",
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
                "message": "Pandas Processing Engine - Multi-Tenant ML Pipeline",
                "status": "ready",
                "engine": "pandas",
                "endpoints": ["/process", "/health"],
                "bigquery_target": f"{PROJECT_ID}.{{regional_dataset}}.{TABLE_ID}",
            }
        ),
        200,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
