#!/usr/bin/env python3
"""
Comprehensive pytest unit tests for the Dask Processing Engine.

This test suite covers:
- Utility functions (tenant extraction, dataset ID generation, etc.)
- Dask-specific data processing (distributed operations, partitioning)
- Flask endpoints (/process, /health, /)
- GCS file operations (download, upload, bad records handling)
- BigQuery operations (table creation, data insertion, schema management)
- Error handling and edge cases
- Performance scenarios and distributed computing
- Multi-tenant isolation and regional support
- Dask-specific features (lazy evaluation, distributed processing)
"""

import io
import json
import os
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, mock_open, patch

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pytest
# Import the Flask app and functions to test
from main import (BASE_DATASET_ID, EXPECTED_COLUMNS, PROJECT_ID,
                  REGION_TO_BQ_LOCATION, TABLE_ID, VALID_EVENT_TYPES, app,
                  create_ml_features_dask, download_file_from_gcs,
                  extract_tenant_from_path, get_bigquery_location,
                  get_regional_dataset_id, move_file_to_bad_records_dask,
                  perform_data_quality_checks_dask, process_ecommerce_data,
                  save_bad_records_dask, save_to_bigquery_historical_dask,
                  validate_schema_dask)

# ============================================================================
# PYTEST FIXTURES
# ============================================================================


@pytest.fixture
def client():
    """Flask test client fixture."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def sample_ecommerce_data():
    """Fixture providing sample e-commerce data as pandas DataFrame."""
    return pd.DataFrame(
        {
            "event_time": [
                "2024-01-01 10:00:00",
                "2024-01-01 10:05:00",
                "2024-01-01 10:10:00",
                "2024-01-01 10:15:00",
            ],
            "event_type": ["view", "cart", "purchase", "view"],
            "product_id": [12345, 12345, 12345, 67890],
            "category_id": [100, 100, 100, 200],
            "category_code": [
                "electronics.smartphones",
                "electronics.smartphones",
                "electronics.smartphones",
                "home.furniture",
            ],
            "brand": ["Apple", "Apple", "Apple", "IKEA"],
            "price": [999.99, 999.99, 999.99, 299.50],
            "user_id": [1001, 1001, 1001, 1002],
            "user_session": [
                "session_abc123",
                "session_abc123",
                "session_abc123",
                "session_def456",
            ],
        }
    )


@pytest.fixture
def sample_dask_data(sample_ecommerce_data):
    """Fixture providing sample e-commerce data as Dask DataFrame."""
    return dd.from_pandas(sample_ecommerce_data, npartitions=2)


@pytest.fixture
def bad_ecommerce_data():
    """Fixture providing bad e-commerce data for quality testing."""
    return pd.DataFrame(
        {
            "event_time": [
                "2024-01-01 10:00:00",
                "invalid_date",
                "2024-01-01 10:10:00",
                "2024-01-01 10:15:00",
            ],
            "event_type": ["view", "invalid_event", "purchase", "view"],
            "product_id": [12345, -1, 12345, None],  # Invalid: negative and None
            "category_id": [100, 100, 100, 200],
            "category_code": [
                "electronics.smartphones",
                "electronics.smartphones",
                "electronics.smartphones",
                "home.furniture",
            ],
            "brand": ["Apple", "Apple", "Apple", "IKEA"],
            "price": [999.99, 999.99, 0, 299.50],  # Invalid: zero price for purchase
            "user_id": [1001, 1001, 0, 1002],  # Invalid: zero user_id
            "user_session": [
                "session_abc123",
                "session_abc123",
                "sess",
                "session_def456",
            ],  # Invalid: too short
        }
    )


@pytest.fixture
def bad_dask_data(bad_ecommerce_data):
    """Fixture providing bad e-commerce data as Dask DataFrame."""
    return dd.from_pandas(bad_ecommerce_data, npartitions=2)


@pytest.fixture
def large_ecommerce_data():
    """Fixture providing large dataset for performance testing."""
    np.random.seed(42)
    n_records = 10000

    data = pd.DataFrame(
        {
            "event_time": pd.date_range("2024-01-01", periods=n_records, freq="1min"),
            "event_type": np.random.choice(VALID_EVENT_TYPES, n_records),
            "product_id": np.random.randint(1, 1000, n_records),
            "category_id": np.random.randint(1, 50, n_records),
            "category_code": np.random.choice(
                ["electronics.smartphones", "home.furniture", "fashion.shoes"],
                n_records,
            ),
            "brand": np.random.choice(["Apple", "Samsung", "Nike", "IKEA"], n_records),
            "price": np.random.uniform(10, 2000, n_records).round(2),
            "user_id": np.random.randint(1, 500, n_records),
            "user_session": [
                f"session_{uuid.uuid4().hex[:8]}" for _ in range(n_records)
            ],
        }
    )

    return dd.from_pandas(data, npartitions=10)  # More partitions for Dask testing


@pytest.fixture
def sample_request_payload():
    """Fixture providing sample API request payload."""
    return {
        "file_info": {
            "file_name": "tenants/demo/data/raw/ecommerce-events-2024.csv",
            "bucket_name": "terraops-us-west1-tenant-data",
            "file_size": 1024000,
            "region": "us-west1",
        }
    }


@pytest.fixture
def mock_gcs_client():
    """Mock Google Cloud Storage client."""
    with patch("main.storage_client") as mock_client:
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        yield mock_client, mock_bucket, mock_blob


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client."""
    with patch("main.bq_client") as mock_client:
        mock_dataset = Mock()
        mock_table = Mock()
        mock_job = Mock()
        mock_client.dataset.return_value = mock_dataset
        mock_client.get_table.return_value = mock_table
        mock_client.load_table_from_dataframe.return_value = mock_job
        mock_table.num_rows = 1000
        mock_job.result.return_value = None
        yield mock_client, mock_dataset, mock_table, mock_job


# ============================================================================
# UTILITY FUNCTIONS TESTS
# ============================================================================


class TestUtilityFunctionsDask:
    """Test cases for utility functions in Dask engine."""

    def test_extract_tenant_from_path_valid_tenant(self):
        """Test extracting valid tenant ID from file path."""
        test_cases = [
            ("tenants/demo/data/raw/file.csv", "demo"),
            ("tenants/enterprise/data/processed/file.csv", "enterprise"),
            ("tenants/test-tenant-123/data/raw/file.csv", "test-tenant-123"),
        ]

        for path, expected_tenant in test_cases:
            result = extract_tenant_from_path(path)
            assert result == expected_tenant

    def test_extract_tenant_from_path_shared_tenant(self):
        """Test extracting shared tenant from file path."""
        test_cases = [
            "shared/data/raw/file.csv",
            "shared/common/file.csv",
        ]

        for path in test_cases:
            result = extract_tenant_from_path(path)
            assert result == "shared"

    def test_extract_tenant_from_path_invalid_paths(self):
        """Test extracting tenant from invalid file paths."""
        test_cases_unknown = [
            "invalid/path/file.csv",
            "file.csv",
            "",
        ]

        for path in test_cases_unknown:
            result = extract_tenant_from_path(path)
            assert result == "unknown"

    def test_extract_tenant_from_path_exception_handling(self):
        """Test tenant extraction with exception scenarios."""
        result = extract_tenant_from_path(None)
        assert result == "unknown"

    @pytest.mark.parametrize(
        "region,expected_dataset",
        [
            ("us-west1", "Tenants_us_west1"),
            ("europe-west1", "Tenants_europe_west1"),
            ("asia-southeast1", "Tenants_asia_southeast1"),
            ("us-central1", "Tenants_us_central1"),
        ],
    )
    def test_get_regional_dataset_id(self, region, expected_dataset):
        """Test regional dataset ID generation."""
        result = get_regional_dataset_id(region)
        assert result == expected_dataset

    @pytest.mark.parametrize(
        "region,expected_location",
        [
            ("us-west1", "US"),
            ("us-central1", "US"),
            ("us-east1", "US"),
            ("europe-west1", "EU"),
            ("europe-west2", "EU"),
            ("asia-southeast1", "asia-southeast1"),
            ("unknown-region", "US"),  # Default fallback
        ],
    )
    def test_get_bigquery_location(self, region, expected_location):
        """Test BigQuery location mapping."""
        result = get_bigquery_location(region)
        assert result == expected_location


# ============================================================================
# DASK-SPECIFIC DATA PROCESSING TESTS
# ============================================================================


class TestDaskDataProcessing:
    """Test cases for Dask-specific data processing functions."""

    def test_validate_schema_dask_success(self, sample_dask_data):
        """Test successful schema validation with Dask DataFrame."""
        clean_data, bad_data = validate_schema_dask(sample_dask_data)

        assert not clean_data.compute().empty
        assert bad_data.empty
        assert len(clean_data.compute()) == len(sample_dask_data.compute())

    def test_validate_schema_dask_missing_columns(self):
        """Test schema validation with missing columns in Dask."""
        incomplete_data = dd.from_pandas(
            pd.DataFrame(
                {
                    "event_time": ["2024-01-01 10:00:00"],
                    "product_id": [12345],
                    # Missing required columns
                }
            ),
            npartitions=1,
        )

        clean_data, bad_data = validate_schema_dask(incomplete_data)

        assert clean_data.compute().empty
        assert len(bad_data) == len(incomplete_data.compute())

    def test_validate_schema_dask_invalid_data_types(self, bad_dask_data):
        """Test schema validation with invalid data types in Dask."""
        clean_data, bad_data = validate_schema_dask(bad_dask_data)

        # Should have some bad records due to invalid dates, event types, etc.
        assert not bad_data.empty
        assert len(clean_data.compute()) < len(bad_dask_data.compute())

    def test_perform_data_quality_checks_dask_success(self, sample_dask_data):
        """Test successful data quality checks with Dask."""
        # First validate schema to get properly typed data
        validated_data, _ = validate_schema_dask(sample_dask_data)

        clean_data, bad_data = perform_data_quality_checks_dask(validated_data)

        assert not clean_data.compute().empty
        assert bad_data.empty or len(bad_data) == 0

    def test_perform_data_quality_checks_dask_with_issues(self):
        """Test data quality checks with various issues using Dask."""
        problem_data = pd.DataFrame(
            {
                "event_time": pd.to_datetime(
                    [
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:05:00",
                        "2024-01-01 10:10:00",
                    ]
                ),
                "event_type": ["purchase", "view", "purchase"],
                "product_id": [12345, -1, 12345],  # Negative ID
                "category_id": [100, 100, 100],
                "category_code": ["electronics", "electronics", "electronics"],
                "brand": ["Apple", "Apple", "Apple"],
                "price": [999.99, 299.50, 0],  # Zero price for purchase
                "user_id": [1001, 0, 1001],  # Zero user ID
                "user_session": [
                    "session_abc123",
                    "sess",
                    "session_def456",
                ],  # Too short session
            }
        )

        problem_dask_data = dd.from_pandas(problem_data, npartitions=2)
        clean_data, bad_data = perform_data_quality_checks_dask(problem_dask_data)

        assert not bad_data.empty
        assert len(clean_data.compute()) < len(problem_data)
        assert "bad_reason" in bad_data.columns

    def test_perform_data_quality_checks_dask_empty_data(self):
        """Test data quality checks with empty Dask dataframe."""
        empty_data = dd.from_pandas(pd.DataFrame(), npartitions=1)

        clean_data, bad_data = perform_data_quality_checks_dask(empty_data)

        assert clean_data.compute().empty
        assert bad_data.empty

    def test_create_ml_features_dask_success(self, sample_dask_data):
        """Test successful ML feature creation with Dask."""
        # First validate and clean the data
        validated_data, _ = validate_schema_dask(sample_dask_data)
        clean_data, _ = perform_data_quality_checks_dask(validated_data)

        result = create_ml_features_dask(
            clean_data, "demo", "test_file.csv", "us-west1"
        )
        computed_result = result.compute()

        # Check that features were added
        expected_features = [
            "record_id",
            "tenant_id",
            "region",
            "processing_engine",
            "hour",
            "day_of_week",
            "month",
            "is_weekend",
            "time_since_last_event",
            "session_event_count",
            "product_popularity",
            "user_conversion_rate",
        ]

        for feature in expected_features:
            assert feature in computed_result.columns

        # Check metadata
        assert computed_result["tenant_id"].iloc[0] == "demo"
        assert computed_result["region"].iloc[0] == "us-west1"
        assert computed_result["processing_engine"].iloc[0] == "dask"

    def test_create_ml_features_dask_empty_data(self):
        """Test ML feature creation with empty Dask dataframe."""
        empty_data = dd.from_pandas(pd.DataFrame(), npartitions=1)

        result = create_ml_features_dask(
            empty_data, "demo", "test_file.csv", "us-west1"
        )

        assert result.compute().empty

    def test_dask_partitioning_behavior(self, sample_dask_data):
        """Test Dask-specific partitioning behavior."""
        assert sample_dask_data.npartitions >= 2

        # Test that operations maintain partitioning
        validated_data, _ = validate_schema_dask(sample_dask_data)
        assert validated_data.npartitions >= 1

        # Test partition-wise operations (simplified)
        total_rows = len(sample_dask_data.compute())
        assert total_rows > 0

    def test_dask_lazy_evaluation(self, sample_dask_data):
        """Test Dask lazy evaluation doesn't execute until compute()."""
        # Operations should be lazy
        validated_data, _ = validate_schema_dask(sample_dask_data)
        clean_data, _ = perform_data_quality_checks_dask(validated_data)

        # These should not execute until compute() is called
        assert hasattr(clean_data, "compute")
        assert hasattr(validated_data, "compute")

        # Only when we call compute should we get pandas DataFrame
        result = clean_data.compute()
        assert isinstance(result, pd.DataFrame)


# ============================================================================
# GCS OPERATIONS TESTS (DASK-SPECIFIC)
# ============================================================================


class TestGCSOperationsDask:
    """Test cases for Google Cloud Storage operations in Dask engine."""

    def test_download_file_from_gcs_success(self, mock_gcs_client):
        """Test successful file download from GCS."""
        mock_client, mock_bucket, mock_blob = mock_gcs_client

        with patch("tempfile.NamedTemporaryFile") as mock_temp:
            mock_temp.return_value.name = "/tmp/test_file.csv"

            result = download_file_from_gcs("test-bucket", "test-file.csv")

            mock_client.bucket.assert_called_once_with("test-bucket")
            mock_bucket.blob.assert_called_once_with("test-file.csv")
            mock_blob.download_to_filename.assert_called_once()

            assert result == "/tmp/test_file.csv"

    def test_download_file_from_gcs_failure(self, mock_gcs_client):
        """Test file download failure from GCS."""
        mock_client, mock_bucket, mock_blob = mock_gcs_client
        mock_blob.download_to_filename.side_effect = Exception("Download failed")

        with pytest.raises(Exception, match="Download failed"):
            download_file_from_gcs("test-bucket", "test-file.csv")

    def test_save_bad_records_dask_success(self, mock_gcs_client, bad_ecommerce_data):
        """Test successful bad records saving with Dask engine."""
        mock_client, mock_bucket, mock_blob = mock_gcs_client

        with patch("tempfile.NamedTemporaryFile") as mock_temp, patch(
            "os.unlink"
        ) as mock_unlink, patch("pandas.DataFrame.to_csv") as mock_to_csv:

            mock_file = Mock()
            mock_file.name = "C:\\temp\\bad_records.csv"
            mock_temp.return_value = mock_file

            save_bad_records_dask(
                bad_ecommerce_data, "test-bucket", "tenants/demo/test.csv", "demo"
            )

            mock_client.bucket.assert_called_once_with("test-bucket")
            mock_blob.upload_from_filename.assert_called_once()
            mock_blob.patch.assert_called_once()
            mock_unlink.assert_called_once()

    def test_save_bad_records_dask_empty_data(self, mock_gcs_client):
        """Test saving empty bad records with Dask engine."""
        mock_client, mock_bucket, mock_blob = mock_gcs_client
        empty_data = pd.DataFrame()

        save_bad_records_dask(empty_data, "test-bucket", "test.csv", "demo")

        # Should not attempt any GCS operations for empty data
        mock_client.bucket.assert_not_called()

    def test_move_file_to_bad_records_dask_success(self, mock_gcs_client):
        """Test successful file move to bad records with Dask engine."""
        mock_client, mock_bucket, mock_blob = mock_gcs_client

        move_file_to_bad_records_dask(
            "test-bucket", "tenants/demo/test.csv", "Processing error", "demo"
        )

        mock_client.bucket.assert_called_with("test-bucket")
        mock_bucket.copy_blob.assert_called_once()
        mock_blob.patch.assert_called_once()

    def test_move_file_to_bad_records_dask_shared_tenant(self, mock_gcs_client):
        """Test file move for shared tenant with Dask engine."""
        mock_client, mock_bucket, mock_blob = mock_gcs_client

        move_file_to_bad_records_dask(
            "test-bucket", "shared/test.csv", "Processing error", "shared"
        )

        # Verify the path structure for shared tenant
        args, kwargs = mock_bucket.copy_blob.call_args
        destination_path = args[2]  # Third argument is the destination path
        assert destination_path.startswith("shared/bad_records/")


# ============================================================================
# BIGQUERY OPERATIONS TESTS (DASK-SPECIFIC)
# ============================================================================


class TestBigQueryOperationsDask:
    """Test cases for BigQuery operations in Dask engine."""

    def test_save_to_bigquery_historical_dask_success(
        self, mock_bq_client, sample_ecommerce_data
    ):
        """Test successful data saving to BigQuery with Dask engine."""
        mock_client, mock_dataset, mock_table, mock_job = mock_bq_client

        with patch(
            "main.get_regional_dataset_id", return_value="Tenants_us_west1"
        ), patch("main.get_bigquery_location", return_value="US"):

            result = save_to_bigquery_historical_dask(
                sample_ecommerce_data, "test.csv", "us-west1", "demo"
            )

            assert result == len(sample_ecommerce_data)
            mock_client.load_table_from_dataframe.assert_called_once()
            mock_job.result.assert_called_once()

    def test_save_to_bigquery_dask_dataset_creation(
        self, mock_bq_client, sample_ecommerce_data
    ):
        """Test BigQuery dataset creation when it doesn't exist with Dask."""
        mock_client, mock_dataset, mock_table, mock_job = mock_bq_client

        # Mock dataset not existing
        from google.cloud.exceptions import NotFound

        mock_client.get_dataset.side_effect = NotFound("Dataset not found")

        with patch(
            "main.get_regional_dataset_id", return_value="Tenants_us_west1"
        ), patch("main.get_bigquery_location", return_value="US"):

            result = save_to_bigquery_historical_dask(
                sample_ecommerce_data, "test.csv", "us-west1", "demo"
            )

            assert result == len(sample_ecommerce_data)
            mock_client.create_dataset.assert_called_once()

    def test_save_to_bigquery_dask_failure(self, mock_bq_client, sample_ecommerce_data):
        """Test BigQuery save failure handling with Dask."""
        mock_client, mock_dataset, mock_table, mock_job = mock_bq_client
        mock_job.result.side_effect = Exception("BigQuery error")

        with patch(
            "main.get_regional_dataset_id", return_value="Tenants_us_west1"
        ), patch("main.get_bigquery_location", return_value="US"):

            with pytest.raises(Exception, match="BigQuery error"):
                save_to_bigquery_historical_dask(
                    sample_ecommerce_data, "test.csv", "us-west1", "demo"
                )


# ============================================================================
# FLASK ENDPOINTS TESTS (DASK-SPECIFIC)
# ============================================================================


class TestFlaskEndpointsDask:
    """Test cases for Flask API endpoints in Dask engine."""

    def test_health_check_endpoint_dask(self, client):
        """Test health check endpoint for Dask engine."""
        response = client.get("/health")

        assert response.status_code == 200
        data = json.loads(response.data)

        assert data["status"] == "healthy"
        assert data["engine"] == "dask"
        assert "supported_formats" in data
        assert "csv" in data["supported_formats"]

    def test_root_endpoint_dask(self, client):
        """Test root endpoint for Dask engine."""
        response = client.get("/")

        assert response.status_code == 200
        data = json.loads(response.data)

        assert data["status"] == "ready"
        assert data["engine"] == "dask"
        assert "endpoints" in data

    def test_process_file_success_dask(
        self, client, sample_request_payload, sample_ecommerce_data
    ):
        """Test successful file processing endpoint with Dask."""
        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data"
        ) as mock_process, patch(
            "main.save_to_bigquery_historical_dask"
        ) as mock_save_bq, patch(
            "main.save_bad_records_dask"
        ) as mock_save_bad, patch(
            "os.unlink"
        ) as mock_unlink:

            mock_download.return_value = "/tmp/test_file.csv"
            mock_process.return_value = (sample_ecommerce_data, pd.DataFrame())
            mock_save_bq.return_value = len(sample_ecommerce_data)

            response = client.post("/process", json=sample_request_payload)

            assert response.status_code == 200
            data = json.loads(response.data)

            assert data["status"] == "success"
            assert data["engine"] == "dask"
            assert data["tenant_id"] == "demo"
            assert data["good_records"] == len(sample_ecommerce_data)
            assert data["bad_records"] == 0

    def test_process_file_with_bad_records_dask(
        self, client, sample_request_payload, sample_ecommerce_data, bad_ecommerce_data
    ):
        """Test file processing with bad records using Dask."""
        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data"
        ) as mock_process, patch(
            "main.save_to_bigquery_historical_dask"
        ) as mock_save_bq, patch(
            "main.save_bad_records_dask"
        ) as mock_save_bad, patch(
            "os.unlink"
        ) as mock_unlink:

            mock_download.return_value = "/tmp/test_file.csv"
            mock_process.return_value = (sample_ecommerce_data, bad_ecommerce_data)
            mock_save_bq.return_value = len(sample_ecommerce_data)

            response = client.post("/process", json=sample_request_payload)

            assert response.status_code == 200
            data = json.loads(response.data)

            assert data["good_records"] == len(sample_ecommerce_data)
            assert data["bad_records"] == len(bad_ecommerce_data)
            mock_save_bad.assert_called_once()

    def test_process_file_missing_payload_dask(self, client):
        """Test process endpoint with missing payload for Dask."""
        response = client.post("/process")

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data["status"] == "error"

    def test_process_file_invalid_payload_dask(self, client):
        """Test process endpoint with invalid payload for Dask."""
        invalid_payload = {"invalid": "data"}

        response = client.post("/process", json=invalid_payload)

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data["status"] == "error"

    def test_process_file_download_failure_dask(self, client, sample_request_payload):
        """Test process endpoint with download failure for Dask."""
        with patch(
            "main.download_file_from_gcs", side_effect=Exception("Download failed")
        ):

            response = client.post("/process", json=sample_request_payload)

            assert response.status_code == 500
            data = json.loads(response.data)
            assert data["status"] == "error"
            assert "Download failed" in data["message"]

    def test_process_file_processing_failure_dask(self, client, sample_request_payload):
        """Test process endpoint with data processing failure for Dask."""
        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data", side_effect=Exception("Processing failed")
        ), patch("main.move_file_to_bad_records_dask") as mock_move:

            mock_download.return_value = "/tmp/test_file.csv"

            response = client.post("/process", json=sample_request_payload)

            assert response.status_code == 500
            data = json.loads(response.data)
            assert data["status"] == "error"
            mock_move.assert_called_once()


# ============================================================================
# INTEGRATION TESTS (DASK-SPECIFIC)
# ============================================================================


class TestIntegrationDask:
    """Integration test cases for complete Dask workflow."""

    def test_complete_processing_workflow_dask(self, client, sample_request_payload):
        """Test complete file processing workflow from API to BigQuery with Dask."""
        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data"
        ) as mock_process, patch(
            "main.save_to_bigquery_historical_dask"
        ) as mock_save_bq, patch(
            "os.unlink"
        ) as mock_unlink:

            sample_data = pd.DataFrame(
                {
                    "event_time": [
                        "2024-01-01 10:00:00",
                        "2024-01-01 10:05:00",
                        "2024-01-01 10:10:00",
                    ],
                    "event_type": ["view", "cart", "purchase"],
                    "product_id": [12345, 12345, 12345],
                    "category_id": [100, 100, 100],
                    "category_code": ["electronics.smartphones"] * 3,
                    "brand": ["Apple"] * 3,
                    "price": [999.99, 999.99, 999.99],
                    "user_id": [1001, 1001, 1001],
                    "user_session": ["session_abc123"] * 3,
                }
            )

            mock_download.return_value = "/tmp/test_file.csv"
            mock_process.return_value = (sample_data, pd.DataFrame())
            mock_save_bq.return_value = 3

            response = client.post("/process", json=sample_request_payload)

            assert response.status_code == 200
            data = json.loads(response.data)

            # Verify complete workflow
            assert data["status"] == "success"
            assert data["engine"] == "dask"
            assert data["tenant_id"] == "demo"
            assert data["region"] == "us-west1"
            assert data["good_records"] > 0

            # Verify all functions were called
            mock_download.assert_called_once()
            mock_process.assert_called_once()
            mock_save_bq.assert_called_once()
            mock_unlink.assert_called_once()

    def test_multi_tenant_isolation_dask(self, client):
        """Test multi-tenant data isolation with Dask."""
        tenant_payloads = [
            {
                "file_info": {
                    "file_name": "tenants/demo/data/raw/file.csv",
                    "bucket_name": "test-bucket",
                    "file_size": 1000,
                    "region": "us-west1",
                }
            },
            {
                "file_info": {
                    "file_name": "tenants/enterprise/data/raw/file.csv",
                    "bucket_name": "test-bucket",
                    "file_size": 1000,
                    "region": "us-west1",
                }
            },
        ]

        with patch("main.download_file_from_gcs"), patch(
            "main.process_ecommerce_data"
        ) as mock_process, patch(
            "main.save_to_bigquery_historical_dask"
        ) as mock_save_bq, patch(
            "os.unlink"
        ):

            mock_process.return_value = (pd.DataFrame({"test": [1]}), pd.DataFrame())
            mock_save_bq.return_value = 1

            for payload in tenant_payloads:
                response = client.post("/process", json=payload)
                assert response.status_code == 200

                data = json.loads(response.data)
                expected_tenant = extract_tenant_from_path(
                    payload["file_info"]["file_name"]
                )
                assert data["tenant_id"] == expected_tenant


# ============================================================================
# PERFORMANCE TESTS (DASK-SPECIFIC)
# ============================================================================


class TestPerformanceDask:
    """Performance test cases for Dask engine."""

    def test_large_dataset_processing_dask(self, large_ecommerce_data):
        """Test processing of large datasets with Dask."""
        import time

        start_time = time.time()

        # Test schema validation with Dask
        clean_data, bad_data = validate_schema_dask(large_ecommerce_data)

        # Test quality checks with Dask
        quality_clean, quality_bad = perform_data_quality_checks_dask(clean_data)

        # Test ML feature creation with Dask
        ml_data = create_ml_features_dask(
            quality_clean, "perf_test", "large_file.csv", "us-west1"
        )

        # Only compute at the end (Dask lazy evaluation)
        final_result = ml_data.compute()

        end_time = time.time()
        processing_time = end_time - start_time

        # Performance assertions (Dask should be competitive with pandas)
        assert processing_time < 45  # Allow more time due to Dask overhead
        assert not final_result.empty
        assert len(final_result) > 0

        # Check that features were created
        expected_features = [
            "hour",
            "day_of_week",
            "product_popularity",
            "user_conversion_rate",
        ]
        for feature in expected_features:
            assert feature in final_result.columns

    def test_dask_distributed_processing_simulation(self, large_ecommerce_data):
        """Test Dask distributed processing capabilities."""
        import time

        start_time = time.time()

        # Test that Dask can handle multiple partitions efficiently
        assert large_ecommerce_data.npartitions == 10

        # Process each partition independently (simulating distributed processing)
        partition_results = []
        for i in range(large_ecommerce_data.npartitions):
            partition = large_ecommerce_data.get_partition(i)
            clean_partition, _ = validate_schema_dask(partition)
            partition_results.append(clean_partition)

        # Concatenate results
        final_data = dd.concat(partition_results)
        computed_result = final_data.compute()

        end_time = time.time()
        processing_time = end_time - start_time

        assert processing_time < 30  # Should be fast with partitioned processing
        assert not computed_result.empty
        assert len(computed_result) > 0

    def test_memory_usage_large_dataset_dask(self, large_ecommerce_data):
        """Test memory usage with large datasets using Dask."""
        import os

        import psutil

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Process large dataset with Dask (should be memory efficient)
        validated_data, _ = validate_schema_dask(large_ecommerce_data)
        clean_data, _ = perform_data_quality_checks_dask(validated_data)
        ml_data = create_ml_features_dask(
            clean_data, "memory_test", "large_file.csv", "us-west1"
        )

        # Only compute when necessary
        final_result = ml_data.compute()

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory

        # Dask should be more memory efficient than pandas
        assert memory_increase < 600  # Allow slightly more for Dask overhead
        assert not final_result.empty

    def test_dask_lazy_evaluation_performance(self, sample_dask_data):
        """Test that Dask lazy evaluation provides performance benefits."""
        import time

        start_time = time.time()

        # Build computation graph (should be very fast)
        validated_data, _ = validate_schema_dask(sample_dask_data)
        clean_data, _ = perform_data_quality_checks_dask(validated_data)
        ml_data = create_ml_features_dask(
            clean_data, "lazy_test", "test.csv", "us-west1"
        )

        graph_build_time = time.time() - start_time

        # Graph building should be reasonably fast (< 2 seconds)
        assert graph_build_time < 2.0

        # Now compute (this is where the actual work happens)
        compute_start = time.time()
        result = ml_data.compute()
        compute_time = time.time() - compute_start

        assert not result.empty
        # Ensure both operations completed successfully
        assert graph_build_time > 0
        assert compute_time > 0


# ============================================================================
# ERROR HANDLING AND EDGE CASES (DASK-SPECIFIC)
# ============================================================================


class TestErrorHandlingDask:
    """Test cases for error handling and edge cases in Dask engine."""

    def test_process_empty_file_dask(self, client):
        """Test processing of empty CSV file with Dask."""
        payload = {
            "file_info": {
                "file_name": "tenants/demo/data/raw/empty.csv",
                "bucket_name": "test-bucket",
                "file_size": 0,
                "region": "us-west1",
            }
        }

        with patch("main.download_file_from_gcs") as mock_download, patch(
            "dask.dataframe.read_csv"
        ) as mock_read_csv, patch("os.unlink"):

            mock_download.return_value = "/tmp/empty_file.csv"
            mock_read_csv.return_value = dd.from_pandas(pd.DataFrame(), npartitions=1)

            response = client.post("/process", json=payload)

            # Should handle empty files gracefully
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["good_records"] == 0

    def test_process_malformed_csv_dask(self, client):
        """Test processing of malformed CSV file with Dask."""
        payload = {
            "file_info": {
                "file_name": "tenants/demo/data/raw/malformed.csv",
                "bucket_name": "test-bucket",
                "file_size": 1000,
                "region": "us-west1",
            }
        }

        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data", side_effect=Exception("CSV parsing error")
        ), patch("main.move_file_to_bad_records_dask") as mock_move, patch("os.unlink"):

            mock_download.return_value = "/tmp/malformed_file.csv"

            response = client.post("/process", json=payload)

            assert response.status_code == 500
            data = json.loads(response.data)
            assert data["status"] == "error"
            mock_move.assert_called_once()

    def test_bigquery_connection_failure_dask(
        self, client, sample_request_payload, sample_ecommerce_data
    ):
        """Test handling of BigQuery connection failures with Dask."""
        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data"
        ) as mock_process, patch(
            "main.save_to_bigquery_historical_dask",
            side_effect=Exception("BigQuery connection failed"),
        ), patch(
            "main.move_file_to_bad_records_dask"
        ) as mock_move, patch(
            "os.unlink"
        ):

            mock_download.return_value = "/tmp/test_file.csv"
            mock_process.return_value = (sample_ecommerce_data, pd.DataFrame())

            response = client.post("/process", json=sample_request_payload)

            assert response.status_code == 500
            data = json.loads(response.data)
            assert data["status"] == "error"
            assert "BigQuery connection failed" in data["message"]

    def test_dask_computation_failure(self, sample_dask_data):
        """Test handling of Dask computation failures."""
        # Simulate a computation failure
        with patch.object(
            sample_dask_data, "compute", side_effect=Exception("Computation failed")
        ):

            # The validate_schema_dask function should handle this gracefully
            clean_data, bad_data = validate_schema_dask(sample_dask_data)

            # Should return empty DataFrames on error
            assert isinstance(clean_data, dd.DataFrame) or clean_data.empty
            assert isinstance(bad_data, pd.DataFrame)

    def test_invalid_region_handling_dask(self, client):
        """Test handling of invalid regions with Dask."""
        payload = {
            "file_info": {
                "file_name": "tenants/demo/data/raw/file.csv",
                "bucket_name": "test-bucket",
                "file_size": 1000,
                "region": "invalid-region-123",
            }
        }

        with patch("main.download_file_from_gcs") as mock_download, patch(
            "main.process_ecommerce_data"
        ) as mock_process, patch(
            "main.save_to_bigquery_historical_dask"
        ) as mock_save_bq, patch(
            "os.unlink"
        ):

            mock_download.return_value = "/tmp/test_file.csv"
            mock_process.return_value = (pd.DataFrame({"test": [1]}), pd.DataFrame())
            mock_save_bq.return_value = 1

            response = client.post("/process", json=payload)

            # Should handle gracefully with default region mapping
            assert response.status_code == 200
            data = json.loads(response.data)
            assert data["region"] == "invalid-region-123"  # Should pass through

    @pytest.mark.parametrize(
        "invalid_data_type", [None, 123, [], "string_instead_of_dict"]
    )
    def test_invalid_request_data_types_dask(self, client, invalid_data_type):
        """Test handling of various invalid request data types with Dask."""
        response = client.post("/process", json=invalid_data_type)

        assert response.status_code == 500
        data = json.loads(response.data)
        assert data["status"] == "error"


# ============================================================================
# SECURITY TESTS (DASK-SPECIFIC)
# ============================================================================


class TestSecurityDask:
    """Security-related test cases for Dask engine."""

    def test_tenant_path_traversal_protection_dask(self):
        """Test protection against path traversal attacks in Dask."""
        malicious_paths = [
            "../../../etc/passwd",
            "tenants/../../../secret.txt",
            "tenants/demo/../../other_tenant/data.csv",
            "tenants/demo/../shared/data.csv",
        ]

        for path in malicious_paths:
            tenant = extract_tenant_from_path(path)
            assert tenant is not None  # Should return something, not crash

    def test_sql_injection_protection_in_metadata_dask(self, sample_dask_data):
        """Test SQL injection protection in metadata fields with Dask."""
        malicious_tenant = "demo'; DROP TABLE tenants; --"
        malicious_filename = "file'; DELETE FROM data; --.csv"

        result = create_ml_features_dask(
            sample_dask_data, malicious_tenant, malicious_filename, "us-west1"
        )

        computed_result = result.compute()

        # Should handle malicious input without breaking
        assert not computed_result.empty
        assert computed_result["tenant_id"].iloc[0] == malicious_tenant

    def test_data_size_limits_dask(self, client):
        """Test handling of oversized data with Dask."""
        oversized_payload = {
            "file_info": {
                "file_name": "tenants/demo/data/raw/huge_file.csv",
                "bucket_name": "test-bucket",
                "file_size": 10 * 1024 * 1024 * 1024,  # 10GB
                "region": "us-west1",
            }
        }

        with patch(
            "main.download_file_from_gcs", side_effect=MemoryError("File too large")
        ):
            response = client.post("/process", json=oversized_payload)

            assert response.status_code == 500
            data = json.loads(response.data)
            assert data["status"] == "error"


# ============================================================================
# DASK-SPECIFIC ADVANCED FEATURES
# ============================================================================


class TestDaskAdvancedFeatures:
    """Test advanced Dask-specific features."""

    def test_dask_partition_optimization(self, large_ecommerce_data):
        """Test Dask partition optimization strategies."""
        # Test repartitioning for better performance
        original_partitions = large_ecommerce_data.npartitions

        # Repartition to optimal size (around 100MB per partition)
        optimized_data = large_ecommerce_data.repartition(partition_size="50MB")

        # Partitions should be optimized
        assert optimized_data.npartitions != original_partitions
        assert len(optimized_data.compute()) == len(large_ecommerce_data.compute())

    def test_dask_persist_strategy(self, large_ecommerce_data):
        """Test Dask persist strategy for repeated operations."""
        # Validate schema
        clean_data, _ = validate_schema_dask(large_ecommerce_data)

        # Persist the result for repeated use
        persisted_data = clean_data.persist()

        # Multiple operations on persisted data should be efficient
        result1 = persisted_data.groupby("event_type").size().compute()
        result2 = persisted_data.groupby("user_id").size().compute()

        assert len(result1) > 0
        assert len(result2) > 0

    def test_dask_custom_map_partitions(self, sample_dask_data):
        """Test custom map_partitions operations."""

        def custom_partition_processor(partition):
            """Custom processing function for each partition."""
            partition["custom_feature"] = partition["price"] * partition["user_id"]
            return partition

        result = sample_dask_data.map_partitions(custom_partition_processor)
        computed_result = result.compute()

        assert "custom_feature" in computed_result.columns
        assert not computed_result.empty

    def test_dask_delayed_operations(self, sample_ecommerce_data):
        """Test Dask delayed operations for complex workflows."""
        import dask

        @dask.delayed
        def process_partition(data_chunk):
            # Simulate complex processing
            return data_chunk.groupby("event_type").size()

        # Split data into chunks
        chunks = [
            sample_ecommerce_data[i : i + 2]
            for i in range(0, len(sample_ecommerce_data), 2)
        ]

        # Process chunks in parallel using delayed
        delayed_results = [process_partition(chunk) for chunk in chunks]

        # Compute all results
        results = dask.compute(*delayed_results)

        assert len(results) > 0
        assert all(isinstance(result, pd.Series) for result in results)


if __name__ == "__main__":
    # Run with coverage reporting
    pytest.main(
        [
            __file__,
            "-v",
            "--tb=short",
            "--cov=main",
            "--cov-report=html",
            "--cov-report=term-missing",
            "--durations=10",
        ]
    )
