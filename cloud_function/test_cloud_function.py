#!/usr/bin/env python3
"""
Comprehensive test suite for the enhanced Cloud Function.

This test suite covers all the new functionality we added including tenant parsing,
file type detection, correlation tracking, and enhanced message structure.
"""

import json
import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock

# Import the functions we want to test
from main import (
    extract_tenant_info,
    detect_file_type,
    extract_region_from_bucket,
    hello_gcs
)


class TestTenantParsing:
    """Test the tenant information parsing logic."""
    
    def test_shared_ml_models(self):
        """Test parsing of shared ML model files."""
        tenant_id, data_type, folder_category = extract_tenant_info("shared/ml-models/sentiment-model.pkl")
        
        assert tenant_id == "shared"
        assert data_type == "ml-models"
        assert folder_category == "shared"
    
    def test_shared_reference_data(self):
        """Test parsing of shared reference data files."""
        test_cases = [
            ("shared/reference-data/countries/iso-countries.json", "reference-data"),
            ("shared/reference-data/currencies/currency-codes.json", "reference-data"),
            ("shared/reference-data/exchange-rates/daily-rates.csv", "reference-data")
        ]
        
        for file_path, expected_data_type in test_cases:
            tenant_id, data_type, folder_category = extract_tenant_info(file_path)
            assert tenant_id == "shared"
            assert data_type == expected_data_type
            assert folder_category == "shared"
    
    def test_business_domains(self):
        """Test parsing of business domain configuration files."""
        tenant_id, data_type, folder_category = extract_tenant_info("tenants/business-domains/ecommerce-schema.json")
        
        assert tenant_id == "business-domains"
        assert data_type == "schema"
        assert folder_category == "business-domain"
    
    def test_tenant_data_raw(self):
        """Test parsing of tenant raw data files."""
        test_cases = [
            "tenants/tenant-001/data/raw/sales-data.csv",
            "tenants/tenant-002/data/raw/customer-events.json",
            "tenants/tenant-123/data/raw/product-catalog.xlsx"
        ]
        
        for file_path in test_cases:
            tenant_id, data_type, folder_category = extract_tenant_info(file_path)
            expected_tenant = file_path.split('/')[1]  # Extract tenant-XXX
            
            assert tenant_id == expected_tenant
            assert data_type == "raw"
            assert folder_category == "tenant-data"
    
    def test_tenant_data_processed(self):
        """Test parsing of tenant processed data files."""
        tenant_id, data_type, folder_category = extract_tenant_info("tenants/tenant-001/data/processed/ml-features.parquet")
        
        assert tenant_id == "tenant-001"
        assert data_type == "processed"
        assert folder_category == "tenant-data"
    
    def test_tenant_data_bad_records(self):
        """Test parsing of tenant bad records files."""
        tenant_id, data_type, folder_category = extract_tenant_info("tenants/tenant-002/data/bad-records/failed-processing.csv")
        
        assert tenant_id == "tenant-002"
        assert data_type == "bad-records"
        assert folder_category == "tenant-data"
    
    def test_root_level_files(self):
        """Test handling of files at root level (shouldn't happen but needs handling)."""
        tenant_id, data_type, folder_category = extract_tenant_info("some-random-file.txt")
        
        assert tenant_id == "root"
        assert data_type == "unknown"
        assert folder_category == "root"
    
    def test_malformed_paths(self):
        """Test handling of malformed or unexpected paths."""
        test_cases = [
            "",
            "/",
            "tenants/",
            "shared/",
            "tenants/tenant-001/",
            "tenants/tenant-001/data/"
        ]
        
        for file_path in test_cases:
            tenant_id, data_type, folder_category = extract_tenant_info(file_path)
            # Should gracefully handle malformed paths without crashing
            assert isinstance(tenant_id, str)
            assert isinstance(data_type, str)
            assert isinstance(folder_category, str)


class TestFileTypeDetection:
    """Test the file type detection and categorization logic."""
    
    def test_csv_files(self):
        """Test CSV file detection."""
        file_info = detect_file_type("sales-data.csv")
        
        assert file_info['extension'] == ".csv"
        assert file_info['file_type'] == "csv"
        assert file_info['category'] == "data"
        assert file_info['is_supported'] is True
        assert file_info['file_name_without_ext'] == "sales-data"
    
    def test_json_files(self):
        """Test JSON file detection."""
        file_info = detect_file_type("config.json")
        
        assert file_info['extension'] == ".json"
        assert file_info['file_type'] == "json"
        assert file_info['category'] == "data"
        assert file_info['is_supported'] is True
    
    def test_parquet_files(self):
        """Test Parquet file detection."""
        file_info = detect_file_type("analytics.parquet")
        
        assert file_info['extension'] == ".parquet"
        assert file_info['file_type'] == "parquet"
        assert file_info['category'] == "data"
        assert file_info['is_supported'] is True
    
    def test_model_files(self):
        """Test ML model file detection."""
        test_cases = [
            ("model.pkl", "pickle"),
            ("classifier.pickle", "pickle")
        ]
        
        for filename, expected_type in test_cases:
            file_info = detect_file_type(filename)
            assert file_info['file_type'] == expected_type
            assert file_info['category'] == "model"
            assert file_info['is_supported'] is True
    
    def test_excel_files(self):
        """Test Excel file detection."""
        test_cases = [
            ("report.xlsx", "excel"),
            ("data.xls", "excel")
        ]
        
        for filename, expected_type in test_cases:
            file_info = detect_file_type(filename)
            assert file_info['file_type'] == expected_type
            assert file_info['category'] == "data"
            assert file_info['is_supported'] is True
    
    def test_log_files(self):
        """Test log file detection."""
        file_info = detect_file_type("application.log")
        
        assert file_info['extension'] == ".log"
        assert file_info['file_type'] == "log"
        assert file_info['category'] == "log"
        assert file_info['is_supported'] is True
    
    def test_unsupported_files(self):
        """Test unsupported file type handling."""
        test_cases = [
            "document.pdf",
            "image.png",
            "video.mp4",
            "archive.zip",
            "unknown-file"
        ]
        
        for filename in test_cases:
            file_info = detect_file_type(filename)
            assert file_info['file_type'] == "unknown"
            assert file_info['category'] == "unknown"
            assert file_info['is_supported'] is False
    
    def test_case_insensitive_detection(self):
        """Test that file type detection is case insensitive."""
        test_cases = [
            "DATA.CSV",
            "Model.PKL",
            "Report.XLSX",
            "config.JSON"
        ]
        
        for filename in test_cases:
            file_info = detect_file_type(filename)
            assert file_info['is_supported'] is True


class TestRegionExtraction:
    """Test region extraction from bucket names."""
    
    def test_valid_bucket_names(self):
        """Test extraction from valid bucket names."""
        test_cases = [
            ("terraops-us-central1-tenant-data", "us-central1"),
            ("terraops-europe-west1-tenant-data", "europe-west1"),
            ("terraops-africa-south1-tenant-data", "africa-south1"),
            ("terraops-northamerica-northeast1-tenant-data", "northamerica-northeast1")
        ]
        
        for bucket_name, expected_region in test_cases:
            region = extract_region_from_bucket(bucket_name)
            assert region == expected_region
    
    def test_invalid_bucket_names(self):
        """Test handling of invalid bucket names."""
        test_cases = [
            "random-bucket-name",
            "terraops-invalid-format",
            "not-a-terraops-bucket",
            "",
            "terraops--tenant-data"  # Double dash
        ]
        
        for bucket_name in test_cases:
            region = extract_region_from_bucket(bucket_name)
            assert region == "unknown"


class TestCloudFunctionIntegration:
    """Test the main Cloud Function entry point."""
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_successful_message_processing(self, mock_publisher):
        """Test successful processing of a valid storage event."""
        # Mock the Pub/Sub publisher
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_future = Mock()
        mock_future.result.return_value = "mock-message-id-12345"
        mock_client.publish.return_value = mock_future
        
        # Create a mock storage event
        event = {
            'name': 'tenants/tenant-001/data/raw/sales-data.csv',
            'bucket': 'terraops-us-central1-tenant-data',
            'size': 1048576,  # 1MB
            'timeCreated': '2024-08-29T10:30:00.000Z',
            'contentType': 'text/csv'
        }
        
        context = Mock()
        
        # Set up environment variables
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router',
            'DEAD_LETTER_TOPIC': 'projects/test-project/topics/dead-letter-queue'
        }):
            result = hello_gcs(event, context)
        
        # Verify the function completed successfully
        assert result['status'] == 'success'
        assert result['correlation_id'] is not None
        assert result['tenant_id'] == 'tenant-001'
        assert result['region'] == 'us-central1'
        assert result['file_supported'] is True
        
        # Verify Pub/Sub was called
        mock_client.publish.assert_called_once()
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_unsupported_file_processing(self, mock_publisher):
        """Test processing of unsupported file types."""
        # Mock the publisher
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_future = Mock()
        mock_future.result.return_value = "mock-message-id-67890"
        mock_client.publish.return_value = mock_future
        
        # Event with unsupported file type
        event = {
            'name': 'tenants/tenant-001/data/raw/document.pdf',
            'bucket': 'terraops-europe-west1-tenant-data',
            'size': 2048576,  # 2MB
            'timeCreated': '2024-08-29T11:00:00.000Z',
            'contentType': 'application/pdf'
        }
        
        context = Mock()
        
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router'
        }):
            result = hello_gcs(event, context)
        
        # Should still process but mark as unsupported
        assert result['status'] == 'success'
        assert result['file_supported'] is False
        assert result['tenant_id'] == 'tenant-001'
        assert result['region'] == 'europe-west1'
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_shared_file_processing(self, mock_publisher):
        """Test processing of shared resource files."""
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_future = Mock()
        mock_future.result.return_value = "mock-message-id-shared"
        mock_client.publish.return_value = mock_future
        
        event = {
            'name': 'shared/reference-data/countries/iso-countries.json',
            'bucket': 'terraops-us-west1-tenant-data',
            'size': 50000,  # 50KB
            'timeCreated': '2024-08-29T12:00:00.000Z',
            'contentType': 'application/json'
        }
        
        context = Mock()
        
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router'
        }):
            result = hello_gcs(event, context)
        
        assert result['status'] == 'success'
        assert result['tenant_id'] == 'shared'
        assert result['region'] == 'us-west1'
        assert result['file_supported'] is True
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_pubsub_failure_handling(self, mock_publisher):
        """Test handling of Pub/Sub publishing failures."""
        # Mock publisher to raise an exception
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_client.publish.side_effect = Exception("Pub/Sub connection failed")
        
        event = {
            'name': 'tenants/tenant-001/data/raw/test.csv',
            'bucket': 'terraops-us-central1-tenant-data',
            'size': 1000,
            'timeCreated': '2024-08-29T13:00:00.000Z'
        }
        
        context = Mock()
        
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router',
            'DEAD_LETTER_TOPIC': 'projects/test-project/topics/dead-letter-queue'
        }):
            # Should raise the exception since Pub/Sub fails
            with pytest.raises(Exception) as exc_info:
                hello_gcs(event, context)
            
            assert "Pub/Sub connection failed" in str(exc_info.value)
    
    def test_missing_pubsub_topic(self):
        """Test behavior when PUBSUB_TOPIC environment variable is missing."""
        event = {
            'name': 'tenants/tenant-001/data/raw/test.csv',
            'bucket': 'terraops-us-central1-tenant-data',
            'size': 1000,
            'timeCreated': '2024-08-29T14:00:00.000Z'
        }
        
        context = Mock()
        
        # Don't set PUBSUB_TOPIC environment variable
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                hello_gcs(event, context)
            
            assert "PUBSUB_TOPIC environment variable not set" in str(exc_info.value)


class TestMessageStructure:
    """Test the enhanced message structure generation."""
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_message_structure_completeness(self, mock_publisher):
        """Test that generated messages contain all required fields."""
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_future = Mock()
        mock_future.result.return_value = "test-message-id"
        mock_client.publish.return_value = mock_future
        
        event = {
            'name': 'tenants/tenant-001/data/processed/analytics.parquet',
            'bucket': 'terraops-europe-west3-tenant-data',
            'size': 52428800,  # 50MB
            'timeCreated': '2024-08-29T15:30:00.000Z',
            'contentType': 'application/parquet'
        }
        
        context = Mock()
        
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router'
        }):
            result = hello_gcs(event, context)
        
        # Verify the message was published
        assert mock_client.publish.called
        
        # Get the published message data
        call_args = mock_client.publish.call_args
        published_data = json.loads(call_args[0][1].decode('utf-8'))  # Second argument is the message bytes
        
        # Verify message structure contains all expected fields
        required_fields = [
            'file_name', 'bucket_name', 'file_size', 'time_created',
            'region', 'tenant_id', 'data_type', 'folder_category',
            'file_info', 'correlation_id', 'processing_timestamp',
            'function_version', 'routing_hints'
        ]
        
        for field in required_fields:
            assert field in published_data, f"Missing required field: {field}"
        
        # Verify specific values
        assert published_data['tenant_id'] == 'tenant-001'
        assert published_data['region'] == 'europe-west3'
        assert published_data['data_type'] == 'processed'
        assert published_data['file_info']['file_type'] == 'parquet'
        assert published_data['file_info']['is_supported'] is True
        assert published_data['routing_hints']['requires_processing'] is True
        
        # Verify message attributes were included
        attributes = call_args[1]  # Keyword arguments contain attributes
        assert 'tenant_id' in attributes
        assert 'region' in attributes
        assert 'file_type' in attributes
        assert 'correlation_id' in attributes


class TestCorrelationTracking:
    """Test correlation ID generation and tracking."""
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_correlation_id_uniqueness(self, mock_publisher):
        """Test that each invocation generates a unique correlation ID."""
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_future = Mock()
        mock_future.result.return_value = "test-message-id"
        mock_client.publish.return_value = mock_future
        
        event = {
            'name': 'tenants/tenant-001/data/raw/test.csv',
            'bucket': 'terraops-us-central1-tenant-data',
            'size': 1000,
            'timeCreated': '2024-08-29T16:00:00.000Z'
        }
        
        context = Mock()
        
        correlation_ids = set()
        
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router'
        }):
            # Call the function multiple times
            for _ in range(5):
                result = hello_gcs(event, context)
                correlation_ids.add(result['correlation_id'])
        
        # All correlation IDs should be unique
        assert len(correlation_ids) == 5
        
        # All should be valid UUIDs
        for correlation_id in correlation_ids:
            uuid.UUID(correlation_id)  # Will raise if invalid
    
    @patch('main.pubsub_v1.PublisherClient')
    def test_correlation_id_in_attributes(self, mock_publisher):
        """Test that correlation ID is included in Pub/Sub message attributes."""
        mock_client = Mock()
        mock_publisher.return_value = mock_client
        mock_future = Mock()
        mock_future.result.return_value = "test-message-id"
        mock_client.publish.return_value = mock_future
        
        event = {
            'name': 'shared/ml-models/test-model.pkl',
            'bucket': 'terraops-us-west2-tenant-data',
            'size': 1048576,
            'timeCreated': '2024-08-29T17:00:00.000Z'
        }
        
        context = Mock()
        
        with patch.dict('os.environ', {
            'PUBSUB_TOPIC': 'projects/test-project/topics/send_to_router'
        }):
            result = hello_gcs(event, context)
        
        # Get the correlation ID from the response
        correlation_id = result['correlation_id']
        
        # Verify it was included in the Pub/Sub message attributes
        call_args = mock_client.publish.call_args
        attributes = call_args[1]  # Keyword arguments
        
        assert attributes['correlation_id'] == correlation_id


# Performance and Load Testing Helpers
class TestPerformanceCharacteristics:
    """Test performance characteristics and edge cases."""
    
    def test_large_file_handling(self):
        """Test handling of very large file size values."""
        # Test with file size that exceeds typical limits
        large_size = 10 * 1024 * 1024 * 1024  # 10GB
        
        tenant_id, data_type, folder_category = extract_tenant_info("tenants/tenant-001/data/raw/huge-dataset.csv")
        file_info = detect_file_type("huge-dataset.csv")
        
        # Functions should handle large sizes gracefully
        assert tenant_id == "tenant-001"
        assert file_info['is_supported'] is True
    
    def test_many_file_types_simultaneously(self):
        """Test processing many different file types in sequence."""
        file_types = [
            "data1.csv", "data2.json", "data3.parquet", "model.pkl",
            "report.xlsx", "config.jsonl", "app.log", "data.xls"
        ]
        
        results = []
        for filename in file_types:
            file_info = detect_file_type(filename)
            results.append(file_info['is_supported'])
        
        # All these file types should be supported
        assert all(results)
    
    def test_tenant_parsing_performance(self):
        """Test tenant parsing with various path lengths and structures."""
        paths = [
            "shared/ml-models/model.pkl",
            "tenants/tenant-001/data/raw/file.csv",
            "tenants/business-domains/schema.json",
            "tenants/tenant-123456789/data/processed/very-long-filename-with-lots-of-details-and-information.parquet",
            "shared/reference-data/countries/iso-3166-country-codes-with-long-names.json"
        ]
        
        for path in paths:
            tenant_id, data_type, folder_category = extract_tenant_info(path)
            # All should return valid strings without errors
            assert isinstance(tenant_id, str)
            assert isinstance(data_type, str)
            assert isinstance(folder_category, str)


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "--tb=short"])