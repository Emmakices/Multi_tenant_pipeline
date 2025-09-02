#!/usr/bin/env python3
"""
Comprehensive pytest unit tests for the router function.

This test suite covers:
- Engine health testing functionality
- Authentication token generation
- HTTP request handling and error scenarios
- Main function orchestration
- Edge cases and error handling
"""

import json
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import google.auth.transport.requests
import google.oauth2.id_token
import pytest
import requests

# Import the functions to test
from main import GLOBAL_ENGINES, check_engine_health, get_auth_token, main

# ============================================================================
# PYTEST FIXTURES
# ============================================================================


@pytest.fixture
def mock_engine_urls():
    """Fixture providing mock engine URLs for testing."""
    return {
        "pandas": "https://pandas-test.run.app",
        "polars": "https://polars-test.run.app",
        "dask": "https://dask-test.run.app",
    }


@pytest.fixture
def healthy_engine_response():
    """Fixture providing a healthy engine response."""
    return {
        "status": "healthy",
        "engine": "pandas",
        "version": "1.0",
        "supported_formats": ["csv", "json"],
        "last_health_check": "2024-01-01T12:00:00Z",
    }


@pytest.fixture
def unhealthy_engine_response():
    """Fixture providing an unhealthy engine response."""
    return {
        "status": "error",
        "message": "Database connection failed",
        "engine": "pandas",
    }


@pytest.fixture
def mock_auth_token():
    """Fixture providing a mock authentication token."""
    return "mock-jwt-token-12345"


@pytest.fixture
def mock_requests_response():
    """Fixture providing a mock requests response object."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "healthy", "engine": "test_engine"}
    mock_response.text = '{"status": "healthy"}'
    return mock_response


@pytest.fixture
def sample_file_metadata():
    """Fixture providing sample file metadata for processing requests."""
    return {
        "file_name": "tenants/demo/data/raw/ecommerce-events-2024.csv",
        "bucket_name": "terraops-us-west1-tenant-data",
        "file_size": 1024000,  # 1MB
        "region": "us-west1",
        "tenant_id": "demo",
        "content_type": "text/csv",
    }


@pytest.fixture
def large_file_metadata():
    """Fixture providing large file metadata for testing engine selection."""
    return {
        "file_name": "tenants/enterprise/data/raw/large-dataset.csv",
        "bucket_name": "terraops-us-west1-tenant-data",
        "file_size": 100000000,  # 100MB - should route to Dask
        "region": "us-west1",
        "tenant_id": "enterprise",
        "content_type": "text/csv",
    }


# ============================================================================
# AUTHENTICATION TOKEN TESTS
# ============================================================================


class TestAuthToken:
    """Test cases for authentication token generation."""

    def test_get_auth_token_success(self, mock_auth_token):
        """Test successful authentication token retrieval."""
        url = "https://test-engine.run.app"

        with patch("google.auth.transport.requests.Request") as mock_request, patch(
            "google.oauth2.id_token.fetch_id_token", return_value=mock_auth_token
        ) as mock_fetch:

            result = get_auth_token(url)

            # Verify the token was returned
            assert result == mock_auth_token

            # Verify the correct functions were called
            mock_request.assert_called_once()
            mock_fetch.assert_called_once_with(mock_request.return_value, url)

    def test_get_auth_token_failure(self):
        """Test authentication token retrieval failure."""
        url = "https://invalid-engine.run.app"

        with patch("google.auth.transport.requests.Request") as mock_request, patch(
            "google.oauth2.id_token.fetch_id_token",
            side_effect=Exception("Auth failed"),
        ) as mock_fetch, patch("builtins.print") as mock_print:

            result = get_auth_token(url)

            # Verify None is returned on failure
            assert result is None

            # Verify error message was printed
            mock_print.assert_called_with("Failed to get auth token: Auth failed")

    @pytest.mark.parametrize(
        "exception_type,exception_msg",
        [
            (google.auth.exceptions.RefreshError, "Token refresh failed"),
            (ValueError, "Invalid token format"),
            (TimeoutError, "Token request timeout"),
            (Exception, "Generic auth error"),
        ],
    )
    def test_get_auth_token_various_exceptions(self, exception_type, exception_msg):
        """Test authentication token retrieval with various exception types."""
        url = "https://test-engine.run.app"

        with patch("google.auth.transport.requests.Request"), patch(
            "google.oauth2.id_token.fetch_id_token",
            side_effect=exception_type(exception_msg),
        ), patch("builtins.print") as mock_print:

            result = get_auth_token(url)

            assert result is None
            mock_print.assert_called_with(f"Failed to get auth token: {exception_msg}")


# ============================================================================
# ENGINE HEALTH TESTING
# ============================================================================


class TestEngineHealth:
    """Test cases for individual engine health testing."""

    def test_engine_health_success_with_auth(
        self, healthy_engine_response, mock_auth_token
    ):
        """Test successful engine health check with authentication."""
        engine_name = "pandas"
        engine_url = "https://pandas-test.run.app"

        with patch("main.get_auth_token", return_value=mock_auth_token), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print") as mock_print:

            # Mock successful HTTP response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = healthy_engine_response
            mock_get.return_value = mock_response

            result = check_engine_health(engine_name, engine_url)

            # Verify the result structure
            assert result["status"] == "healthy"
            assert result["details"] == healthy_engine_response

            # Verify correct API call was made with auth headers
            expected_headers = {"Authorization": f"Bearer {mock_auth_token}"}
            mock_get.assert_called_once_with(
                f"{engine_url}/health", headers=expected_headers, timeout=60
            )

            # Verify success message was printed
            assert any(
                "PASS pandas:" in str(call) for call in mock_print.call_args_list
            )

    def test_engine_health_success_without_auth(self, healthy_engine_response):
        """Test successful engine health check without authentication."""
        engine_name = "polars"
        engine_url = "https://polars-test.run.app"

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print"):

            # Mock successful HTTP response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = healthy_engine_response
            mock_get.return_value = mock_response

            result = check_engine_health(engine_name, engine_url)

            # Verify the result
            assert result["status"] == "healthy"
            assert result["details"] == healthy_engine_response

            # Verify API call was made without auth headers
            mock_get.assert_called_once_with(f"{engine_url}/health", timeout=60)

    @pytest.mark.parametrize(
        "status_code,expected_status",
        [
            (400, "unhealthy"),
            (401, "unhealthy"),
            (403, "unhealthy"),
            (404, "unhealthy"),
            (500, "unhealthy"),
            (502, "unhealthy"),
            (503, "unhealthy"),
        ],
    )
    def test_engine_health_http_error_codes(self, status_code, expected_status):
        """Test engine health check with various HTTP error codes."""
        engine_name = "dask"
        engine_url = "https://dask-test.run.app"
        error_text = f"HTTP {status_code} Error"

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print") as mock_print:

            # Mock HTTP error response
            mock_response = Mock()
            mock_response.status_code = status_code
            mock_response.text = error_text
            mock_get.return_value = mock_response

            result = check_engine_health(engine_name, engine_url)

            # Verify error result structure
            assert result["status"] == expected_status
            assert result["error"] == f"HTTP {status_code}"
            assert result["details"] == error_text

            # Verify error message was printed
            assert any(
                f"FAIL {engine_name}: HTTP {status_code}" in str(call)
                for call in mock_print.call_args_list
            )

    def test_engine_health_timeout_error(self):
        """Test engine health check with timeout error."""
        engine_name = "pandas"
        engine_url = "https://slow-engine.run.app"

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get", side_effect=requests.exceptions.Timeout
        ), patch("builtins.print") as mock_print:

            result = check_engine_health(engine_name, engine_url)

            # Verify timeout result
            assert result["status"] == "timeout"
            assert result["error"] == "Request timeout"

            # Verify timeout message was printed
            assert any(
                "TIMEOUT pandas: Timeout" in str(call)
                for call in mock_print.call_args_list
            )

    def test_engine_health_connection_error(self):
        """Test engine health check with connection error."""
        engine_name = "polars"
        engine_url = "https://unreachable-engine.run.app"
        connection_error = "Connection refused"

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get",
            side_effect=requests.exceptions.ConnectionError(connection_error),
        ), patch("builtins.print") as mock_print:

            result = check_engine_health(engine_name, engine_url)

            # Verify connection error result
            assert result["status"] == "connection_error"
            assert connection_error in result["error"]

            # Verify connection error message was printed
            assert any(
                "CONNECTION_ERROR polars: Connection Error" in str(call)
                for call in mock_print.call_args_list
            )

    def test_engine_health_unexpected_error(self):
        """Test engine health check with unexpected error."""
        engine_name = "dask"
        engine_url = "https://broken-engine.run.app"
        unexpected_error = "Unexpected JSON decode error"

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get", side_effect=Exception(unexpected_error)
        ), patch("builtins.print") as mock_print:

            result = check_engine_health(engine_name, engine_url)

            # Verify unexpected error result
            assert result["status"] == "error"
            assert result["error"] == unexpected_error

            # Verify error message was printed
            assert any(
                "ERROR dask: Unexpected Error" in str(call)
                for call in mock_print.call_args_list
            )

    def test_engine_health_json_decode_error(self):
        """Test engine health check when response JSON is malformed."""
        engine_name = "pandas"
        engine_url = "https://pandas-test.run.app"

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print"):

            # Mock response with invalid JSON
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
            mock_get.return_value = mock_response

            result = check_engine_health(engine_name, engine_url)

            # Should handle JSON decode error as unexpected error
            assert result["status"] == "error"
            assert "Invalid JSON" in result["error"]


# ============================================================================
# MAIN FUNCTION TESTS
# ============================================================================


class TestMainFunction:
    """Test cases for the main orchestration function."""

    def test_main_all_engines_healthy(self, healthy_engine_response):
        """Test main function when all engines are healthy."""
        with patch("main.check_engine_health") as mock_health_test, patch(
            "builtins.print"
        ) as mock_print:

            # Mock all engines as healthy
            mock_health_test.return_value = {
                "status": "healthy",
                "details": healthy_engine_response,
            }

            result = main()

            # Verify all engines were tested
            assert mock_health_test.call_count == len(GLOBAL_ENGINES)

            # Verify result structure
            assert len(result) == len(GLOBAL_ENGINES)
            for engine_name in GLOBAL_ENGINES.keys():
                assert engine_name in result
                assert result[engine_name]["status"] == "healthy"

            # Verify success messages were printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any("All engines are healthy!" in call for call in print_calls)
            assert any("Healthy engines: 3/3" in call for call in print_calls)

    def test_main_some_engines_unhealthy(self):
        """Test main function when some engines are unhealthy."""

        def mock_health_response(name, url):
            if "pandas" in url:
                return {"status": "healthy", "details": {"engine": "pandas"}}
            elif "polars" in url:
                return {"status": "unhealthy", "error": "HTTP 503"}
            else:  # dask
                return {"status": "timeout", "error": "Request timeout"}

        with patch("main.check_engine_health", side_effect=mock_health_response), patch(
            "builtins.print"
        ) as mock_print:

            result = main()

            # Verify mixed results
            assert result["pandas"]["status"] == "healthy"
            assert result["polars"]["status"] == "unhealthy"
            assert result["dask"]["status"] == "timeout"

            # Verify warning message was printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any("Some engines have issues" in call for call in print_calls)
            assert any("Healthy engines: 1/3" in call for call in print_calls)

    def test_main_all_engines_failed(self):
        """Test main function when all engines are failed."""
        with patch("main.check_engine_health") as mock_health_test, patch(
            "builtins.print"
        ) as mock_print:

            # Mock all engines as failed
            mock_health_test.return_value = {
                "status": "error",
                "error": "Connection failed",
            }

            result = main()

            # Verify all engines failed
            for engine_result in result.values():
                assert engine_result["status"] == "error"

            # Verify failure message was printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any("All engines are unavailable" in call for call in print_calls)
            assert any("Healthy engines: 0/3" in call for call in print_calls)

    def test_main_json_output_format(self, healthy_engine_response):
        """Test that main function outputs properly formatted JSON."""
        with patch("main.check_engine_health") as mock_health_test, patch(
            "builtins.print"
        ) as mock_print, patch("json.dumps") as mock_json_dumps:

            mock_health_test.return_value = {
                "status": "healthy",
                "details": healthy_engine_response,
            }
            mock_json_dumps.return_value = '{"test": "json"}'

            result = main()

            # Verify JSON output was formatted with indentation
            mock_json_dumps.assert_called_once_with(result, indent=2)

            # Verify JSON was printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any('"test": "json"' in call for call in print_calls)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


class TestIntegration:
    """Integration test cases for complete workflow."""

    def test_complete_workflow_success(self, mock_auth_token, healthy_engine_response):
        """Test complete workflow from auth to health check to main function."""
        with patch("google.auth.transport.requests.Request") as mock_request, patch(
            "google.oauth2.id_token.fetch_id_token", return_value=mock_auth_token
        ), patch("requests.get") as mock_get, patch("builtins.print") as mock_print:

            # Mock successful HTTP responses for all engines
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = healthy_engine_response
            mock_get.return_value = mock_response

            result = main()

            # Verify authentication was attempted for each engine
            assert mock_request.call_count == len(GLOBAL_ENGINES)

            # Verify HTTP requests were made with auth headers
            expected_calls = len(GLOBAL_ENGINES)
            assert mock_get.call_count == expected_calls

            # Verify all engines reported as healthy
            for engine_name in GLOBAL_ENGINES.keys():
                assert result[engine_name]["status"] == "healthy"

            # Verify success summary was printed
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any("All engines are healthy!" in call for call in print_calls)

    def test_mixed_auth_scenarios(self, mock_auth_token, healthy_engine_response):
        """Test workflow with mixed authentication success/failure scenarios."""

        def mock_auth_response(url):
            # Simulate auth failure for polars engine only
            if "polars" in url:
                return None
            return mock_auth_token

        with patch("main.get_auth_token", side_effect=mock_auth_response), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print"):

            # Mock successful HTTP responses
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = healthy_engine_response
            mock_get.return_value = mock_response

            result = main()

            # Verify mixed auth scenarios were handled
            # All engines should still report healthy despite auth differences
            for engine_name in GLOBAL_ENGINES.keys():
                assert result[engine_name]["status"] == "healthy"


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================


class TestEdgeCases:
    """Test cases for edge cases and boundary conditions."""

    def test_empty_engines_dict(self):
        """Test behavior when GLOBAL_ENGINES dict is empty."""
        with patch("main.GLOBAL_ENGINES", {}), patch("builtins.print") as mock_print:

            result = main()

            # Should handle empty engines gracefully
            assert result == {}

            # Should show 0/0 healthy engines
            print_calls = [str(call) for call in mock_print.call_args_list]
            assert any("Healthy engines: 0/0" in call for call in print_calls)

    def test_malformed_engine_urls(self):
        """Test behavior with malformed engine URLs."""
        bad_engines = {
            "bad_engine": "not-a-valid-url",
            "empty_engine": "",
            "none_engine": None,
        }

        with patch("main.GLOBAL_ENGINES", bad_engines), patch(
            "main.get_auth_token", return_value=None
        ), patch("requests.get", side_effect=requests.exceptions.InvalidURL), patch(
            "builtins.print"
        ):

            result = main()

            # Should handle malformed URLs as errors
            for engine_result in result.values():
                assert engine_result["status"] == "error"

    def test_very_large_response_data(self):
        """Test handling of very large response data."""
        large_response = {
            "status": "healthy",
            "engine": "test",
            "large_data": "x" * 10000,  # 10KB of data
            "metrics": {f"metric_{i}": i for i in range(1000)},  # Large nested dict
        }

        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print") as mock_print:

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = large_response
            mock_get.return_value = mock_response

            result = check_engine_health("test_engine", "https://test.com")

            # Should handle large response data
            assert result["status"] == "healthy"
            assert result["details"] == large_response

    @pytest.mark.parametrize("timeout_value", [1, 30, 60, 120])
    def test_different_timeout_values(self, timeout_value):
        """Test engine health check with different timeout values."""
        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print"):

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"status": "healthy"}
            mock_get.return_value = mock_response

            # Temporarily patch the timeout value in the function
            with patch("main.check_engine_health") as mock_health:

                def custom_health_check(name, url):
                    # Call requests.get with custom timeout
                    mock_get(f"{url}/health", timeout=timeout_value)
                    return {"status": "healthy", "details": {}}

                mock_health.side_effect = custom_health_check

                result = check_engine_health("test", "https://test.com")

                # Verify function completes regardless of timeout value
                assert result is not None


# ============================================================================
# PERFORMANCE AND CONCURRENCY TESTS
# ============================================================================


class TestPerformance:
    """Test cases for performance and concurrency scenarios."""

    def test_concurrent_engine_testing_simulation(self):
        """Test simulated concurrent engine testing."""
        import threading
        import time

        results = {}

        def test_single_engine(name, url):
            # Simulate some processing time
            time.sleep(0.1)
            with patch("main.get_auth_token", return_value=None), patch(
                "requests.get"
            ) as mock_get:

                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"status": "healthy", "engine": name}
                mock_get.return_value = mock_response

                result = check_engine_health(name, url)
                results[name] = result

        # Start threads for each engine
        threads = []
        for name, url in GLOBAL_ENGINES.items():
            thread = threading.Thread(target=test_single_engine, args=(name, url))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all engines were tested
        assert len(results) == len(GLOBAL_ENGINES)
        for engine_name in GLOBAL_ENGINES.keys():
            assert engine_name in results
            assert results[engine_name]["status"] == "healthy"


# ============================================================================
# PARAMETERIZED TESTS
# ============================================================================


class TestParametrized:
    """Parameterized test cases for comprehensive coverage."""

    @pytest.mark.parametrize(
        "engine_name,expected_url",
        [
            ("pandas", GLOBAL_ENGINES["pandas"]),
            ("polars", GLOBAL_ENGINES["polars"]),
            ("dask", GLOBAL_ENGINES["dask"]),
        ],
    )
    def test_each_engine_individually(
        self, engine_name, expected_url, healthy_engine_response
    ):
        """Test each engine individually with parameterized inputs."""
        with patch("main.get_auth_token", return_value="test-token"), patch(
            "requests.get"
        ) as mock_get, patch("builtins.print"):

            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = healthy_engine_response
            mock_get.return_value = mock_response

            result = check_engine_health(engine_name, expected_url)

            # Verify correct URL was called
            mock_get.assert_called_once_with(
                f"{expected_url}/health",
                headers={"Authorization": "Bearer test-token"},
                timeout=60,
            )

            # Verify healthy response
            assert result["status"] == "healthy"
            assert result["details"] == healthy_engine_response

    @pytest.mark.parametrize(
        "error_type,error_message,expected_status",
        [
            (requests.exceptions.Timeout(), "Request timeout", "timeout"),
            (
                requests.exceptions.ConnectionError("Connection failed"),
                "Connection failed",
                "connection_error",
            ),
            (requests.exceptions.HTTPError("HTTP Error"), "HTTP Error", "error"),
            (
                requests.exceptions.RequestException("Request failed"),
                "Request failed",
                "error",
            ),
            (Exception("Generic error"), "Generic error", "error"),
        ],
    )
    def test_various_request_exceptions(
        self, error_type, error_message, expected_status
    ):
        """Test various request exceptions with parameterized inputs."""
        with patch("main.get_auth_token", return_value=None), patch(
            "requests.get", side_effect=error_type
        ), patch("builtins.print"):

            result = check_engine_health("test_engine", "https://test.com")

            assert result["status"] == expected_status
            if expected_status != "timeout":
                assert error_message in result["error"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
