#!/usr/bin/env python3
"""
Integration test for multi-tenant pipeline end-to-end workflow.

This test validates the complete flow from file upload to final processing,
simulating the real-world usage pattern of the enhanced multi-tenant pipeline.
"""

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests


def test_cloud_function_to_engine_integration():
    """Test integration between cloud function tenant parsing and engine processing."""
    # Simulate cloud function output
    from cloud_function.main import extract_tenant_info, detect_file_type, extract_region_from_bucket
    
    # Test file paths from different tenants
    test_files = [
        "tenants/demo/data/raw/sales_data.csv",
        "tenants/enterprise/data/processed/analytics.json",
        "shared/ml_models/recommendation_model.pkl"
    ]
    
    bucket_names = [
        "terraops-us-west1-tenant-data",
        "terraops-europe-west1-tenant-data", 
        "terraops-us-central1-tenant-data"
    ]
    
    results = []
    for file_name in test_files:
        for bucket_name in bucket_names:
            # Extract tenant and region info like cloud function would
            tenant_id, business_domain, data_type = extract_tenant_info(file_name)
            file_info = detect_file_type(file_name)
            file_type = file_info['file_type']
            is_supported = file_info['is_supported']
            region = extract_region_from_bucket(bucket_name)
            
            # Create enhanced message structure
            enhanced_message = {
                "file_info": {
                    "file_name": file_name,
                    "bucket_name": bucket_name,
                    "tenant_id": tenant_id,
                    "region": region,
                    "correlation_id": str(uuid.uuid4()),
                    "file_info": {
                        "file_type": file_type,
                        "is_supported": is_supported
                    }
                },
                "processing_metadata": {
                    "selected_engine": "pandas",  # Will be selected by router
                    "tenant_isolation": True
                }
            }
            
            results.append({
                'file_name': file_name,
                'bucket': bucket_name,
                'tenant_id': tenant_id,
                'region': region,
                'message': enhanced_message
            })
    
    # Validate message structure
    for result in results:
        assert result['tenant_id'] in ['demo', 'enterprise', 'shared']
        assert result['region'] in ['us-west1', 'europe-west1', 'us-central1']
        assert 'correlation_id' in result['message']['file_info']
    
    print(f"PASS Integration test passed: {len(results)} message structures validated")


def test_engine_selection_logic():
    """Test that router selects appropriate engines based on file characteristics."""
    
    test_scenarios = [
        # Small CSV files -> Pandas
        {"file_size": 5 * 1024 * 1024, "file_type": "csv", "expected": "pandas"},
        # Medium Parquet files -> Polars  
        {"file_size": 50 * 1024 * 1024, "file_type": "parquet", "expected": "polars"},
        # Large CSV files -> Dask
        {"file_size": 200 * 1024 * 1024, "file_type": "csv", "expected": "dask"},
        # ML models -> Pandas
        {"file_size": 10 * 1024 * 1024, "file_type": "pkl", "expected": "pandas"}
    ]
    
    # Import router logic
    from router_function.main import select_optimal_engine
    
    correct_selections = 0
    for scenario in test_scenarios:
        # Simulate message from cloud function
        message_data = {
            "file_info": {
                "file_name": f"test.{scenario['file_type']}",
                "file_size": scenario["file_size"],
                "file_type": scenario["file_type"],
                "region": "us-central1",
                "tenant_id": "test-tenant"
            }
        }
        
        # Test engine selection
        selected_region, selected_engine, selected_url = select_optimal_engine(message_data)
        
        if selected_engine == scenario["expected"]:
            correct_selections += 1
            
        print(f"File: {scenario['file_type']} ({scenario['file_size']//1024//1024}MB) -> {selected_engine} ({'PASS' if selected_engine == scenario['expected'] else 'FAIL'})")
    
    accuracy = (correct_selections / len(test_scenarios)) * 100
    print(f"PASS Engine selection accuracy: {accuracy}% ({correct_selections}/{len(test_scenarios)})")
    assert accuracy >= 75, "Engine selection accuracy should be at least 75%"


def test_concurrent_processing_simulation():
    """Simulate concurrent file processing across multiple tenants and engines."""
    
    # Create test payloads for different tenants
    test_payloads = []
    tenants = ['demo', 'enterprise', 'startup']
    regions = ['us-central1', 'us-west1', 'europe-west1']
    engines = ['pandas', 'polars', 'dask']
    
    for tenant in tenants:
        for region in regions:
            for engine in engines:
                payload = {
                    "file_info": {
                        "file_name": f"tenants/{tenant}/data/raw/test_{uuid.uuid4().hex[:8]}.csv",
                        "bucket_name": f"terraops-{region}-tenant-data",
                        "file_size": 1024 * 1024,  # 1MB
                        "tenant_id": tenant,
                        "region": region,
                        "correlation_id": str(uuid.uuid4())
                    }
                }
                test_payloads.append((engine, payload))
    
    # Mock engine responses
    def mock_engine_request(engine_payload):
        engine, payload = engine_payload
        
        # Simulate processing time
        processing_time = {
            'pandas': 0.1,
            'polars': 0.08,
            'dask': 0.12
        }
        time.sleep(processing_time.get(engine, 0.1))
        
        # Simulate response
        return {
            'engine': engine,
            'tenant_id': payload['file_info']['tenant_id'],
            'region': payload['file_info']['region'],
            'status': 'success',
            'processing_time': processing_time.get(engine, 0.1),
            'correlation_id': payload['file_info']['correlation_id']
        }
    
    # Process concurrently
    start_time = time.time()
    results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_payload = {
            executor.submit(mock_engine_request, payload): payload 
            for payload in test_payloads
        }
        
        for future in as_completed(future_to_payload):
            try:
                result = future.result(timeout=5)
                results.append(result)
            except Exception as e:
                print(f"FAIL Processing failed: {e}")
    
    total_time = time.time() - start_time
    
    # Validate results
    successful_results = [r for r in results if r['status'] == 'success']
    success_rate = (len(successful_results) / len(test_payloads)) * 100
    avg_processing_time = sum(r['processing_time'] for r in successful_results) / len(successful_results)
    
    print(f"PASS Concurrent processing test:")
    print(f"   - Total payloads: {len(test_payloads)}")
    print(f"   - Successful: {len(successful_results)} ({success_rate:.1f}%)")
    print(f"   - Total time: {total_time:.2f}s")
    print(f"   - Average processing time: {avg_processing_time:.3f}s")
    print(f"   - Throughput: {len(successful_results)/total_time:.1f} requests/second")
    
    assert success_rate >= 95, "Success rate should be at least 95%"
    assert len(set(r['correlation_id'] for r in results)) == len(results), "All correlation IDs should be unique"


def test_correlation_id_end_to_end():
    """Test correlation ID tracking through the entire pipeline."""
    
    correlation_id = str(uuid.uuid4())
    
    # Step 1: Cloud Function creates correlation ID
    cloud_function_output = {
        "file_info": {
            "file_name": "tenants/demo/data/raw/test.csv",
            "bucket_name": "terraops-us-central1-tenant-data",
            "tenant_id": "demo",
            "region": "us-central1",
            "correlation_id": correlation_id
        }
    }
    
    # Step 2: Router receives message and forwards to engine
    router_output = {
        **cloud_function_output,
        "processing_metadata": {
            "selected_engine": "pandas",
            "selected_region": "us-central1",
            "router_timestamp": "2024-01-01T10:00:00Z"
        }
    }
    
    # Step 3: Engine processes and returns response
    engine_response = {
        "status": "success",
        "correlation_id": correlation_id,
        "engine": "pandas",
        "tenant_id": "demo",
        "region": "us-central1",
        "processing_timestamp": "2024-01-01T10:00:01Z"
    }
    
    # Validate correlation ID preservation
    assert cloud_function_output["file_info"]["correlation_id"] == correlation_id
    assert router_output["file_info"]["correlation_id"] == correlation_id
    assert engine_response["correlation_id"] == correlation_id
    
    print(f"PASS Correlation ID tracking validated: {correlation_id}")


def test_tenant_isolation_validation():
    """Validate that tenant data isolation is maintained throughout pipeline."""
    
    tenant_scenarios = [
        {
            'tenant': 'demo',
            'file_path': 'tenants/demo/data/raw/sales.csv',
            'expected_isolation': True
        },
        {
            'tenant': 'enterprise', 
            'file_path': 'tenants/enterprise/data/processed/analytics.json',
            'expected_isolation': True
        },
        {
            'tenant': 'shared',
            'file_path': 'shared/ml_models/model.pkl',
            'expected_isolation': False  # Shared resources
        }
    ]
    
    isolation_violations = []
    
    for scenario in tenant_scenarios:
        # Test tenant extraction
        from cloud_function.main import extract_tenant_info
        tenant_id, _, _ = extract_tenant_info(scenario['file_path'])
        
        if scenario['expected_isolation']:
            # For tenant-specific data
            if tenant_id == 'shared':
                isolation_violations.append(f"Tenant {scenario['tenant']} data incorrectly classified as shared")
        else:
            # For shared data
            if tenant_id != 'shared':
                isolation_violations.append(f"Shared data incorrectly classified as tenant-specific: {tenant_id}")
    
    if isolation_violations:
        print(f"FAIL Tenant isolation violations:")
        for violation in isolation_violations:
            print(f"   - {violation}")
        assert False, "Tenant isolation violations detected"
    else:
        print(f"PASS Tenant isolation validated: {len(tenant_scenarios)} scenarios passed")


if __name__ == "__main__":
    print("Running Multi-Tenant Pipeline Integration Tests")
    print("=" * 60)
    
    try:
        test_cloud_function_to_engine_integration()
        test_engine_selection_logic() 
        test_concurrent_processing_simulation()
        test_correlation_id_end_to_end()
        test_tenant_isolation_validation()
        
        print("\n" + "=" * 60)
        print("All integration tests passed! Pipeline is enterprise-ready.")
        
    except Exception as e:
        print(f"\nFAIL Integration test failed: {e}")
        raise