#!/usr/bin/env python3
"""
Upload test data to Google Cloud Storage and test pipeline processing.
This script validates the pipeline with real data instead of relying only on unit tests.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from google.cloud import storage

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "multi-tenant-data-pipeline"
BUCKET_NAME = "multi-tenant-ecommerce-data"
ENGINE_URLS = {
    "pandas": "https://pandas-processor-253940201126.us-central1.run.app",
    "polars": "https://polars-processor-253940201126.us-central1.run.app", 
    "dask": "https://dask-processor-253940201126.us-central1.run.app"
}

def upload_to_gcs(local_file_path: str, gcs_path: str) -> bool:
    """Upload file to Google Cloud Storage."""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        
        logger.info(f"Uploading {local_file_path} to gs://{BUCKET_NAME}/{gcs_path}")
        blob.upload_from_filename(local_file_path)
        
        logger.info(f"✅ Successfully uploaded {gcs_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload {gcs_path}: {e}")
        return False

def test_engine(engine_name: str, file_info: dict) -> bool:
    """Test a specific processing engine with the uploaded data."""
    try:
        url = f"{ENGINE_URLS[engine_name]}/process"
        payload = {"file_info": file_info}
        
        logger.info(f"Testing {engine_name} engine with {file_info['file_name']}")
        
        response = requests.post(url, json=payload, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ {engine_name} engine processed successfully:")
            logger.info(f"   - Good records: {result.get('good_records', 'N/A')}")
            logger.info(f"   - Bad records: {result.get('bad_records', 'N/A')}")
            logger.info(f"   - Tenant: {result.get('tenant_id', 'N/A')}")
            return True
        else:
            logger.error(f"❌ {engine_name} engine failed: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ {engine_name} engine error: {e}")
        return False

def main():
    """Main function to upload test data and test pipeline processing."""
    logger.info("🚀 Starting pipeline validation with real test data")
    
    # Test data files to upload
    test_files = [
        {
            "local_path": "test_data/tenant_a_sample.csv",
            "gcs_path": "tenants/tenant-a/us-west1/ecommerce-data-2024-08-28.csv",
            "region": "us-west1"
        },
        {
            "local_path": "test_data/tenant_b_sample.csv", 
            "gcs_path": "tenants/tenant-b/europe-west1/ecommerce-data-2024-08-28.csv",
            "region": "europe-west1"
        },
        {
            "local_path": "test_data/shared_sample.csv",
            "gcs_path": "shared/us-central1/ecommerce-data-2024-08-28.csv", 
            "region": "us-central1"
        }
    ]
    
    # Upload test files to GCS
    upload_success = True
    for file_info in test_files:
        if not upload_to_gcs(file_info["local_path"], file_info["gcs_path"]):
            upload_success = False
    
    if not upload_success:
        logger.error("❌ Failed to upload some test files. Cannot proceed with testing.")
        sys.exit(1)
    
    logger.info("✅ All test files uploaded successfully")
    time.sleep(5)  # Wait for files to be available
    
    # Test each engine with each file
    all_tests_passed = True
    engines_to_test = ["pandas", "polars", "dask"]
    
    for file_info in test_files:
        file_size = os.path.getsize(file_info["local_path"])
        gcs_file_info = {
            "file_name": file_info["gcs_path"],
            "bucket_name": BUCKET_NAME,
            "file_size": file_size,
            "region": file_info["region"]
        }
        
        logger.info(f"\n📊 Testing processing of {file_info['gcs_path']}")
        
        for engine in engines_to_test:
            if not test_engine(engine, gcs_file_info):
                all_tests_passed = False
        
        time.sleep(2)  # Brief pause between files
    
    # Summary
    logger.info("\n" + "="*60)
    if all_tests_passed:
        logger.info("🎉 All pipeline tests PASSED! Pipeline is working correctly.")
        sys.exit(0)
    else:
        logger.error("❌ Some pipeline tests FAILED. Check logs above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()