#!/usr/bin/env python3
"""
Simple health check for the multi-tenant pipeline engines.
This validates that all engines are deployed and responding correctly.
"""

import json
import logging
import sys
import time

import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Engine URLs
ENGINE_URLS = {
    "pandas": "https://pandas-processor-253940201126.us-central1.run.app",
    "polars": "https://polars-processor-253940201126.us-central1.run.app", 
    "dask": "https://dask-processor-253940201126.us-central1.run.app"
}

def test_health_endpoint(engine_name: str, base_url: str) -> bool:
    """Test the health endpoint of an engine."""
    try:
        url = f"{base_url}/health"
        logger.info(f"Testing {engine_name} health at {url}")
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ {engine_name} engine is healthy")
            logger.info(f"   Status: {data.get('status', 'unknown')}")
            logger.info(f"   Engine: {data.get('engine', 'unknown')}")
            return True
        else:
            logger.error(f"❌ {engine_name} engine unhealthy: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ {engine_name} engine error: {e}")
        return False

def test_root_endpoint(engine_name: str, base_url: str) -> bool:
    """Test the root endpoint of an engine."""
    try:
        logger.info(f"Testing {engine_name} root endpoint")
        
        response = requests.get(base_url, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"✅ {engine_name} root endpoint accessible")
            return True
        else:
            logger.error(f"❌ {engine_name} root endpoint failed: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ {engine_name} root endpoint error: {e}")
        return False

def main():
    """Main function to test all engine health."""
    logger.info("🏥 Starting pipeline health check")
    
    all_healthy = True
    
    for engine_name, base_url in ENGINE_URLS.items():
        logger.info(f"\n🔍 Testing {engine_name} engine...")
        
        # Test root endpoint
        root_ok = test_root_endpoint(engine_name, base_url)
        
        # Test health endpoint
        health_ok = test_health_endpoint(engine_name, base_url)
        
        if not (root_ok and health_ok):
            all_healthy = False
        
        time.sleep(1)  # Brief pause between engines
    
    # Summary
    logger.info("\n" + "="*50)
    if all_healthy:
        logger.info("🎉 All engines are HEALTHY! Pipeline is ready for processing.")
        return 0
    else:
        logger.error("❌ Some engines are UNHEALTHY. Check deployment status.")
        return 1

if __name__ == "__main__":
    sys.exit(main())