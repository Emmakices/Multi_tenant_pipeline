#!/usr/bin/env python3

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import google.auth.transport.requests
import google.oauth2.id_token
import requests
from flask import Flask, request, jsonify
from google.cloud import pubsub_v1

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Global configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'data-pipeline-project-450922')
DEAD_LETTER_TOPIC = os.environ.get('DEAD_LETTER_TOPIC')

# Global engine registry across all 12 regions
GLOBAL_ENGINES = {
    # US Regions
    "us-central1": {
        "pandas": "https://pandas-processor-253940201126.us-central1.run.app",
        "polars": "https://polars-processor-253940201126.us-central1.run.app", 
        "dask": "https://dask-processor-253940201126.us-central1.run.app"
    },
    "us-east1": {
        "pandas": "https://pandas-processor-253940201126.us-east1.run.app",
        "polars": "https://polars-processor-253940201126.us-east1.run.app",
        "dask": "https://dask-processor-253940201126.us-east1.run.app"
    },
    "us-east4": {
        "pandas": "https://pandas-processor-253940201126.us-east4.run.app",
        "polars": "https://polars-processor-253940201126.us-east4.run.app",
        "dask": "https://dask-processor-253940201126.us-east4.run.app"
    },
    "us-west1": {
        "pandas": "https://pandas-processor-253940201126.us-west1.run.app",
        "polars": "https://polars-processor-253940201126.us-west1.run.app",
        "dask": "https://dask-processor-253940201126.us-west1.run.app"
    },
    "us-west2": {
        "pandas": "https://pandas-processor-253940201126.us-west2.run.app",
        "polars": "https://polars-processor-253940201126.us-west2.run.app",
        "dask": "https://dask-processor-253940201126.us-west2.run.app"
    },
    # North America
    "northamerica-northeast1": {
        "pandas": "https://pandas-processor-253940201126.northamerica-northeast1.run.app",
        "polars": "https://polars-processor-253940201126.northamerica-northeast1.run.app",
        "dask": "https://dask-processor-253940201126.northamerica-northeast1.run.app"
    },
    # Europe Regions  
    "europe-west1": {
        "pandas": "https://pandas-processor-253940201126.europe-west1.run.app",
        "polars": "https://polars-processor-253940201126.europe-west1.run.app",
        "dask": "https://dask-processor-253940201126.europe-west1.run.app"
    },
    "europe-west2": {
        "pandas": "https://pandas-processor-253940201126.europe-west2.run.app",
        "polars": "https://polars-processor-253940201126.europe-west2.run.app",
        "dask": "https://dask-processor-253940201126.europe-west2.run.app"
    },
    "europe-west3": {
        "pandas": "https://pandas-processor-253940201126.europe-west3.run.app",
        "polars": "https://polars-processor-253940201126.europe-west3.run.app",
        "dask": "https://dask-processor-253940201126.europe-west3.run.app"
    },
    "europe-west4": {
        "pandas": "https://pandas-processor-253940201126.europe-west4.run.app",
        "polars": "https://polars-processor-253940201126.europe-west4.run.app",
        "dask": "https://dask-processor-253940201126.europe-west4.run.app"
    },
    "europe-west9": {
        "pandas": "https://pandas-processor-253940201126.europe-west9.run.app",
        "polars": "https://polars-processor-253940201126.europe-west9.run.app",
        "dask": "https://dask-processor-253940201126.europe-west9.run.app"
    },
    # Africa
    "africa-south1": {
        "pandas": "https://pandas-processor-253940201126.africa-south1.run.app",
        "polars": "https://polars-processor-253940201126.africa-south1.run.app",
        "dask": "https://dask-processor-253940201126.africa-south1.run.app"
    }
}

# Engine health cache
ENGINE_HEALTH_CACHE = {}
HEALTH_CHECK_INTERVAL = 300  # 5 minutes


def get_auth_token(url: str) -> str:
    """Get authentication token for Cloud Run."""
    try:
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        return token
    except Exception as e:
        logger.warning(f"Failed to get auth token for {url}: {e}")
        return None

def check_engine_health(region: str, engine: str, url: str) -> Dict[str, Any]:
    """Check health of a single engine with caching."""
    cache_key = f"{region}:{engine}"
    current_time = time.time()
    
    # Check cache first
    if cache_key in ENGINE_HEALTH_CACHE:
        cached_result, cache_time = ENGINE_HEALTH_CACHE[cache_key]
        if current_time - cache_time < HEALTH_CHECK_INTERVAL:
            return cached_result
    
    try:
        token = get_auth_token(url)
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        
        response = requests.get(f"{url}/health", headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            result = {
                "status": "healthy",
                "region": region,
                "engine": engine,
                "details": data,
                "last_checked": current_time
            }
        else:
            result = {
                "status": "unhealthy",
                "region": region,
                "engine": engine,
                "error": f"HTTP {response.status_code}",
                "last_checked": current_time
            }
            
    except requests.exceptions.Timeout:
        result = {
            "status": "timeout",
            "region": region,
            "engine": engine,
            "error": "Request timeout",
            "last_checked": current_time
        }
    except Exception as e:
        result = {
            "status": "error",
            "region": region,
            "engine": engine,
            "error": str(e),
            "last_checked": current_time
        }
    
    # Cache result
    ENGINE_HEALTH_CACHE[cache_key] = (result, current_time)
    return result

def select_optimal_engine(message_data: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    This is where we make the smart decision about which processing engine to use.
    
    We look at things like file size, file type, and where the file came from, then
    try to pick the best engine for the job. Big files generally go to Dask since it
    can handle distributed processing, while smaller files often work fine with Pandas.
    
    We also try to keep processing close to where the data lives to minimize network
    transfer costs and latency.
    
    Returns: (region, engine_type, engine_url) - the engine we picked
    """
    file_info = message_data.get('file_info', {})
    file_size = message_data.get('file_size', 0)
    source_region = message_data.get('region', 'us-central1')
    tenant_id = message_data.get('tenant_id', 'unknown')
    
    # Figure out which engines would be best for this particular file
    # We've learned through testing that different engines have sweet spots
    
    if file_info.get('file_type') == 'parquet' or file_info.get('category') == 'data':
        # Parquet files are columnar and work really well with Polars
        # But if it's a big file, Dask can distribute the work
        if file_size > 100 * 1024 * 1024:  # Anything over 100MB
            preferred_engines = ['dask', 'polars', 'pandas']
        else:
            preferred_engines = ['polars', 'pandas', 'dask']
            
    elif file_info.get('file_type') in ['csv', 'excel']:
        # Traditional row-based formats that Pandas handles well
        # But again, big files benefit from Dask's distributed approach
        if file_size > 500 * 1024 * 1024:  # Over 500MB gets heavy for single-machine processing
            preferred_engines = ['dask', 'pandas', 'polars']
        else:
            preferred_engines = ['pandas', 'polars', 'dask']
            
    elif file_info.get('category') == 'model':
        # ML model files (pickles, etc.) usually work best with Pandas ecosystem
        preferred_engines = ['pandas', 'polars', 'dask']
    else:
        # When in doubt, start with Pandas since it's the most compatible
        preferred_engines = ['pandas', 'polars', 'dask']
    
    # Try to keep processing close to where the data lives
    # This reduces network costs and improves performance
    if source_region in GLOBAL_ENGINES:
        preferred_regions = [source_region]
        
        # If the source region doesn't work, try nearby regions first
        # This geography-aware fallback helps with latency and data sovereignty
        if source_region.startswith('us-'):
            # For US regions, prefer other US regions as fallback
            preferred_regions.extend([r for r in GLOBAL_ENGINES.keys() if r.startswith('us-') and r != source_region])
        elif source_region.startswith('europe-'):
            # For European regions, stay in Europe if possible
            preferred_regions.extend([r for r in GLOBAL_ENGINES.keys() if r.startswith('europe-') and r != source_region])
        elif source_region.startswith('northamerica-'):
            # Canada is close to US regions, so those are good fallbacks
            preferred_regions.extend([r for r in GLOBAL_ENGINES.keys() if r.startswith('us-')])
        
        # Finally, add all remaining regions as last resort
        preferred_regions.extend([r for r in GLOBAL_ENGINES.keys() if r not in preferred_regions])
    else:
        # If we don't recognize the source region, just try all available regions
        preferred_regions = list(GLOBAL_ENGINES.keys())
    
    # Now we go through our preferences and find the first healthy engine
    for region in preferred_regions:
        if region not in GLOBAL_ENGINES:
            continue  # Skip regions we don't have engines for
            
        for engine_type in preferred_engines:
            if engine_type not in GLOBAL_ENGINES[region]:
                continue  # Skip engines that aren't deployed in this region
                
            engine_url = GLOBAL_ENGINES[region][engine_type]
            health_result = check_engine_health(region, engine_type, engine_url)
            
            if health_result['status'] == 'healthy':
                logger.info(f"Selected {engine_type} engine in {region} for tenant {tenant_id}")
                return region, engine_type, engine_url
    
    # If we get here, nothing was healthy - this is bad but we need a fallback
    # Let's use our most reliable combination even if the health check failed
    logger.warning(f"Couldn't find any healthy engines for tenant {tenant_id}, falling back to default")
    fallback_region = 'us-central1'
    fallback_engine = 'pandas'
    fallback_url = GLOBAL_ENGINES[fallback_region][fallback_engine]
    return fallback_region, fallback_engine, fallback_url

def send_to_dead_letter_queue(message_data: Dict[str, Any], error: str, correlation_id: str):
    """Send failed message to Dead Letter Queue."""
    if not DEAD_LETTER_TOPIC:
        logger.error(f"Dead letter topic not configured, cannot send failed message: {correlation_id}")
        return
        
    try:
        publisher = pubsub_v1.PublisherClient()
        
        dlq_message = {
            'original_message': message_data,
            'error': error,
            'correlation_id': correlation_id,
            'failed_at': datetime.now(timezone.utc).isoformat(),
            'router_function': 'enhanced_v2.0'
        }
        
        message_bytes = json.dumps(dlq_message).encode('utf-8')
        future = publisher.publish(DEAD_LETTER_TOPIC, message_bytes)
        message_id = future.result()
        
        logger.info(f"Sent message to DLQ: {message_id} for correlation: {correlation_id}")
        
    except Exception as dlq_error:
        logger.error(f"Failed to send message to DLQ for correlation {correlation_id}: {str(dlq_error)}")

def process_file_message(message_data: Dict[str, Any]) -> Dict[str, Any]:
    """Process file upload message and route to appropriate engine."""
    correlation_id = message_data.get('correlation_id', 'unknown')
    tenant_id = message_data.get('tenant_id', 'unknown')
    file_name = message_data.get('file_name', 'unknown')
    
    log_context = {
        'correlation_id': correlation_id,
        'tenant_id': tenant_id,
        'file_name': file_name,
        'function': 'process_file_message'
    }
    
    logger.info("Processing file message", extra=log_context)
    
    try:
        # Check if file should be processed
        routing_hints = message_data.get('routing_hints', {})
        if not routing_hints.get('requires_processing', True):
            logger.info("File marked as skip processing", extra=log_context)
            return {
                'status': 'skipped',
                'reason': 'File type not supported for processing',
                'correlation_id': correlation_id
            }
        
        # Handle shared resources
        if tenant_id == 'shared':
            logger.info("Processing shared resource", extra=log_context)
            # Shared resources can be processed with lower priority
        elif tenant_id == 'business-domains':
            logger.info("Processing business domain configuration", extra=log_context)
            # Business domain configs may need special handling
        
        # Select optimal engine
        region, engine_type, engine_url = select_optimal_engine(message_data)
        
        log_context.update({
            'selected_region': region,
            'selected_engine': engine_type,
            'engine_url': engine_url
        })
        
        logger.info("Selected processing engine", extra=log_context)
        
        # Prepare payload for processing engine
        processing_payload = {
            'file_info': message_data,
            'processing_metadata': {
                'correlation_id': correlation_id,
                'router_timestamp': datetime.now(timezone.utc).isoformat(),
                'selected_engine': engine_type,
                'selected_region': region,
                'tenant_isolation': True if tenant_id.startswith('tenant-') else False
            }
        }
        
        # Send to processing engine
        try:
            token = get_auth_token(engine_url)
            headers = {
                'Content-Type': 'application/json',
                'X-Correlation-ID': correlation_id,
                'X-Tenant-ID': tenant_id
            }
            if token:
                headers['Authorization'] = f'Bearer {token}'
            
            response = requests.post(
                f"{engine_url}/process",
                json=processing_payload,
                headers=headers,
                timeout=300  # 5 minutes
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info("Successfully processed file", extra=log_context)
                return {
                    'status': 'success',
                    'correlation_id': correlation_id,
                    'engine_response': result,
                    'processing_details': {
                        'region': region,
                        'engine': engine_type,
                        'tenant_id': tenant_id
                    }
                }
            else:
                error_msg = f"Engine processing failed: HTTP {response.status_code}"
                logger.error(error_msg, extra=log_context)
                send_to_dead_letter_queue(message_data, error_msg, correlation_id)
                return {
                    'status': 'error',
                    'correlation_id': correlation_id,
                    'error': error_msg
                }
                
        except requests.exceptions.Timeout:
            error_msg = "Engine processing timeout"
            logger.error(error_msg, extra=log_context)
            send_to_dead_letter_queue(message_data, error_msg, correlation_id)
            return {
                'status': 'timeout',
                'correlation_id': correlation_id,
                'error': error_msg
            }
            
        except Exception as processing_error:
            error_msg = f"Engine communication error: {str(processing_error)}"
            logger.error(error_msg, extra=log_context, exc_info=True)
            send_to_dead_letter_queue(message_data, error_msg, correlation_id)
            return {
                'status': 'error',
                'correlation_id': correlation_id,
                'error': error_msg
            }
        
    except Exception as e:
        error_msg = f"Router processing error: {str(e)}"
        logger.error(error_msg, extra=log_context, exc_info=True)
        send_to_dead_letter_queue(message_data, error_msg, correlation_id)
        return {
            'status': 'error',
            'correlation_id': correlation_id,
            'error': error_msg
        }

# Flask endpoints for Pub/Sub integration and health monitoring

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'service': 'enhanced-router-function',
        'version': 'v2.0',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'global_engines_configured': len(GLOBAL_ENGINES),
        'regions': list(GLOBAL_ENGINES.keys())
    }), 200

@app.route('/pubsub-webhook', methods=['POST'])
def pubsub_webhook():
    """
    Webhook endpoint to receive Pub/Sub push messages.
    This endpoint receives enhanced messages from the Cloud Function.
    """
    try:
        # Verify request format
        if not request.json:
            logger.error("No JSON payload received")
            return 'No JSON payload', 400
            
        message = request.json.get('message')
        if not message:
            logger.error("No message in payload")
            return 'No message in payload', 400
        
        # Decode message data
        if 'data' in message:
            try:
                message_data = json.loads(base64.b64decode(message['data']).decode('utf-8'))
            except Exception as decode_error:
                logger.error(f"Failed to decode message data: {str(decode_error)}")
                return 'Failed to decode message', 400
        else:
            logger.error("No data in message")
            return 'No data in message', 400
        
        # Extract attributes for additional context
        attributes = message.get('attributes', {})
        correlation_id = attributes.get('correlation_id', message_data.get('correlation_id', 'unknown'))
        
        # Enhanced logging with correlation tracking
        log_context = {
            'correlation_id': correlation_id,
            'tenant_id': attributes.get('tenant_id', 'unknown'),
            'region': attributes.get('region', 'unknown'),
            'file_type': attributes.get('file_type', 'unknown'),
            'message_id': message.get('messageId', 'unknown'),
            'publish_time': message.get('publishTime', 'unknown')
        }
        
        logger.info("Received Pub/Sub message", extra=log_context)
        
        # Process the file message
        result = process_file_message(message_data)
        
        # Log result
        log_context.update({'processing_result': result.get('status', 'unknown')})
        logger.info("Message processing completed", extra=log_context)
        
        # Return success to acknowledge message
        return jsonify({
            'status': 'processed',
            'correlation_id': correlation_id,
            'result': result
        }), 200
        
    except Exception as e:
        logger.error(f"Error processing Pub/Sub message: {str(e)}", exc_info=True)
        
        # Try to send to DLQ if we can extract correlation ID
        try:
            correlation_id = request.json.get('message', {}).get('attributes', {}).get('correlation_id', 'unknown')
            if correlation_id != 'unknown':
                send_to_dead_letter_queue({}, f"Webhook processing error: {str(e)}", correlation_id)
        except:
            pass  # Best effort DLQ sending
            
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/engines/health', methods=['GET'])
def global_engine_health():
    """Get health status of all global engines."""
    try:
        health_results = {}
        healthy_count = 0
        total_count = 0
        
        for region, engines in GLOBAL_ENGINES.items():
            health_results[region] = {}
            for engine_type, engine_url in engines.items():
                health_result = check_engine_health(region, engine_type, engine_url)
                health_results[region][engine_type] = health_result
                total_count += 1
                if health_result['status'] == 'healthy':
                    healthy_count += 1
        
        overall_status = 'healthy' if healthy_count > total_count * 0.8 else 'degraded' if healthy_count > 0 else 'unhealthy'
        
        return jsonify({
            'overall_status': overall_status,
            'healthy_engines': healthy_count,
            'total_engines': total_count,
            'health_percentage': round((healthy_count / total_count) * 100, 2) if total_count > 0 else 0,
            'regional_health': health_results,
            'last_checked': datetime.now(timezone.utc).isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error checking global engine health: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/engines/clear-cache', methods=['POST'])
def clear_health_cache():
    """Clear engine health cache to force fresh checks."""
    try:
        global ENGINE_HEALTH_CACHE
        cache_size = len(ENGINE_HEALTH_CACHE)
        ENGINE_HEALTH_CACHE.clear()
        
        logger.info(f"Cleared health cache with {cache_size} entries")
        
        return jsonify({
            'status': 'success',
            'message': f'Cleared {cache_size} cached health check results',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error clearing health cache: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Get router function statistics."""
    try:
        return jsonify({
            'service': 'enhanced-router-function',
            'version': 'v2.0',
            'configuration': {
                'total_regions': len(GLOBAL_ENGINES),
                'engines_per_region': 3,  # pandas, polars, dask
                'total_engines': len(GLOBAL_ENGINES) * 3,
                'health_cache_ttl_seconds': HEALTH_CHECK_INTERVAL,
                'dead_letter_queue_configured': DEAD_LETTER_TOPIC is not None
            },
            'regions': list(GLOBAL_ENGINES.keys()),
            'engine_types': ['pandas', 'polars', 'dask'],
            'features': [
                'Intelligent engine selection based on file type and size',
                'Regional preference and geographic failover',
                'Tenant-aware processing with isolation',
                'Dead letter queue integration',
                'Correlation tracking and structured logging',
                'Health monitoring with caching',
                'Enhanced Pub/Sub message processing'
            ],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}", exc_info=True)
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

# Legacy health check function for backwards compatibility
def legacy_health_check():
    """Legacy health check function for testing purposes."""
    print("Enhanced Router Function v2.0 - Global Multi-Tenant Processing")
    print("=" * 60)
    
    healthy_count = 0
    total_count = 0
    
    for region, engines in GLOBAL_ENGINES.items():
        print(f"\nRegion: {region}")
        for engine_type, engine_url in engines.items():
            result = check_engine_health(region, engine_type, engine_url)
            total_count += 1
            
            if result['status'] == 'healthy':
                print(f"  ✓ {engine_type}: HEALTHY")
                healthy_count += 1
            else:
                print(f"  ✗ {engine_type}: {result['status'].upper()} - {result.get('error', 'Unknown error')}")
    
    print(f"\nGLOBAL SUMMARY:")
    print(f"Healthy engines: {healthy_count}/{total_count}")
    print(f"Health percentage: {(healthy_count/total_count)*100:.1f}%")
    
    if healthy_count > total_count * 0.8:
        print("Status: HEALTHY - Ready for production traffic")
    elif healthy_count > 0:
        print("Status: DEGRADED - Some engines unavailable")
    else:
        print("Status: UNHEALTHY - All engines unavailable")
        
    return healthy_count, total_count

if __name__ == "__main__":
    if os.environ.get('FLASK_ENV') == 'development':
        # Development mode - run Flask app
        print("Starting Enhanced Router Function v2.0 in development mode")
        app.run(host='0.0.0.0', port=8080, debug=True)
    else:
        # Production mode or testing - run legacy health check
        legacy_health_check()
