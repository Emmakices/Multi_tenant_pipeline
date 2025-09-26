import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

from google.cloud import pubsub_v1

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Supported file types for processing
SUPPORTED_FILE_TYPES = {
    '.csv': 'csv',
    '.json': 'json', 
    '.jsonl': 'jsonl',
    '.parquet': 'parquet',
    '.xlsx': 'excel',
    '.xls': 'excel',
    '.pkl': 'pickle',
    '.pickle': 'pickle',
    '.txt': 'text',
    '.log': 'log'
}

# File type categories for processing routing
FILE_CATEGORIES = {
    'data': ['.csv', '.json', '.jsonl', '.parquet', '.xlsx', '.xls'],
    'model': ['.pkl', '.pickle'],
    'reference': ['.json', '.csv', '.txt'],
    'log': ['.log', '.txt']
}

def extract_tenant_info(file_path: str) -> Tuple[str, str, str]:
    """
    Parse the file path to figure out which tenant owns this file and what type of data it is.
    
    This is pretty important for our multi-tenant setup - we need to know if this file
    belongs to a specific tenant, is shared across all tenants, or is some kind of 
    business configuration. The routing downstream depends on getting this right.
    
    Args:
        file_path: The full path where the file lives, like "tenants/tenant-001/data/raw/sales.csv"
        
    Returns:
        A tuple with three pieces of info:
        - tenant_id: Who owns this file (could be 'shared', 'tenant-001', etc.)
        - data_type: What kind of data this is ('raw', 'processed', 'ml-models', etc.)  
        - folder_category: The broad category ('shared', 'tenant-data', 'business-domain')
    """
    try:
        # Split the path into parts so we can analyze the structure
        path_parts = file_path.split('/')
        
        # Handle shared resources - these are files that all tenants can access
        if file_path.startswith('shared/'):
            if len(path_parts) >= 2:
                # Second part tells us what type of shared resource (ml-models, reference-data, etc.)
                return 'shared', path_parts[1], 'shared'
            return 'shared', 'unknown', 'shared'
            
        # Handle tenant-specific files
        elif file_path.startswith('tenants/'):
            if len(path_parts) >= 2:
                # Check if this is business domain configuration
                if path_parts[1] == 'business-domains':
                    return 'business-domains', 'schema', 'business-domain'
                
                # Check if this is a specific tenant folder (tenant-001, tenant-002, etc.)
                elif path_parts[1].startswith('tenant-'):
                    tenant_id = path_parts[1]
                    
                    # If it's in the data subfolder, we can be more specific about the type
                    if len(path_parts) >= 4 and path_parts[2] == 'data':
                        folder_type = path_parts[3]  # This will be 'raw', 'processed', or 'bad-records'
                        return tenant_id, folder_type, 'tenant-data'
                    
                    # File is under the tenant but not in a data subfolder
                    return tenant_id, 'unknown', 'tenant-data'
                else:
                    # Some other folder under tenants that we don't recognize
                    return path_parts[1], 'unknown', 'tenant-data'
                    
            return 'unknown', 'unknown', 'tenant-data'
        else:
            # Files at the root level - this shouldn't happen with our folder structure
            return 'root', 'unknown', 'root'
            
    except Exception as e:
        # If anything goes wrong parsing the path, log it and return defaults
        logger.warning(f"Had trouble figuring out tenant info from path {file_path}: {str(e)}")
        return 'unknown', 'unknown', 'unknown'

def detect_file_type(file_name: str) -> Dict[str, str]:
    """
    Look at a file name and figure out what kind of file it is and how we should process it.
    
    This helps the router decide which processing engine to use. For example, large parquet
    files might be better handled by Dask, while small CSV files work fine with Pandas.
    
    Args:
        file_name: Just the file name with its extension, like "sales_data.csv"
        
    Returns:
        A dictionary with details about the file:
        - extension: The file extension (.csv, .json, etc.)
        - file_type: What we think this file contains (csv, json, parquet, etc.)
        - category: Broader category (data, model, reference, log)
        - is_supported: Whether we can actually process this file type
        - file_name_without_ext: The name without the extension
    """
    try:
        file_path = Path(file_name)
        extension = file_path.suffix.lower()
        
        file_type = SUPPORTED_FILE_TYPES.get(extension, 'unknown')
        
        # Determine processing category
        category = 'unknown'
        for cat, extensions in FILE_CATEGORIES.items():
            if extension in extensions:
                category = cat
                break
        
        return {
            'extension': extension,
            'file_type': file_type,
            'category': category,
            'is_supported': extension in SUPPORTED_FILE_TYPES,
            'file_name_without_ext': file_path.stem
        }
        
    except Exception as e:
        logger.warning(f"Error detecting file type for {file_name}: {str(e)}")
        return {
            'extension': 'unknown',
            'file_type': 'unknown', 
            'category': 'unknown',
            'is_supported': False,
            'file_name_without_ext': file_name
        }

def extract_region_from_bucket(bucket_name: str) -> str:
    """
    Extract region from bucket name.
    
    Args:
        bucket_name: Bucket name (e.g., "terraops-us-central1-tenant-data")
        
    Returns:
        Region name or 'unknown'
    """
    try:
        # Pattern: terraops-{region}-tenant-data
        match = re.search(r'terraops-(.+)-tenant-data', bucket_name)
        if match:
            return match.group(1)
        return 'unknown'
    except Exception:
        return 'unknown'

def hello_gcs(event, context):
    """
    Enhanced Cloud Function triggered by Cloud Storage file uploads.
    Publishes enriched file metadata to Pub/Sub topic with tenant parsing,
    file type detection, and structured logging.
    """
    
    # Generate correlation ID for tracking
    correlation_id = str(uuid.uuid4())
    processing_timestamp = datetime.now(timezone.utc).isoformat()
    
    # Set up structured logging context
    log_context = {
        'correlation_id': correlation_id,
        'function': 'hello_gcs',
        'processing_timestamp': processing_timestamp
    }
    
    logger.info("Processing file upload event", extra=log_context)
    
    try:
        # Get environment variables
        pubsub_topic = os.environ.get('PUBSUB_TOPIC')
        dead_letter_topic = os.environ.get('DEAD_LETTER_TOPIC')
        
        # Extract file information from the event
        file_name = event['name']
        bucket_name = event['bucket']
        file_size = event.get('size', 0)
        time_created = event.get('timeCreated')
        content_type = event.get('contentType', 'unknown')
        
        # Extract region from bucket name
        region = extract_region_from_bucket(bucket_name)
        
        # Parse tenant information from file path
        tenant_id, data_type, folder_category = extract_tenant_info(file_name)
        
        # Detect file type and processing category
        file_info = detect_file_type(file_name)
        
        # Enhanced logging with context
        log_context.update({
            'file_name': file_name,
            'bucket_name': bucket_name,
            'tenant_id': tenant_id,
            'region': region,
            'file_type': file_info['file_type']
        })
        
        logger.info("File metadata extracted successfully", extra=log_context)
        
        # Create enriched message payload
        message_data = {
            # Core file information
            'file_name': file_name,
            'bucket_name': bucket_name,
            'file_size': int(file_size),
            'time_created': time_created,
            'content_type': content_type,
            'event_type': 'file_upload',
            
            # Regional and tenant context
            'region': region,
            'tenant_id': tenant_id,
            'data_type': data_type,
            'folder_category': folder_category,
            
            # File type information
            'file_info': file_info,
            
            # Processing metadata
            'correlation_id': correlation_id,
            'processing_timestamp': processing_timestamp,
            'function_version': '2.0-enhanced',
            
            # Routing hints for downstream processing
            'routing_hints': {
                'requires_processing': file_info['is_supported'],
                'priority': 'high' if folder_category == 'tenant-data' else 'normal',
                'engine_suggestion': 'auto' if file_info['is_supported'] else 'skip'
            }
        }
        
        # Validate message before publishing
        if not pubsub_topic:
            raise ValueError("PUBSUB_TOPIC environment variable not set")
            
        if not file_info['is_supported']:
            logger.warning("Unsupported file type detected, publishing with skip flag", extra=log_context)
        
        # Publish to Pub/Sub with enhanced error handling
        publisher = pubsub_v1.PublisherClient()
        
        # Convert message to JSON bytes
        message_bytes = json.dumps(message_data).encode('utf-8')
        
        # Add message attributes for Pub/Sub filtering
        attributes = {
            'tenant_id': tenant_id,
            'region': region,
            'file_type': file_info['file_type'],
            'category': file_info['category'],
            'correlation_id': correlation_id
        }
        
        # Publish message with attributes
        future = publisher.publish(pubsub_topic, message_bytes, **attributes)
        message_id = future.result()
        
        # Success logging
        log_context.update({
            'message_id': message_id,
            'pubsub_topic': pubsub_topic,
            'status': 'success'
        })
        
        logger.info("Message published successfully to Pub/Sub", extra=log_context)
        
        return {
            'status': 'success',
            'message_id': message_id,
            'correlation_id': correlation_id,
            'tenant_id': tenant_id,
            'region': region,
            'file_supported': file_info['is_supported']
        }
        
    except Exception as e:
        # Enhanced error logging
        error_context = log_context.copy()
        error_context.update({
            'error': str(e),
            'error_type': type(e).__name__,
            'status': 'error'
        })
        
        logger.error("Error processing file upload event", extra=error_context, exc_info=True)
        
        # Attempt to send to dead letter queue if configured
        if 'dead_letter_topic' in locals() and dead_letter_topic:
            try:
                error_message = {
                    'original_event': event,
                    'error': str(e),
                    'correlation_id': correlation_id,
                    'processing_timestamp': processing_timestamp,
                    'function': 'hello_gcs'
                }
                
                publisher = pubsub_v1.PublisherClient()
                error_bytes = json.dumps(error_message).encode('utf-8')
                future = publisher.publish(dead_letter_topic, error_bytes)
                dlq_message_id = future.result()
                
                logger.info(f"Error message sent to dead letter queue: {dlq_message_id}", extra=error_context)
                
            except Exception as dlq_error:
                logger.error(f"Failed to send message to dead letter queue: {str(dlq_error)}", extra=error_context)
        
        # Re-raise the original exception
        raise e