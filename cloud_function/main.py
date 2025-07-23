import json
import os
from google.cloud import pubsub_v1

def hello_gcs(event, context):
    """
    Cloud Function triggered by Cloud Storage file uploads.
    Publishes file metadata to Pub/Sub topic.
    """
    
    # Get environment variables
    pubsub_topic = os.environ.get('PUBSUB_TOPIC')
    region = os.environ.get('REGION')
    
    # Extract file information from the event
    file_name = event['name']
    bucket_name = event['bucket']
    file_size = event.get('size', 0)
    time_created = event.get('timeCreated')
    
    # Create message payload
    message_data = {
        'file_name': file_name,
        'bucket_name': bucket_name,
        'file_size': int(file_size),
        'region': region,
        'time_created': time_created,
        'event_type': 'file_upload'
    }
    
    # Publish to Pub/Sub
    try:
        publisher = pubsub_v1.PublisherClient()
        
        # Convert message to JSON bytes
        message_bytes = json.dumps(message_data).encode('utf-8')
        
        # Publish message
        future = publisher.publish(pubsub_topic, message_bytes)
        message_id = future.result()
        
        print(f"Message published to {pubsub_topic}. Message ID: {message_id}")
        print(f"File: {file_name}, Size: {file_size}, Region: {region}")
        
        return f"Message published successfully: {message_id}"
        
    except Exception as e:
        print(f"Error publishing message: {str(e)}")
        raise e