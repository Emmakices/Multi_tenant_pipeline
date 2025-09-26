variable "pubsub_kms_key_name" {
  description = "The Cloud KMS key name to use for CMEK encryption of the Pub/Sub topic"
  type        = string
  default     = "projects/data-pipeline-project-450922/locations/global/keyRings/multi-tenant-keyring/cryptoKeys/pubsub-key"
}

terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = "northamerica-northeast1"  # Canada region
}

# Create Pub/Sub topic in Canada region for routing messages
resource "google_pubsub_topic" "send_to_router" {
  name    = "send_to_router"
  project = var.project_id
  kms_key_name = var.pubsub_kms_key_name
  
  # Message retention for reliability
  message_retention_duration = "604800s"  # 7 days
  
  # Labels for organization
  labels = {
    environment = var.environment
    purpose     = "multi_region_ingestion"
    region      = "canada"
    created_by  = "terraform"
    version     = "v2-enhanced"
  }
}

# Create Dead Letter Queue topic for failed message handling
resource "google_pubsub_topic" "dead_letter_queue" {
  name    = "file-processing-dead-letter-queue"
  project = var.project_id
  kms_key_name = var.pubsub_kms_key_name
  
  # Message retention for error analysis
  message_retention_duration = "2592000s"  # 30 days for error investigation
  
  # Labels for organization
  labels = {
    environment = var.environment
    purpose     = "dead_letter_queue"
    region      = "canada"
    created_by  = "terraform"
    version     = "v2-enhanced"
  }
}

# Create subscription for router function to consume file processing messages
resource "google_pubsub_subscription" "router_subscription" {
  name  = "router-file-processing-subscription"
  topic = google_pubsub_topic.send_to_router.name
  
  # Acknowledgment deadline - time for router to process message
  ack_deadline_seconds = 300  # 5 minutes
  
  # Message retention for unacknowledged messages
  message_retention_duration = "604800s"  # 7 days
  
  # Retry policy for failed messages
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"  # 10 minutes max backoff
  }
  
  # Dead letter policy - send failed messages to DLQ after 5 attempts
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter_queue.id
    max_delivery_attempts = 5
  }
  
  # Push configuration for router function
  push_config {
    push_endpoint = "https://router-function-url"  # Will be updated with actual router URL
    
    # Attributes for message filtering and routing
    attributes = {
      "x-goog-version" = "v1"
      "content-type"   = "application/json"
    }
    
    # Authentication for secure message delivery
    oidc_token {
      service_account_email = "router-service-account@${var.project_id}.iam.gserviceaccount.com"
    }
  }
  
  # Enable exactly once delivery for data consistency
  enable_exactly_once_delivery = true
  
  # Filter for processing only supported file types (optional optimization)
  filter = "attributes.category IN ('data', 'model', 'reference') AND attributes.file_type != 'unknown'"
  
  labels = {
    environment = var.environment
    purpose     = "router_message_consumption"
    created_by  = "terraform"
    version     = "v2-enhanced"
  }
}

# Create subscription for dead letter queue monitoring
resource "google_pubsub_subscription" "dlq_monitoring_subscription" {
  name  = "dlq-monitoring-subscription"
  topic = google_pubsub_topic.dead_letter_queue.name
  
  # Long acknowledgment deadline for manual investigation
  ack_deadline_seconds = 600  # 10 minutes
  
  # Longer retention for error analysis
  message_retention_duration = "2592000s"  # 30 days
  
  # Pull-based subscription for manual monitoring tools
  # No push_config - will be pulled by monitoring systems
  
  labels = {
    environment = var.environment
    purpose     = "error_monitoring"
    created_by  = "terraform"
  }
}

# Output the enhanced Pub/Sub configuration details
output "enhanced_pubsub_info" {
  description = "Complete information about enhanced Pub/Sub messaging system"
  value = {
    # Main routing topic
    main_topic = {
      name                       = google_pubsub_topic.send_to_router.name
      id                        = google_pubsub_topic.send_to_router.id
      message_retention_days    = 7
      purpose                   = "Enhanced multi-region file ingestion with tenant parsing"
    }
    
    # Dead letter queue
    dead_letter_queue = {
      name                       = google_pubsub_topic.dead_letter_queue.name
      id                        = google_pubsub_topic.dead_letter_queue.id
      message_retention_days    = 30
      purpose                   = "Error handling and failed message analysis"
    }
    
    # Router subscription
    router_subscription = {
      name                      = google_pubsub_subscription.router_subscription.name
      id                       = google_pubsub_subscription.router_subscription.id
      ack_deadline_seconds     = 300
      max_delivery_attempts    = 5
      exactly_once_delivery    = true
      message_filter          = "Processes only supported file types (data, model, reference)"
    }
    
    # Monitoring subscription
    monitoring_subscription = {
      name = google_pubsub_subscription.dlq_monitoring_subscription.name
      id   = google_pubsub_subscription.dlq_monitoring_subscription.id
      purpose = "Dead letter queue monitoring for error analysis"
    }
  }
}

# Output message attributes that Cloud Function publishes
output "message_attributes_info" {
  description = "Message attributes published by enhanced Cloud Function"
  value = {
    attributes = [
      "tenant_id - Extracted tenant identifier (shared, business-domains, tenant-001, etc.)",
      "region - Source region from bucket name",
      "file_type - Detected file type (csv, json, parquet, etc.)",
      "category - Processing category (data, model, reference, log)",
      "correlation_id - Unique tracking ID for end-to-end tracing"
    ]
    
    message_structure = {
      core_info = "file_name, bucket_name, file_size, time_created, content_type"
      tenant_context = "region, tenant_id, data_type, folder_category" 
      file_intelligence = "file_info (type, category, supported status)"
      processing_metadata = "correlation_id, processing_timestamp, function_version"
      routing_hints = "requires_processing, priority, engine_suggestion"
    }
    
    filtering_capabilities = {
      subscription_filter = "Only processes supported file types automatically"
      attribute_routing = "Router can filter by tenant, region, file type, category"
      correlation_tracking = "Full message traceability with correlation IDs"
    }
  }
}