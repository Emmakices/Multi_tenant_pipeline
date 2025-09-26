# Create Pub/Sub subscription to receive messages from existing send_to_router topic
resource "google_pubsub_subscription" "router_subscription" {
  name  = "router-subscription"
  topic = google_pubsub_topic.send_to_router.name  # Uses existing topic
  
  # Message retention and retry configuration
  message_retention_duration = "604800s"  # 7 days
  retain_acked_messages      = false
  ack_deadline_seconds       = 300        # 5 minutes
  
  labels = {
    environment = var.environment
    purpose     = "message_routing"
    created_by  = "terraform"
  }
}

# Create router Cloud Function triggered by Pub/Sub subscription
resource "google_cloudfunctions_function" "router_function" {
  name        = "processing-router"
  region      = "northamerica-northeast1"  # Canada region (same as Pub/Sub topic)
  description = "Routes file processing requests to appropriate engines based on size and region"
  runtime     = "python311"
  
  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.router_source.name
  entry_point          = "route_message"
  
  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_subscription.router_subscription.name
  }
  
  environment_variables = {
    PROJECT_ID = var.project_id
  }
  
  labels = {
    environment = var.environment
    purpose     = "message_routing"
    created_by  = "terraform"
  }
}

# Upload router function source code
resource "google_storage_bucket_object" "router_source" {
  name   = "router-source.zip"
  bucket = google_storage_bucket.function_source.name
  source = "../router_function/router-source.zip"
  
  depends_on = [google_storage_bucket.function_source]
}

# Output router function information
output "router_function_info" {
  description = "Information about the router function"
  value = {
    function_name    = google_cloudfunctions_function.router_function.name
    region          = google_cloudfunctions_function.router_function.region
    subscription    = google_pubsub_subscription.router_subscription.name
    source_topic    = google_pubsub_topic.send_to_router.name
    purpose         = "Routes messages to appropriate processing engines"
  }
}