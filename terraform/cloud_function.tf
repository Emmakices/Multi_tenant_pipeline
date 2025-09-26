# Define the Cloud Function configuration for global bucket monitoring
locals {
  # Single Cloud Function region (Canada) - monitors ALL regional buckets
  function_region = "northamerica-northeast1"
  
  # All 12 regional storage buckets that will trigger the function
  monitored_buckets = [
    "terraops-us-central1-tenant-data",
    "terraops-us-east1-tenant-data", 
    "terraops-us-east4-tenant-data",
    "terraops-us-west1-tenant-data",
    "terraops-us-west2-tenant-data",
    "terraops-northamerica-northeast1-tenant-data",
    "terraops-europe-west1-tenant-data",
    "terraops-europe-west2-tenant-data",
    "terraops-europe-west3-tenant-data",
    "terraops-europe-west4-tenant-data",
    "terraops-europe-west9-tenant-data",
    "terraops-africa-south1-tenant-data"
  ]
}

# Create bucket to store Cloud Function source code
# trunk-ignore(checkov/CKV_GCP_29)
resource "google_storage_bucket" "function_source" {
  name     = "${var.project_id}-function-source"
  location = "northamerica-northeast1"  # Canada region

  # Enforce public access prevention
  public_access_prevention = "enforced"

  # Enable uniform bucket-level access
  uniform_bucket_level_access = true

  # Enable versioning for function code management
  versioning {
    enabled = true
  }

  # Enable access logging
  logging {
    log_bucket        = "${var.project_id}-logs-bucket"
    log_object_prefix = "function-source-access-logs"
  }

  labels = {
    environment = var.environment
    purpose     = "function_source_code"
    created_by  = "terraform"
  }
}

# Upload function source code zip file
resource "google_storage_bucket_object" "function_source" {
  name   = "function-source.zip"
  bucket = google_storage_bucket.function_source.name
  source = "../cloud_function/function-source.zip"  # Updated path to external folder
  
  depends_on = [google_storage_bucket.function_source]
}

# Create Cloud Functions for each monitored bucket
# Single function per bucket for proper event handling
resource "google_cloudfunctions_function" "global_file_processor" {
  for_each = toset(local.monitored_buckets)
  
  name        = "global-file-processor-${replace(each.value, "terraops-", "")}"
  region      = local.function_region
  description = "Enhanced function triggered by file uploads to ${each.value} with tenant parsing and file type detection"
  runtime     = "python311"
  
  available_memory_mb   = 256  # Increased for enhanced processing
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.function_source.name
  entry_point          = "hello_gcs"
  timeout              = 60   # 60 seconds timeout
  
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = each.value
  }
  
  environment_variables = {
    PUBSUB_TOPIC      = google_pubsub_topic.send_to_router.id
    DEAD_LETTER_TOPIC = google_pubsub_topic.dead_letter_queue.id
  }
  
  labels = {
    environment = var.environment
    purpose     = "global_file_ingestion"
    version     = "2.0-enhanced"
    created_by  = "terraform"
  }
}

# Output Cloud Function information
output "global_cloud_functions_info" {
  description = "Information about global file processing Cloud Functions"
  value = {
    deployment_region = local.function_region
    function_count    = length(local.monitored_buckets)
    monitored_buckets = local.monitored_buckets
    pubsub_topic     = google_pubsub_topic.send_to_router.id
    dead_letter_queue = google_pubsub_topic.dead_letter_queue.id
    functions = {
      for bucket in local.monitored_buckets : bucket => {
        function_name = google_cloudfunctions_function.global_file_processor[bucket].name
        trigger_bucket = bucket
        region_deployed = local.function_region
      }
    }
  }
}