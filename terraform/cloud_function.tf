# Define the regions where Cloud Functions will be deployed
locals {
  # Use the same active regions from your storage configuration
  function_regions = [
    "us-west1",
    # "europe-west1",        # Uncomment when you expand
    # "northamerica-northeast1",
    # "asia-southeast1", 
    # "southamerica-east1"
  ]
  
  # Map regions to their corresponding storage buckets
  storage_buckets = {
    for region in local.function_regions : region => "terraops-${region}-tenant-data"
  }
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

# Create Cloud Functions for each region
# trunk-ignore(checkov/CKV_GCP_124)
# trunk-ignore(checkov/CKV_GCP_124)
resource "google_cloudfunctions_function" "trigger_pubsub" {
  for_each = toset(local.function_regions)
  
  name        = "trigger_pubsub"
  region      = each.value  
  description = "Function triggered by file uploads to ${local.storage_buckets[each.value]} bucket"
  runtime     = "python311"
  
  available_memory_mb   = 128
  source_archive_bucket = google_storage_bucket.function_source.name
  source_archive_object = google_storage_bucket_object.function_source.name
  entry_point          = "hello_gcs"
  
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = local.storage_buckets[each.value]
  }
  
  environment_variables = {
    PUBSUB_TOPIC = google_pubsub_topic.send_to_router.id
    REGION       = each.value
  }
  
  labels = {
    environment = var.environment
    purpose     = "multi_region_ingestion"
    region      = replace(each.value, "-", "_")
    created_by  = "terraform"
  }
}

# Output Cloud Function information
output "cloud_functions_info" {
  description = "Information about deployed Cloud Functions"
  value = {
    for region in local.function_regions : region => {
      function_name = google_cloudfunctions_function.trigger_pubsub[region].name
      region        = region
      trigger_bucket = local.storage_buckets[region]
      pubsub_topic  = google_pubsub_topic.send_to_router.id
    }
  }
}