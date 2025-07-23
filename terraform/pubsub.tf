variable "pubsub_kms_key_name" {
  description = "The Cloud KMS key name to use for CMEK encryption of the Pub/Sub topic"
  type        = string
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
  kms_key_name = var.pubsub_kms_key_name # Ensure this variable is defined and points to your CMEK resource
  
  # Labels for organization
  labels = {
    environment = var.environment
    purpose     = "multi_region_ingestion"
    region      = "canada"
    created_by  = "terraform"
  }
}

# Output the topic details for reference
output "pubsub_topic_info" {
  description = "Information about the send_to_router Pub/Sub topic"
  value = {
    topic_name = google_pubsub_topic.send_to_router.name
    topic_id   = google_pubsub_topic.send_to_router.id
    project    = google_pubsub_topic.send_to_router.project
    purpose    = "Central routing topic for multi-region file ingestion"
  }
}