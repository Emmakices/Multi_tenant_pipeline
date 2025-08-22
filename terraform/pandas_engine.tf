# Enable Cloud Run API
resource "google_project_service" "cloudrun_api" {
  service = "run.googleapis.com"
  project = var.project_id
}

# Create Cloud Run services for Pandas processing engine in each active region
resource "google_cloud_run_service" "pandas_processor" {
  for_each = toset(local.active_regions)  # Deploy to same regions as storage
  
  name     = "pandas-processor"
  location = each.value
  project  = var.project_id

  template {
    spec {
      # Optimized for single file processing
      container_concurrency = 1
      timeout_seconds       = 3600  # 1 hour timeout for large processing
      
      containers {
        image = "gcr.io/${var.project_id}/pandas-processor:latest"
        
        # Resource allocation for 0-100MB files
        resources {
          limits = {
            cpu    = "2"      # 2 vCPUs
            memory = "4Gi"    # 4GB RAM
          }
        }
        
        env {
          name  = "PROJECT_ID"
          value = var.project_id
        }
        
        env {
          name  = "PROCESSING_ENGINE"
          value = "pandas"
        }
        
        env {
          name  = "REGION"
          value = each.value
        }
      }
    }
    
    metadata {
      annotations = {
        "autoscaling.knative.dev/maxScale" = "3"    # Max 3 instances per region
        "autoscaling.knative.dev/minScale" = "0"    # Scale to zero when idle
        "run.googleapis.com/cpu-throttling" = "false"
      }
    }
  }

  depends_on = [google_project_service.cloudrun_api]
}

# Output Pandas processor information
output "pandas_processors_info" {
  description = "Information about deployed Pandas processors"
  value = {
    for region in local.active_regions : region => {
      service_name = google_cloud_run_service.pandas_processor[region].name
      region       = region
      url          = google_cloud_run_service.pandas_processor[region].status[0].url
      purpose      = "Processes 0-100MB files using Pandas"
    }
  }
}