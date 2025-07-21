# variables.tf
variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = "data-pipeline-project-450922"
}

variable "bucket_prefix" {
  description = "Prefix for bucket names"
  type        = string
  default     = "terraops"
}

variable "environment" {
  description = "Environment (e.g., prod, staging, dev)"
  type        = string
  default     = "prod"
}

# locals.tf
locals {
  # Define ALL regions - easily extendable (keeping all regions for future expansion)
  regions = [
    "us-west1",
    "europe-west1",
    "northamerica-northeast1",
    "asia-southeast1",
    "southamerica-east1"
  ]

  # Active regions for startup phase - ONLY THESE WILL CREATE BUCKETS
  # Uncomment regions as you expand to new markets
  active_regions = [
    "us-west1",              # Primary region - ACTIVE
    # "europe-west1",        # Uncomment when you have EU customers
    # "northamerica-northeast1", # Uncomment for Canadian customers
    # "asia-southeast1",     # Uncomment for Asian customers
    # "southamerica-east1"   # Uncomment for South American customers
  ]

  # Generate bucket names for tenant data buckets (ONLY for active regions)
  tenant_buckets = {
    for region in local.active_regions : region => "${var.bucket_prefix}-${region}-tenant-data"
  }

  # Generate bucket names for shared buckets (ONLY for active regions)
  shared_buckets = {
    for region in local.active_regions : region => "${var.bucket_prefix}-${region}-shared"
  }

  # Startup-optimized bucket configuration for MAXIMUM cost savings
  common_bucket_config = {
    storage_class               = "NEARLINE"  # 50% cheaper than STANDARD, still immediate access
    uniform_bucket_level_access = true
    versioning_enabled          = false      # DISABLED for cost savings - enable later when revenue grows
  }

  # Simplified tenant folder structure optimized for startup workflow
  # Each tenant now has their own data folder for complete isolation
  tenant_folder_structure = [
    "tenants/",
    "tenants/demo/",                          # Demo tenant for testing
    "tenants/demo/data/",                     # Demo tenant's data folder
    "tenants/demo/data/raw/",                 # Demo tenant's raw data
    "tenants/demo/data/processed/",           # Demo tenant's processed data
    "tenants/demo/data/bad_records/",         # Demo tenant's bad records
    "tenants/demo/data/models/",              # Demo tenant's ML models
    "tenants/demo/ecommerce/",                # Demo tenant's ecommerce domain
    "tenants/demo/ecommerce/orders/",
    "tenants/demo/ecommerce/products/",
    "tenants/demo/ecommerce/bad_records/",    # Domain-specific bad records
    "tenants/demo/healthcare/",               # Demo tenant's healthcare domain
    "tenants/demo/healthcare/bad_records/",   
    "tenants/tenant-001/",                    # First real tenant
    "tenants/tenant-001/data/",               # Tenant-001's data folder
    "tenants/tenant-001/data/raw/",           # Tenant-001's raw data
    "tenants/tenant-001/data/processed/",     # Tenant-001's processed data
    "tenants/tenant-001/data/bad_records/",   # Tenant-001's bad records
    "tenants/tenant-001/data/models/",        # Tenant-001's ML models
    "tenants/tenant-001/bad_records/"         # Tenant-001's general bad records
  ]

  # Simplified shared folder structure
  shared_folder_structure = [
    "reference-data/",
    "reference-data/countries/",
    "reference-data/currencies/",
    "reference-data/exchange-rates/",
    "reference-data/bad_records/",    # NEW: Bad records in shared reference data
    "ml-models/",
    "ml-models/shared/",
    "ml-models/bad_records/"          # NEW: Failed or corrupted ML model artifacts
  ]
}

# main.tf
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id  # Uses data-pipeline-project-450922 by default
  region  = "us-west1"      # Default region for resources
}

# Create tenant data buckets ONLY for active regions (cost-optimized)
resource "google_storage_bucket" "tenant_buckets" {
  for_each = local.tenant_buckets

  name          = each.value
  location      = upper(each.key)
  storage_class = local.common_bucket_config.storage_class
  
  uniform_bucket_level_access = local.common_bucket_config.uniform_bucket_level_access

  versioning {
    enabled = local.common_bucket_config.versioning_enabled
  }

  # STARTUP-OPTIMIZED lifecycle rules for automatic cost management
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 90  # Move to COLDLINE after 90 days (70% cheaper than STANDARD)
    }
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
    condition {
      age = 365  # Move to ARCHIVE after 1 year (80% cheaper than STANDARD)
    }
  }

  # Special lifecycle rule for bad_records - move to cheaper storage much faster
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age                   = 7   # Move bad records to COLDLINE after just 7 days
      matches_prefix        = ["tenants/"]
      matches_suffix        = ["/bad_records/"]
    }
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
    condition {
      age                   = 30  # Archive bad records after 30 days (vs 365 for normal data)
      matches_prefix        = ["tenants/"]
      matches_suffix        = ["/bad_records/"]
    }
  }

  # Optional: Uncomment to delete very old data and save even more costs
  # lifecycle_rule {
  #   action {
  #     type = "Delete"
  #   }
  #   condition {
  #     age = 2555  # Delete after 7 years (adjust based on compliance requirements)
  #   }
  # }

  labels = {
    environment = var.environment
    type        = "tenant"
    region      = replace(each.key, "-", "_")
    cost_tier   = "startup_optimized"
    created_by  = "terraform"
  }

  # Prevent accidental deletion of buckets with data
  lifecycle {
    prevent_destroy = true
  }
}

# Create shared buckets ONLY for active regions (cost-optimized)
resource "google_storage_bucket" "shared_buckets" {
  for_each = local.shared_buckets

  name          = each.value
  location      = upper(each.key)
  storage_class = local.common_bucket_config.storage_class
  
  uniform_bucket_level_access = local.common_bucket_config.uniform_bucket_level_access

  versioning {
    enabled = local.common_bucket_config.versioning_enabled
  }

  # Aggressive lifecycle rules for shared data (accessed even less frequently)
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 60  # Shared data moves to COLDLINE sooner (60 days vs 90 for tenant data)
    }
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
    condition {
      age = 180  # Archive shared data after 6 months (vs 1 year for tenant data)
    }
  }

  # Special lifecycle rule for bad_records - move to cheaper storage faster
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age                   = 7   # Move bad records to COLDLINE after just 7 days
      matches_prefix        = ["reference-data/bad_records/", "ml-models/bad_records/"]
    }
  }

  labels = {
    environment = var.environment
    type        = "shared"
    region      = replace(each.key, "-", "_")
    cost_tier   = "startup_optimized"
    created_by  = "terraform"
  }

  # Prevent accidental deletion of buckets with data
  lifecycle {
    prevent_destroy = true
  }
}

# Create placeholder objects to establish folder structure in tenant buckets (ONLY active regions)
resource "google_storage_bucket_object" "tenant_folder_structure" {
  for_each = {
    for combination in setproduct(keys(local.tenant_buckets), local.tenant_folder_structure) :
    "${combination[0]}-${replace(combination[1], "/", "-")}" => {
      region = combination[0]
      folder = combination[1]
    }
  }

  name   = "${each.value.folder}.gitkeep"
  bucket = google_storage_bucket.tenant_buckets[each.value.region].name
  content = "# Folder structure maintained for startup-optimized multi-tenant architecture"
  
  # Set to NEARLINE immediately to match bucket default
  storage_class = "NEARLINE"
  
  depends_on = [google_storage_bucket.tenant_buckets]
}

# Create placeholder objects to establish folder structure in shared buckets (ONLY active regions)
resource "google_storage_bucket_object" "shared_folder_structure" {
  for_each = {
    for combination in setproduct(keys(local.shared_buckets), local.shared_folder_structure) :
    "${combination[0]}-${replace(combination[1], "/", "-")}" => {
      region = combination[0]
      folder = combination[1]
    }
  }

  name   = "${each.value.folder}.gitkeep"
  bucket = google_storage_bucket.shared_buckets[each.value.region].name
  content = "# Shared folder structure for reference data and ML models"
  
  # Set to NEARLINE immediately to match bucket default
  storage_class = "NEARLINE"
  
  depends_on = [google_storage_bucket.shared_buckets]
}

# outputs.tf
output "startup_configuration_summary" {
  description = "Summary of startup-optimized configuration"
  value = {
    total_regions_configured = length(local.regions)
    active_regions_count     = length(local.active_regions)
    active_regions          = local.active_regions
    inactive_regions        = setsubtract(local.regions, local.active_regions)
    storage_class           = local.common_bucket_config.storage_class
    versioning_enabled      = local.common_bucket_config.versioning_enabled
    estimated_monthly_cost  = "$5-20 (vs $100-200 with all regions + STANDARD + versioning)"
  }
}

output "active_regions" {
  description = "Currently active regions where buckets are created"
  value       = local.active_regions
}

output "all_available_regions" {
  description = "All configured regions available for future expansion"
  value       = local.regions
}

output "tenant_bucket_names" {
  description = "Map of active regions to tenant bucket names"
  value       = local.tenant_buckets
}

output "shared_bucket_names" {
  description = "Map of active regions to shared bucket names"
  value       = local.shared_buckets
}

output "tenant_bucket_urls" {
  description = "Map of active regions to tenant bucket URLs"
  value = {
    for region, bucket_name in local.tenant_buckets :
    region => "gs://${bucket_name}"
  }
}

output "shared_bucket_urls" {
  description = "Map of active regions to shared bucket URLs"
  value = {
    for region, bucket_name in local.shared_buckets :
    region => "gs://${bucket_name}"
  }
}

output "startup_bucket_details" {
  description = "Complete bucket information optimized for startup phase"
  value = {
    configuration = {
      storage_class       = local.common_bucket_config.storage_class
      versioning_enabled  = local.common_bucket_config.versioning_enabled
      lifecycle_rules     = "Enabled - auto-move to cheaper storage"
      uniform_access      = local.common_bucket_config.uniform_bucket_level_access
    }
    cost_optimization = {
      storage_savings     = "50% vs STANDARD storage class"
      versioning_savings  = "30-50% (versioning disabled)"
      lifecycle_savings   = "Additional 70-80% for old data"
      total_savings       = "70-80% vs full enterprise configuration"
    }
    active_buckets = {
      tenant_buckets = {
        for region in local.active_regions :
        region => {
          name          = local.tenant_buckets[region]
          url           = "gs://${local.tenant_buckets[region]}"
          location      = upper(region)
          storage_class = local.common_bucket_config.storage_class
        }
      }
      shared_buckets = {
        for region in local.active_regions :
        region => {
          name          = local.shared_buckets[region]
          url           = "gs://${local.shared_buckets[region]}"
          location      = upper(region)
          storage_class = local.common_bucket_config.storage_class
        }
      }
    }
  }
}

# Startup-specific usage examples (only for active regions)
output "startup_usage_examples" {
  description = "Example paths optimized for startup workflow with tenant-isolated data"
  value = length(local.active_regions) > 0 ? {
    # Demo tenant examples
    demo_raw_data          = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/demo/data/raw/customer-data-2024-07-21.csv"
    demo_processed_data    = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/demo/data/processed/ml-features-2024.parquet"
    demo_models            = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/demo/data/models/customer-prediction-v1.pkl"
    demo_bad_records       = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/demo/data/bad_records/corrupted-data-2024-07-21.json"
    demo_ecommerce_orders  = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/demo/ecommerce/orders/sample-orders.csv"
    demo_ecommerce_bad     = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/demo/ecommerce/bad_records/invalid-orders.csv"
    
    # Tenant-001 examples (production tenant)
    tenant_raw_data        = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/raw/sales-data-2024-07-21.csv"
    tenant_processed_data  = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/processed/analytics-ready.parquet"
    tenant_models          = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/models/revenue-forecast-v2.pkl"
    tenant_bad_records     = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/bad_records/failed-processing.csv"
    
    # Shared resources
    reference_countries    = "gs://${local.shared_buckets[local.active_regions[0]]}/reference-data/countries/countries.json"
    reference_bad_data     = "gs://${local.shared_buckets[local.active_regions[0]]}/reference-data/bad_records/invalid-currency-data.json"
    shared_ml_models       = "gs://${local.shared_buckets[local.active_regions[0]]}/ml-models/shared/sentiment-analysis-v2.pkl"
    failed_ml_artifacts    = "gs://${local.shared_buckets[local.active_regions[0]]}/ml-models/bad_records/corrupted-model.pkl"
  } : {
    error = "No active regions configured. Please uncomment at least one region in locals.active_regions"
  }
}

# Cost monitoring and scaling guidance
output "cost_optimization_summary" {
  description = "Detailed breakdown of cost optimization features"
  value = {
    current_savings = {
      storage_class_savings    = "50% (NEARLINE vs STANDARD)"
      versioning_savings      = "30-50% (disabled)"
      regional_savings        = "${100 - (length(local.active_regions) * 100 / length(local.regions))}% (${length(local.active_regions)}/${length(local.regions)} regions active)"
      lifecycle_savings       = "70-80% for data >90 days old"
    }
    lifecycle_rules = {
      nearline_to_coldline = "90 days"
      coldline_to_archive  = "365 days"
      shared_data_faster   = "60 days to COLDLINE, 180 days to ARCHIVE"
    }
    estimated_costs = {
      current_configuration = "$5-20/month"
      if_all_regions       = "$25-100/month"
      if_standard_storage  = "$50-200/month"
      if_versioning_enabled = "$75-300/month"
      enterprise_full      = "$100-500/month"
    }
  }
}

# Scaling instructions for when you're ready to expand
output "scaling_instructions" {
  description = "Step-by-step guide to scale your infrastructure"
  value = {
    phase_1_startup = {
      description = "Current phase - optimized for minimal costs"
      active_regions = length(local.active_regions)
      features = ["NEARLINE storage", "No versioning", "Lifecycle rules", "Single region"]
      monthly_cost = "$5-20"
    }
    phase_2_early_customers = {
      description = "Add second region when you get international customers"
      action = "Uncomment europe-west1 in active_regions list, run terraform apply"
      features = ["2 regions", "NEARLINE storage", "Consider enabling versioning"]
      monthly_cost = "$15-40"
    }
    phase_3_growth = {
      description = "Scale to multiple regions as customer base grows"
      action = "Uncomment additional regions, change storage_class to STANDARD for active data"
      features = ["3+ regions", "Mixed storage classes", "Versioning enabled"]
      monthly_cost = "$50-150"
    }
    phase_4_enterprise = {
      description = "Full enterprise features when you have steady revenue"
      action = "Enable all regions, STANDARD storage, versioning, add monitoring"
      features = ["All regions", "STANDARD storage", "Full versioning", "Advanced monitoring"]
      monthly_cost = "$100-500+"
    }
    quick_expansion_commands = [
      "# Add Europe: Uncomment 'europe-west1' in active_regions",
      "# Add Asia: Uncomment 'asia-southeast1' in active_regions", 
      "# Enable versioning: Set versioning_enabled = true",
      "# Faster storage: Change storage_class to 'STANDARD'",
      "# Then run: terraform plan && terraform apply"
    ]
  }
}

# Ready-to-use bucket information for your applications
output "application_integration" {
  description = "Ready-to-use configuration for your application code"
  value = length(local.active_regions) > 0 ? {
    primary_region = local.active_regions[0]
    tenant_bucket_template = "${var.bucket_prefix}-{region}-tenant-data"
    shared_bucket_template = "${var.bucket_prefix}-{region}-shared"
    folder_patterns = {
      tenant_data = "tenants/{tenant_id}/data/{data_type}/"
      tenant_raw_data = "tenants/{tenant_id}/data/raw/"
      tenant_processed_data = "tenants/{tenant_id}/data/processed/"
      tenant_models = "tenants/{tenant_id}/data/models/"
      tenant_bad_records = "tenants/{tenant_id}/data/bad_records/"
      tenant_domain_data = "tenants/{tenant_id}/ecommerce/{data_type}/"
      reference_data = "reference-data/{data_type}/"
      reference_bad_records = "reference-data/bad_records/"
    }
    storage_settings = {
      default_class = local.common_bucket_config.storage_class
      versioning = local.common_bucket_config.versioning_enabled
      lifecycle_enabled = true
    }
  } : null
}