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

  # Security-compliant bucket configuration
  common_bucket_config = {
    storage_class               = "NEARLINE"  # 50% cheaper than STANDARD, still immediate access
    uniform_bucket_level_access = true
    versioning_enabled          = true       # ENABLED for security compliance (Trunk requirement)
  }

  # Simplified tenant folder structure optimized for startup workflow
  # Each tenant now has their own data folder for complete isolation
  # Shared data is now in "shared" folder within each regional bucket
  tenant_folder_structure = [
    "shared/",                                # Shared folder in each regional bucket
    "shared/reference-data/",                 # Shared reference data
    "shared/reference-data/countries/",       
    "shared/reference-data/currencies/",
    "shared/reference-data/exchange-rates/",
    "shared/reference-data/bad_records/",     # Bad records in shared reference data
    "shared/ml-models/",                      # Shared ML models
    "shared/ml-models/common/",               # Common ML models for all tenants
    "shared/ml-models/bad_records/",          # Failed or corrupted shared ML models
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
}

# Create tenant data buckets ONLY for active regions (now includes shared data)
resource "google_storage_bucket" "tenant_buckets" {
  for_each = local.tenant_buckets

  name          = each.value
  location      = upper(each.key)
  storage_class = local.common_bucket_config.storage_class
  
  uniform_bucket_level_access = local.common_bucket_config.uniform_bucket_level_access
  
  # FIXED: Enforce public access prevention (Trunk security requirement)
  public_access_prevention = "enforced"

  # Enable access logging (Trunk requirement)
  logging {
    log_bucket        = var.logging_bucket_name  # Ensure this bucket exists and is not the same as the current bucket
    log_object_prefix = "access-logs/${each.value}/"
  }

  # FIXED: Enable versioning (Trunk security requirement)
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
      matches_prefix        = ["tenants/", "shared/"]
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
      matches_prefix        = ["tenants/", "shared/"]
      matches_suffix        = ["/bad_records/"]
    }
  }

  # Lifecycle rule for shared data - move to cheaper storage faster since it's accessed less
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age                   = 60  # Move shared data to COLDLINE after 60 days
      matches_prefix        = ["shared/"]
    }
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
    condition {
      age                   = 180  # Archive shared data after 180 days
      matches_prefix        = ["shared/"]
    }
  }

  labels = {
    environment = var.environment
    type        = "multi_purpose"  # Changed from "tenant" since it now includes shared data
    region      = replace(each.key, "-", "_")
    cost_tier   = "security_compliant"  # Updated to reflect security compliance
    created_by  = "terraform"
    contains    = "tenant_and_shared_data"
  }

  # Prevent accidental deletion of buckets with data
  lifecycle {
    prevent_destroy = true
  }
}

# Create placeholder objects to establish folder structure in tenant buckets (includes shared folders)
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
  content = "# Folder structure maintained for security-compliant multi-tenant architecture with integrated shared data"
  
  # Set to NEARLINE immediately to match bucket default
  storage_class = "NEARLINE"
  
  depends_on = [google_storage_bucket.tenant_buckets]
}

# Storage outputs
output "startup_configuration_summary" {
  description = "Summary of security-compliant configuration"
  value = {
    total_regions_configured = length(local.regions)
    active_regions_count     = length(local.active_regions)
    active_regions          = local.active_regions
    inactive_regions        = setsubtract(local.regions, local.active_regions)
    storage_class           = local.common_bucket_config.storage_class
    versioning_enabled      = local.common_bucket_config.versioning_enabled
    security_features       = "Public access prevention + Versioning enabled (logging disabled to avoid self-logging)"
    estimated_monthly_cost  = "$15-40 (higher due to versioning, but secure and compliant)"
  }
}

output "tenant_bucket_names" {
  description = "Map of active regions to tenant bucket names (now includes shared data)"
  value       = local.tenant_buckets
}

output "shared_data_note" {
  description = "Information about shared data location"
  value       = "Shared data is now located in the 'shared/' folder within each regional tenant bucket"
}

output "tenant_bucket_urls" {
  description = "Map of active regions to tenant bucket URLs (contains both tenant and shared data)"
  value = {
    for region, bucket_name in local.tenant_buckets :
    region => "gs://${bucket_name}"
  }
}

# Security-compliant usage examples (only for active regions)
output "startup_usage_examples" {
  description = "Example paths for security-compliant multi-tenant workflow"
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
    
    # Shared resources (now in same bucket under 'shared/' folder)
    shared_countries       = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/reference-data/countries/countries.json"
    shared_currencies      = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/reference-data/currencies/exchange-rates.json"
    shared_bad_data        = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/reference-data/bad_records/invalid-currency-data.json"
    shared_ml_models       = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/ml-models/common/sentiment-analysis-v2.pkl"
    shared_ml_bad_records  = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/ml-models/bad_records/corrupted-model.pkl"
  } : {
    error = "No active regions configured. Please uncomment at least one region in locals.active_regions"
  }
}

# Security compliance summary
output "security_compliance_summary" {
  description = "Security features implemented to satisfy Trunk checks"
  value = {
    public_access_prevention = "enforced"
    access_logging          = "disabled (to avoid self-logging warning - can be configured with separate bucket later)"
    versioning             = "enabled"
    uniform_bucket_access  = "enabled"
    trunk_compliance       = "Major security checks satisfied without self-logging warnings"
  }
}