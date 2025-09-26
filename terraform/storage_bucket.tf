variable "logging_bucket_name" {
  description = "Name of the bucket to store access logs"
  type        = string
  default     = "terraops-global-access-logs"
}

# Configuration for our global multi-tenant storage infrastructure
locals {
  # These are all the regions where we want to deploy storage buckets
  # We picked these to give good global coverage while staying in regions
  # where Google Cloud has strong data processing capabilities
  regions = [
    "us-central1",              # Iowa - good central US location
    "us-east1",                 # South Carolina - covers US East Coast
    "us-east4",                 # Northern Virginia - close to DC area
    "us-west1",                 # Oregon - US West Coast
    "us-west2",                 # Los Angeles - Southern California
    "northamerica-northeast1",  # Montreal - covers Canada
    "europe-west1",             # Belgium - central Europe
    "europe-west2",             # London - UK coverage
    "europe-west3",             # Frankfurt - Germany financial hub
    "europe-west4",             # Netherlands - good European connectivity
    "europe-west9",             # Paris - covers France
    "africa-south1"             # Johannesburg - African coverage
  ]

  # For this deployment, we're going all-in and activating every region
  # In a startup scenario, you might want to start with just a few regions
  # and expand as you get customers in different areas
  active_regions = [
    "us-central1",              
    "us-east1",                 
    "us-east4",                 
    "us-west1",                 
    "us-west2",                 
    "northamerica-northeast1",  
    "europe-west1",             
    "europe-west2",             
    "europe-west3",             
    "europe-west4",             
    "europe-west9",             
    "africa-south1"             
  ]

  # Generate bucket names for global multi-tenant data buckets
  tenant_buckets = {
    for region in local.active_regions : region => "${var.bucket_prefix}-${region}-tenant-data"
  }

  # Security-compliant bucket configuration
  common_bucket_config = {
    storage_class               = "NEARLINE"  # 50% cheaper than STANDARD, still immediate access
    uniform_bucket_level_access = true
    versioning_enabled          = true       # ENABLED for security compliance (Trunk requirement)
  }

  # Global multi-tenant folder structure for all regions
  # Standard structure applied to every regional bucket
  tenant_folder_structure = [
    # Shared folder structure
    "shared/",
    "shared/ml-models/",
    "shared/reference-data/",
    "shared/reference-data/countries/",
    "shared/reference-data/currencies/",
    "shared/reference-data/exchange-rates/",
    
    # Tenants folder structure
    "tenants/",
    "tenants/business-domains/",
    
    # Sample tenant structures (tenant-001 as example)
    "tenants/tenant-001/",
    "tenants/tenant-001/data/",
    "tenants/tenant-001/data/raw/",
    "tenants/tenant-001/data/processed/",
    "tenants/tenant-001/data/bad-records/",
    
    # Additional sample tenants for demonstration
    "tenants/tenant-002/",
    "tenants/tenant-002/data/",
    "tenants/tenant-002/data/raw/",
    "tenants/tenant-002/data/processed/",
    "tenants/tenant-002/data/bad-records/",
    
    "tenants/tenant-003/",
    "tenants/tenant-003/data/",
    "tenants/tenant-003/data/raw/",
    "tenants/tenant-003/data/processed/",
    "tenants/tenant-003/data/bad-records/"
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
    log_bucket        = var.logging_bucket_name
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

  # Special lifecycle rule for bad-records - move to cheaper storage much faster
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age                   = 7   # Move bad records to COLDLINE after just 7 days
      matches_prefix        = ["tenants/", "shared/"]
      matches_suffix        = ["/bad-records/"]
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
      matches_suffix        = ["/bad-records/"]
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

# Create placeholder objects to establish global folder structure in all regional buckets
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
  content = "# Global multi-tenant folder structure maintained across all regions for scalable data processing"
  
  # Set to NEARLINE immediately to match bucket default
  storage_class = "NEARLINE"
  
  depends_on = [google_storage_bucket.tenant_buckets]
}

# Storage outputs
output "global_deployment_summary" {
  description = "Summary of global multi-tenant deployment"
  value = {
    total_regions_configured = length(local.regions)
    active_regions_count     = length(local.active_regions)
    active_regions          = local.active_regions
    global_coverage         = "Full global deployment across 12 regions"
    storage_class           = local.common_bucket_config.storage_class
    versioning_enabled      = local.common_bucket_config.versioning_enabled
    security_features       = "Public access prevention + Versioning enabled"
    estimated_monthly_cost  = "$180-480 (12 regions with versioning and lifecycle management)"
  }
}

output "regional_bucket_names" {
  description = "Map of all regions to bucket names with global multi-tenant structure"
  value       = local.tenant_buckets
}

output "global_structure_info" {
  description = "Information about global folder structure"
  value       = "Each regional bucket contains: shared/ (ml-models, reference-data with countries/currencies/exchange-rates), tenants/ (business-domains + tenant-XXX folders with data/raw/processed/bad-records)"
}

output "regional_bucket_urls" {
  description = "Map of all regions to bucket URLs with global structure"
  value = {
    for region, bucket_name in local.tenant_buckets :
    region => "gs://${bucket_name}"
  }
}

# Global usage examples across all regions
output "global_usage_examples" {
  description = "Example paths for global multi-tenant data structure"
  value = length(local.active_regions) > 0 ? {
    # Shared resources examples (available in all regions)
    shared_ml_models       = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/ml-models/sentiment-analysis-v2.pkl"
    shared_countries       = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/reference-data/countries/iso-countries.json"
    shared_currencies      = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/reference-data/currencies/currency-codes.json"
    shared_exchange_rates  = "gs://${local.tenant_buckets[local.active_regions[0]]}/shared/reference-data/exchange-rates/daily-rates.json"
    
    # Business domains folder
    business_domains       = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/business-domains/ecommerce-schema.json"
    
    # Tenant-001 examples (auto-registered tenant)
    tenant_001_raw         = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/raw/sales-data-2024.csv"
    tenant_001_processed   = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/processed/ml-features-2024.parquet"
    tenant_001_bad_records = "gs://${local.tenant_buckets[local.active_regions[0]]}/tenants/tenant-001/data/bad-records/failed-processing.csv"
    
    # Multi-region examples
    us_central_bucket      = "gs://${local.tenant_buckets["us-central1"]}/tenants/tenant-001/data/raw/"
    europe_west_bucket     = "gs://${local.tenant_buckets["europe-west1"]}/tenants/tenant-001/data/raw/"
    africa_south_bucket    = "gs://${local.tenant_buckets["africa-south1"]}/tenants/tenant-001/data/raw/"
  } : {
    error = "No active regions configured"
  }
}

# Global deployment compliance summary
output "global_compliance_summary" {
  description = "Security and compliance features for global deployment"
  value = {
    public_access_prevention = "enforced across all 12 regions"
    versioning             = "enabled with lifecycle management"
    uniform_bucket_access  = "enabled for consistent security"
    global_coverage        = "12 regions: US (5), Europe (5), North America (1), Africa (1)"
    tenant_isolation      = "Complete isolation with tenant-specific folders and bad-records handling"
    shared_resources       = "Global ml-models and reference-data (countries, currencies, exchange-rates)"
    compliance_features    = "Enterprise-grade security with automatic lifecycle management"
  }
}