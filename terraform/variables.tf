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