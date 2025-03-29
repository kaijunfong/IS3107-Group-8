variable "project_id" {
  type        = string
  description = "The GCP project ID."
  default = "is3107-group-8"
}

variable "region" {
  type        = string
  description = "The region for GCP resources."
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "The zone for GCP resources."
  default     = "us-central1-a"
}

variable "credentials_file" {
  description = "Path to the GCP service account credentials file."
  type        = string
  sensitive   = true
}

variable "dag_bucket_name" {
  description = "Name of the GCS bucket to store dag-related files"
  type        = string
  sensitive   = true
  default     = "dag-related"
}