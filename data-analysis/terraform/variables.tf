variable "gcp_project_id" {
  type        = string
  description = "GCP project ID."
  default     = "uber-data-analysis-470512"
}

variable "gcp_region" {
  type        = string
  description = "The GCP region to deploy resources in."
  default     = "europe-west4"
}

variable "bq_dataset_id" {
  type        = string
  description = "The BigQuery dataset ID."
  default     = "uber_analytics"
}

variable "bq_table_staging_id" {
  type        = string
  description = "The BigQuery table ID for raw/staging data."
  default     = "bronze"
}

# Treat personal email as deployment sa
variable "personal_email" {
  type        = string
  description = "Your personal email for ACLs."
  default     = "danielperezjensen@gmail.com"
}

variable "gcp_credentials_file" {
  type        = string
  description = "Path to the GCP credentials JSON file."
  default     = "../.config/key.json"
}
