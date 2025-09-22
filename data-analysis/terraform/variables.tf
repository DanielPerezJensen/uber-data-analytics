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

variable "bq_table_bronze_id" {
  type        = string
  description = "The BigQuery table ID for bronze data."
  default     = "bronze"
}

variable "bq_table_silver_id" {
  type        = string
  description = "The BigQuery table ID for silver data."
  default     = "silver"
}

variable "bq_table_gold_locations_id" {
  type        = string
  description = "The BigQuery table ID for gold locations data."
  default     = "gold_locations"
}

variable "bq_table_gold_rides_id" {
  type        = string
  description = "The BigQuery table ID for gold rides data."
  default     = "gold_rides"
}

variable "bq_table_gold_weather_id" {
  type        = string
  description = "The BigQuery table ID for gold weather data."
  default     = "gold_weather"
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
