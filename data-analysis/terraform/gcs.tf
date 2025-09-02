# GCS Terraform Configuration

# Cloud Storage Bucket for raw data with Uniform Bucket-Level Access enabled
resource "google_storage_bucket" "raw_data_bucket" {
  name                        = "${var.gcp_project_id}-raw-data-ingestion"
  location                    = var.gcp_region
  force_destroy               = true
  uniform_bucket_level_access = true # This is the crucial setting
}

resource "google_storage_bucket" "cloud_function_bucket" {
  name                        = "${var.gcp_project_id}-cloud-function-bucket"
  location                    = var.gcp_region
  force_destroy               = true
  uniform_bucket_level_access = true
}
