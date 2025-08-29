# Define your GCP provider and project
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# Service Account for Data Analytics
resource "google_service_account" "data_analytics_sa" {
  account_id   = "uber-da-sa"
  display_name = "Uber Data Analytics Service Account"
}

# Cloud Storage Bucket for raw data with Uniform Bucket-Level Access enabled
resource "google_storage_bucket" "raw_data_bucket" {
  name                        = "${var.gcp_project_id}-raw-data-ingestion"
  location                    = var.gcp_region
  force_destroy               = true
  uniform_bucket_level_access = true # This is the crucial setting
}

resource "google_storage_bucket_iam_member" "data_analytics_sa_iam_admin" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

resource "google_storage_bucket_iam_member" "personal_email_iam_writer" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.admin"
  member = "user:${var.personal_email}"
}

# BigQuery Dataset
resource "google_bigquery_dataset" "uber_analytics_dataset" {
  dataset_id = var.bq_dataset_id
  project    = var.gcp_project_id
  location   = var.gcp_region
  friendly_name = "Uber Analytics Dataset"
  description   = "Dataset for storing Uber ride analytics data"

}

# Grant the user running Terraform Data Owner access to the dataset
resource "google_bigquery_dataset_iam_member" "personal_email_data_owner" {
  dataset_id = google_bigquery_dataset.uber_analytics_dataset.dataset_id
  role       = "roles/bigquery.dataOwner"
  member     = "user:${var.personal_email}"
}

data "google_iam_policy" "owner" {
  binding {
    role = "roles/bigquery.dataOwner"

    members = [
      "user:${var.personal_email}",
    ]
  }

  binding {
    role = "roles/bigquery.dataEditor"

    members = [
      "serviceAccount:${google_service_account.data_analytics_sa.email}",
    ]
  }
}

resource "google_bigquery_dataset_iam_policy" "dataset" {
  dataset_id  = google_bigquery_dataset.uber_analytics_dataset.dataset_id
  policy_data = data.google_iam_policy.owner.policy_data
}

# BigQuery Table (created with a placeholder schema)
resource "google_bigquery_table" "uber_analytics_table" {
  dataset_id = google_bigquery_dataset.uber_analytics_dataset.dataset_id
  table_id   = var.bq_table_staging_id
  project    = var.gcp_project_id
}
