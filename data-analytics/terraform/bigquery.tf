# BigQuery Terraform Configuration

# BigQuery Dataset
resource "google_bigquery_dataset" "uber_analytics_dataset" {
  dataset_id    = var.bq_dataset_id
  project       = var.gcp_project_id
  location      = var.gcp_region
  friendly_name = "Uber Analytics Dataset"
  description   = "Dataset for storing Uber ride analytics data"

}

# Grant bigquery.dataEditor at the dataset level
resource "google_bigquery_dataset_iam_member" "data_editor" {
  dataset_id = google_bigquery_dataset.uber_analytics_dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

# Grant bigquery.dataOwner at the dataset level
resource "google_bigquery_dataset_iam_member" "data_admin" {
  dataset_id = google_bigquery_dataset.uber_analytics_dataset.dataset_id
  role       = "roles/bigquery.admin"
  member     = "user:${var.personal_email}"
}

# BigQuery Table (created with a placeholder schema)
resource "google_bigquery_table" "uber_analytics_table" {
  dataset_id          = google_bigquery_dataset.uber_analytics_dataset.dataset_id
  table_id            = var.bq_table_staging_id
  project             = var.gcp_project_id
  deletion_protection = false
}
