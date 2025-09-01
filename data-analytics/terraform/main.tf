# Terraform configuration for GCP Data Analytics Project

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

# All project level IAM roles go here
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

resource "google_project_iam_member" "bigquery_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

resource "google_project_iam_member" "project_owner" {
  project = var.gcp_project_id
  role    = "roles/owner"
  member  = "user:${var.personal_email}"
}

# Grant the service account permissions to deploy Cloud Functions
resource "google_project_iam_member" "cloud_functions_admin" {
  project = var.gcp_project_id
  role    = "roles/cloudfunctions.admin"
  member  = "user:${var.personal_email}"
}
