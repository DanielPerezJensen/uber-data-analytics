# This module contains all the resources for the Cloud Function that ingests data from GCS to BigQuery
# Most of it is based on: https://cloud.google.com/functions/docs/samples/functions-v2-basic-gcs

# Create a zip archive of the Cloud Function source code
data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "../cloud_functions/data_ingestion"
  output_path = "../cloud_functions/data_ingestion/tmp/function.zip"
}

# Upload the zip file to a GCS bucket
resource "google_storage_bucket_object" "function_zip_upload" {
  name   = "src-${data.archive_file.function_zip.output_md5}.zip"
  bucket = google_storage_bucket.cloud_function_bucket.name
  source = data.archive_file.function_zip.output_path

  depends_on = [
    google_storage_bucket.cloud_function_bucket,
    data.archive_file.function_zip
  ]
}

# Get GCS service account for Pub/Sub publishing role
# Grant Pub/Sub Publisher role to the GCS service account, needed for event triggers
data "google_storage_project_service_account" "gcs_service_account" {
}

# Grant the service account read access to the raw data bucket
resource "google_storage_bucket_iam_member" "function_gcs_reader" {
  bucket = google_storage_bucket.raw_data_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

resource "google_project_iam_member" "gcs_pubsub_publishing" {
  project = var.gcp_project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${data.google_storage_project_service_account.gcs_service_account.email_address}"
}

# Grant all necesarry permissions to the service account running the function
resource "google_project_iam_member" "invoking" {
  project    = var.gcp_project_id
  role       = "roles/run.invoker"
  member     = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

resource "google_project_iam_member" "event_receiving" {
  project    = var.gcp_project_id
  role       = "roles/eventarc.eventReceiver"
  member     = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

resource "google_project_iam_member" "artifactregistry_reader" {
  project    = var.gcp_project_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.data_analytics_sa.email}"
}

# Configure the Cloud Function using previously defined resources
resource "google_cloudfunctions2_function" "data_ingestion_function" {
  name        = "data-ingestion-function"
  description = "Cloud Function to ingest data from GCS to BigQuery"
  location    = var.gcp_region

  build_config {
    runtime     = "python312"
    entry_point = "load_data_from_gcs_to_bigquery"

    source {
      storage_source {
        bucket = google_storage_bucket.cloud_function_bucket.name
        object = google_storage_bucket_object.function_zip_upload.name
      }
    }
  }

  service_config {
    available_memory      = "256M"      # Minimum memory, fits free tier
    timeout_seconds       = 60          # Default timeout, adjust as needed
    max_instance_count    = 1           # Prevents scaling beyond 1 instance (limits cost)
    min_instance_count    = 0           # No always-on instances
    ingress_settings      = "ALLOW_ALL" # Default, restrict if needed
    service_account_email = google_service_account.data_analytics_sa.email

    environment_variables = {
      GCP_PROJECT = var.gcp_project_id
      BQ_DATASET  = google_bigquery_dataset.uber_analytics_dataset.dataset_id
      BQ_TABLE    = google_bigquery_table.uber_analytics_table.table_id
    }
  }

  # This trigger specifies the function runs when a new file is finalized in the bucket
  event_trigger {
    trigger_region        = var.gcp_region
    retry_policy          = "RETRY_POLICY_RETRY"
    service_account_email = google_service_account.data_analytics_sa.email
    event_type            = "google.cloud.storage.object.v1.finalized"
    event_filters {
      attribute = "bucket"
      value     = google_storage_bucket.raw_data_bucket.name
    }
  }

  depends_on = [
    google_project_iam_member.event_receiving,
    google_project_iam_member.artifactregistry_reader,
    google_project_iam_member.gcs_pubsub_publishing,
    google_project_iam_member.invoking
  ]
}
