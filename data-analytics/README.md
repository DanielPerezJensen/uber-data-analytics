# Data Pre-Processing & Analytics

## Deliverables

The deliverables of this sub-module are three-fold:

* An automated data pipeline
* An interactive dashboard
* Pre-processed data for LLM Companion

### An Automated Data Pipeline - Solution Design

#### Overview

The data pipeline is designed to automate the ingestion, storage, and processing of ride booking data for analytics purposes. The infrastructure is provisioned and managed using Terraform, ensuring reproducibility and scalability on Google Cloud Platform (GCP).

### Key Resources Deployed via Terraform

1. Google Cloud Storage (GCS) Buckets
Purpose: Store raw and processed data files (e.g., CSVs) for ingestion and analytics.
Terraform Resource: Defined in `gcs.tf`.
Design: Buckets are created with appropriate access controls to allow data upload and retrieval by other components.
2. BigQuery Dataset & Tables
Purpose: Store structured data for analytics and reporting.
Terraform Resource: Defined in `bigquery.tf`.
Design: A BigQuery dataset is provisioned, along with tables to hold ingested ride booking data.
3. Google Cloud Function
Purpose: Automate data ingestion from GCS to BigQuery.
Terraform Resource: Defined in `cloud_function.tf`.
Design:
The Cloud Function is deployed from a zipped source (zipped version of `cloud_functions` directory), which contains the ingestion logic (see `main.py` in `cloud_functions/data_ingestion/`).
The function is triggered by events (a new file upload to raw GCS). After trigger, data is uploaded to staging table in uber-analytics BigQuery dataset
4. Supporting Terraform Files
main.tf: Coordinates resource creation and provider configuration.
variables.tf: Defines configurable parameters (e.g., project ID, bucket names).

#### Workflow

* Data Upload: New ride booking data is uploaded to the GCS bucket.
* Trigger: The upload event triggers the Cloud Function.
* Ingestion: The Cloud Function reads the file, processes the data, and loads it into BigQuery.
* Analytics: Data in BigQuery is available for querying and analysis.
* Security & Access:
  * IAM roles and permissions are managed to restrict access to resources.
  * Service accounts are used for Cloud Function execution with least privilege.

### An Interactive Dashboard - Solution Design

A visual tool for stakeholders to monitor key metrics, track cancellation trends, and gain a quick understanding of the operational health of the ride-sharing service

TBD

### Pre-processed data useful for the LLM Companion - Solution Design

The LLM companion will be a natural language interface that allows non-technical users to get instant, factual insights from the data by simply asking questions. We must prepare the data to be useful for this purpose.

TBD
