# IAM is only ever added with _member resources, never _binding or _policy, so Terraform cannot remove a
# binding it was not told about (the legacy project bindings on both buckets stay as they are).

resource "google_service_account" "bronze_writer" {
  project      = var.project
  account_id   = "bronze-writer"
  display_name = "Bronze writer"
}

resource "google_storage_bucket_iam_member" "bronze_writer_reads_landing" {
  bucket = google_storage_bucket.cdc_landing.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.bronze_writer.member
}

resource "google_storage_bucket_iam_member" "bronze_writer_writes_bronze" {
  bucket = google_storage_bucket.fintech_bronze.name
  role   = "roles/storage.objectUser"
  member = google_service_account.bronze_writer.member
}

resource "google_service_account_iam_member" "admin_impersonates_bronze_writer" {
  service_account_id = google_service_account.bronze_writer.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.admin_user}"
}

# The CDC consumer's account. It creates landed objects and reads one back after an "already exists"
# answer (the replay compare), and that is all: objectCreator has no update or delete, and objectViewer
# is read only. No project role, nothing on fintech_bronze, no Postgres access. It must never be given
# update or delete, so a landed object can never be overwritten. The consumer does not run as this
# account until stage 6; creating it changes nothing at run time.
resource "google_service_account" "cdc_consumer" {
  project      = var.project
  account_id   = "cdc-consumer"
  display_name = "CDC consumer"
}

resource "google_storage_bucket_iam_member" "cdc_consumer_creates_landing" {
  bucket = google_storage_bucket.cdc_landing.name
  role   = "roles/storage.objectCreator"
  member = google_service_account.cdc_consumer.member
}

resource "google_storage_bucket_iam_member" "cdc_consumer_reads_landing" {
  bucket = google_storage_bucket.cdc_landing.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.cdc_consumer.member
}

resource "google_service_account_iam_member" "admin_impersonates_cdc_consumer" {
  service_account_id = google_service_account.cdc_consumer.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.admin_user}"
}

# The Lightdash reader account. It runs queries against BigQuery on behalf of Lightdash Cloud.
# bigquery.jobUser allows it to create query jobs at the project level; bigquery.dataViewer on
# the marts dataset alone limits table reads to marts only. It authenticates via a service account
# key file held by Lightdash Cloud (not impersonation).
resource "google_service_account" "lightdash_reader" {
  project      = var.project
  account_id   = "lightdash-reader"
  display_name = "Lightdash reader"
}

resource "google_project_iam_member" "lightdash_reader_bq_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.lightdash_reader.member
}

resource "google_bigquery_dataset_iam_member" "lightdash_reader_marts_viewer" {
  project    = var.project
  dataset_id = "marts"
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.lightdash_reader.member
}

# pipeline-runner: runs register_bronze_bq.py, dbt run and dbt test inside the Airflow DAG.
# bigquery.jobUser allows query jobs at the project level.
# dataEditor on bronze allows CREATE OR REPLACE EXTERNAL TABLE (register_bq step).
# dataEditor on staging and marts allows dbt to create and update all model layers.
resource "google_service_account" "pipeline_runner" {
  project      = var.project
  account_id   = "pipeline-runner"
  display_name = "Pipeline runner (register_bq + dbt)"
}

resource "google_project_iam_member" "pipeline_runner_bq_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.pipeline_runner.member
}

resource "google_bigquery_dataset_iam_member" "pipeline_runner_bronze_editor" {
  project    = var.project
  dataset_id = "bronze"
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline_runner.member
}

resource "google_bigquery_dataset_iam_member" "pipeline_runner_staging_editor" {
  project    = var.project
  dataset_id = "staging"
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline_runner.member
}

resource "google_bigquery_dataset_iam_member" "pipeline_runner_marts_editor" {
  project    = var.project
  dataset_id = "marts"
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.pipeline_runner.member
}

resource "google_service_account_iam_member" "admin_impersonates_pipeline_runner" {
  service_account_id = google_service_account.pipeline_runner.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:${var.admin_user}"
}
