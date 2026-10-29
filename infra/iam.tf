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
