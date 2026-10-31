# infra/scratch.tf: the throwaway bucket for the G4 cloud crash run (GCS Plan section 20.4 step 8).
#
# This is TEST INFRASTRUCTURE ONLY. It must never hold production data and must be destroyed after
# step 8 is done. It is kept in its own file so the `--target` flags in the apply and destroy
# commands are visually obvious and touch nothing else in the state.
#
# Apply (two resources, no data at risk):
#   terraform plan -target=google_storage_bucket.bronze_crash_test \
#                  -target=google_storage_bucket_iam_member.bronze_writer_crash_test_user \
#                  -out=scratch_apply.tfplan
#   # review, show Ashley the plan, get her go
#   terraform apply scratch_apply.tfplan
#
# Run the step 8 crash tests with:
#   BRONZE_BUCKET=payments-512910-bronze-crash-test \
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/bronze_credentials.json python -m bronze.main --write
#   (and the injection tests, and destroy below)
#
# Destroy (only on Ashley's go after the crash run is done and the bucket is confirmed empty):
#   terraform plan -destroy \
#                  -target=google_storage_bucket.bronze_crash_test \
#                  -target=google_storage_bucket_iam_member.bronze_writer_crash_test_user \
#                  -out=scratch_destroy.tfplan
#   # review, show Ashley the plan, get her go
#   terraform apply scratch_destroy.tfplan

resource "google_storage_bucket" "bronze_crash_test" {
  name          = "${var.project}-bronze-crash-test"
  project       = var.project
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Must be destroyable; the real data buckets have force_destroy = false and deletion_policy = PREVENT.
  force_destroy = true

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  # No lifecycle { prevent_destroy = true } here: this bucket is intentionally temporary.
}

resource "google_storage_bucket_iam_member" "bronze_writer_crash_test_user" {
  bucket = google_storage_bucket.bronze_crash_test.name
  role   = "roles/storage.objectUser"
  member = google_service_account.bronze_writer.member
}

output "crash_test_bucket_name" {
  description = "The throwaway bucket for the G4 cloud crash run. Set BRONZE_BUCKET to this value."
  value       = google_storage_bucket.bronze_crash_test.name
}
