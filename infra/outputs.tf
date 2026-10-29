output "bucket_names" {
  description = "The two data buckets."

  value = {
    cdc_landing    = google_storage_bucket.cdc_landing.name
    fintech_bronze = google_storage_bucket.fintech_bronze.name
  }
}

output "bronze_writer_email" {
  description = "The service account the Bronze job runs as, by impersonation."
  value       = google_service_account.bronze_writer.email
}
