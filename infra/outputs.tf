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

output "cdc_consumer_email" {
  description = "The service account the CDC consumer will run as, by impersonation (stage 6)."
  value       = google_service_account.cdc_consumer.email
}

output "lightdash_reader_email" {
  description = "The service account Lightdash Cloud uses to query BigQuery marts."
  value       = google_service_account.lightdash_reader.email
}
