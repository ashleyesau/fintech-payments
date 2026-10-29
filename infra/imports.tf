# One time. Applied once (stage 3), then this file is deleted.

import {
  to = google_storage_bucket.cdc_landing
  id = "payments-512910/cdc_landing"
}

import {
  to = google_storage_bucket.fintech_bronze
  id = "payments-512910/fintech_bronze"
}

import {
  to = google_service_account.bronze_writer
  id = "projects/payments-512910/serviceAccounts/bronze-writer@payments-512910.iam.gserviceaccount.com"
}

import {
  to = google_storage_bucket_iam_member.bronze_writer_reads_landing
  id = "b/cdc_landing roles/storage.objectViewer serviceAccount:bronze-writer@payments-512910.iam.gserviceaccount.com"
}

import {
  to = google_storage_bucket_iam_member.bronze_writer_writes_bronze
  id = "b/fintech_bronze roles/storage.objectUser serviceAccount:bronze-writer@payments-512910.iam.gserviceaccount.com"
}

import {
  to = google_service_account_iam_member.admin_impersonates_bronze_writer
  id = "projects/payments-512910/serviceAccounts/bronze-writer@payments-512910.iam.gserviceaccount.com roles/iam.serviceAccountTokenCreator user:ashley.esau@gmail.com"
}
