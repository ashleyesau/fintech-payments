# Both buckets exist already and are imported (imports.tf), not created. The attributes below are what the
# read-only inventory of 2026-10-29 showed; soft delete is set to 0 explicitly because the provider's
# default is 7 days. prevent_destroy is on both: they hold the landed objects and the Bronze tables.
# The three encryption enforcement settings on cdc_landing are stated as the inventory printed them
# (customer-supplied keys refused for new objects). Never change them to make a plan quiet.

resource "google_storage_bucket" "cdc_landing" {
  name                        = "cdc_landing"
  project                     = var.project
  location                    = "US-EAST1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  encryption {
    customer_managed_encryption_enforcement_config {
      restriction_mode = "NotRestricted"
    }

    customer_supplied_encryption_enforcement_config {
      restriction_mode = "FullyRestricted"
    }

    google_managed_encryption_enforcement_config {
      restriction_mode = "NotRestricted"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_storage_bucket" "fintech_bronze" {
  name                        = "fintech_bronze"
  project                     = var.project
  location                    = "US-EAST1"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle {
    prevent_destroy = true
  }
}
