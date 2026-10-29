variable "project" {
  description = "The Google Cloud project that holds everything here. Every resource pins it."
  type        = string
  default     = "payments-512910"
}

variable "region" {
  description = "The region of the buckets."
  type        = string
  default     = "us-east1"
}

variable "admin_user" {
  description = "The person who may impersonate the service accounts. An email address is not a secret."
  type        = string
  default     = "ashley.esau@gmail.com"
}
