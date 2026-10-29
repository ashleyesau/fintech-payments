terraform {
  required_version = ">= 1.17.1, < 1.18.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.9"
    }
  }

  # The state bucket was made by hand, once, outside Terraform (infra/README.md). A backend block cannot
  # use variables.
  backend "gcs" {
    bucket = "payments-512910-tfstate"
    prefix = "terraform/state"
  }
}
