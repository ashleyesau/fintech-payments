# Infrastructure (Terraform)

The Google Cloud pieces the pipeline stands on, as code: the two data buckets and the `bronze-writer` service
account with its bindings. They already existed (made by hand while the pipeline was built), so this
configuration imports them without changing them. Plan: the Infrastructure as Code Plan page in Notion, a child of
the GCS Plan (V2). The decisions are in DECISION_LOG.

## What it manages

| Resource | What it is |
|---|---|
| `google_storage_bucket.cdc_landing` | `gs://cdc_landing`: the raw landing zone. US-EAST1, standard, uniform access, public access prevention enforced, soft delete off. Customer-supplied encryption keys are refused for new objects (stated as the inventory found it) |
| `google_storage_bucket.fintech_bronze` | `gs://fintech_bronze`: the Bronze Iceberg tables. Same settings, no encryption enforcement fields |
| `google_service_account.bronze_writer` | The account the Bronze job runs as, by impersonation (no key file) |
| `google_storage_bucket_iam_member` x2 | bronze-writer: `objectViewer` on `cdc_landing`, `objectUser` on `fintech_bronze` |
| `google_service_account_iam_member` | The administrator may impersonate bronze-writer (`serviceAccountTokenCreator`) |
| `google_service_account.cdc_consumer` | The account the CDC consumer will run as, by impersonation. Not in use until the consumer is switched to it |
| `google_storage_bucket_iam_member` x2 | cdc-consumer: `objectCreator` and `objectViewer` on `cdc_landing` only. It can create a landed object and read one back after an "already exists" answer; it can never update or delete one. No project role, nothing on `fintech_bronze` |
| `google_service_account_iam_member` | The administrator may impersonate cdc-consumer (`serviceAccountTokenCreator`) |

Both buckets carry `lifecycle { prevent_destroy = true }` and `force_destroy = false`. **Never replace or destroy
either bucket.** `prevent_destroy` stops Terraform, not `gcloud` or the console. Terraform manages no objects:
the landed objects in `gs://cdc_landing/cdc/cdc_r01/` are the deliverable and are never written to or deleted.

IAM is added only through `_member` resources, never `_binding` or `_policy`, so Terraform cannot remove a
binding it was not told about (both buckets keep the default legacy project bindings).

## Running it

Terraform is pinned to 1.17.1 (the official `darwin_amd64` binary; it runs on the Intel Air under macOS 12) and the
Google provider to `~> 8.9`. Use Ashley's own login, never the bronze-writer file:

```
cd infra
unset GOOGLE_APPLICATION_CREDENTIALS
terraform init
terraform plan
```

Every `gcloud` command in this project carries `--project=payments-512910`. A `terraform apply` or `destroy` is
run only on an explicit go-ahead for that plan; a plan that destroys or replaces a bucket is never accepted.

## The state bucket

State lives in `gs://payments-512910-tfstate` (us-east1, uniform access, public access prevention enforced,
versioning on, soft delete at the 7 day default). It was made once, by hand, because a bucket cannot hold the
state that creates it, and is not managed by Terraform:

```
gcloud storage buckets create gs://payments-512910-tfstate --project=payments-512910 --location=us-east1 --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update gs://payments-512910-tfstate --project=payments-512910 --versioning
```

The backend is in `versions.tf`. The state is small and can be rebuilt by importing the resources again. A crashed
run can leave a lock object under `terraform/state/`; `terraform force-unlock` with the lock id releases it.

## How it was imported

The six resources above were imported into the state on 2026-10-29 with `import` blocks, from a saved plan whose
summary was exactly "6 to import, 0 to add, 0 to change, 0 to destroy". The apply changed no real infrastructure,
and a second plan said "No changes". The import blocks were then deleted. The state holds the imported IAM
members' bucket as `b/cdc_landing` and `b/fintech_bronze`; the plan does not treat that as a difference. If a
later plan ever says one of those members "must be replaced", stop and do not apply it.

## Not here yet

Running the CDC consumer as `cdc-consumer` (the account exists but the consumer still uses the administrator's own
login) and the CI check are later stages of the plan.
