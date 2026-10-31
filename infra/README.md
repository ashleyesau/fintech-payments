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

Both buckets carry `lifecycle { prevent_destroy = true }`, `deletion_policy = "PREVENT"` and `force_destroy = false`. **Never replace or destroy
either bucket.** `prevent_destroy` is code and goes if the block or the line is removed; `deletion_policy` is kept in the state and still makes a delete fail. Neither stops `gcloud` or the console. Terraform manages no objects:
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

## What CI checks

The `terraform` job in `.github/workflows/ci.yml` runs on every push, with Terraform 1.17.1 and no credentials:
`fmt -check -recursive -diff`, `init -backend=false -lockfile=readonly` and `validate`. The read only lock file
makes init fail if `.terraform.lock.hcl` lacks a hash for the runner, so it must hold `linux_amd64` as well as
`darwin_amd64` (made with `terraform providers lock -platform=darwin_amd64 -platform=linux_amd64`). CI never plans
or applies: a plan needs the state bucket and an apply needs a go-ahead, so both are run by hand on the Air.

## Running the CDC consumer as cdc-consumer

The consumer runs as `cdc-consumer` through an impersonated credentials file made once in its own gcloud configuration
directory (so the administrator's own login is untouched), and `cdc.main` refuses to start unless
`GOOGLE_APPLICATION_CREDENTIALS` names a file:

```
CLOUDSDK_CONFIG=$HOME/.config/gcloud_cdc gcloud auth application-default login --impersonate-service-account=cdc-consumer@payments-512910.iam.gserviceaccount.com --project=payments-512910
GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud_cdc/application_default_credentials.json python -m cdc.permissions
GOOGLE_APPLICATION_CREDENTIALS=$HOME/.config/gcloud_cdc/application_default_credentials.json python -m cdc.main --once
```

The file holds a refresh token for the administrator's login and no key; it stays outside the repo, mode 600. It carries
no Google project, so `CDC_PROJECT` supplies one. `python -m cdc.permissions` must exit 0: `cdc-consumer` may create, get
and list objects in `cdc_landing` and nothing else, and has nothing on `fintech_bronze`. The account cannot delete or
overwrite, so a throwaway prefix (for example a smoke test's) is removed with the administrator's own login, after
checking the object count, and never `cdc_r01`. Proof on the real bucket (2026-10-29): the 45-day smoke test landed 83
objects as `cdc-consumer`, and a replay from a restored slot found all 83 already there.

## Not here yet

Nothing of the original plan's stages is left. Not done: a scheduled plan to catch drift (a plan is run by hand), and the cause of
`cdc_landing`'s customer-supplied-key restriction (stated in code as found).

## Throwaway bucket for G4 step 8 (infra/scratch.tf)

`infra/scratch.tf` defines two resources for the G4 cloud crash run: the throwaway bucket
`payments-512910-bronze-crash-test` and bronze-writer's `objectUser` binding on it. It is kept in its own file
so the `-target` flags in the apply and destroy commands are obvious and touch nothing else in the state.

**Never apply or destroy without Ashley's go for that exact plan.**

Apply (two resources added, nothing changed or destroyed):
```
terraform plan -target=google_storage_bucket.bronze_crash_test \
               -target=google_storage_bucket_iam_member.bronze_writer_crash_test_user \
               -out=scratch_apply.tfplan
# review with Ashley, get her go
terraform apply scratch_apply.tfplan
```

Use the bucket by setting `BRONZE_BUCKET=payments-512910-bronze-crash-test` for the Bronze commands.
The `crash_test_bucket_name` output prints the exact name.

Destroy (only after the crash run and on Ashley's go, with the bucket confirmed empty or relying on `force_destroy = true`):
```
terraform plan -destroy \
               -target=google_storage_bucket.bronze_crash_test \
               -target=google_storage_bucket_iam_member.bronze_writer_crash_test_user \
               -out=scratch_destroy.tfplan
# review with Ashley, get her go
terraform apply scratch_destroy.tfplan
```

The bucket carries `force_destroy = true` (objects are deleted on destroy), soft delete off, no `prevent_destroy`
and no `deletion_policy = PREVENT`. The real data buckets are unaffected by these commands.
