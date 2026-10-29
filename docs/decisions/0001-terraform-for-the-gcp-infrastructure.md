# 0001. Terraform for the Google Cloud infrastructure

Status: accepted, 2026-10-29. Applies to everything in `infra/`.

## Context

The pipeline stands on a small amount of Google Cloud: the bucket the CDC consumer lands objects in
(`cdc_landing`), the bucket the Bronze Iceberg tables will live in (`fintech_bronze`), and the service
accounts and permissions around them. Both buckets and the `bronze-writer` account were made by hand while
the pipeline was built. Settings that the design depends on (private buckets, soft delete off, one role per
account) lived only in the console and in notes, and nothing would show if one drifted.

Soft delete is off on both data buckets, so a deleted or overwritten object is gone. The landed objects in
`gs://cdc_landing/cdc/cdc_r01/` are the project's deliverable.

## Decision

Describe the buckets, the two service accounts and their bindings as Terraform in `infra/`, import what
already exists without changing it, and add the `cdc-consumer` account as new code.

- **One root configuration.** No modules and no workspaces: there is one project and about ten resources.
- **Remote state** in `gs://payments-512910-tfstate`, made once with two `gcloud` commands (documented in
  `infra/README.md`) and not managed by Terraform, because a bucket cannot hold the state that creates it.
- **Import, then verify.** The existing resources were imported from a saved plan that had to read exactly
  "6 to import, 0 to add, 0 to change, 0 to destroy", and a second plan had to say "No changes". Only then
  were the import blocks deleted.
- **Both data buckets have `prevent_destroy`, `deletion_policy = "PREVENT"`** and `force_destroy = false`. The first is
  code, so removing it or the block drops it; the second is kept in the state and still refuses a delete; the third
  protects only a bucket that holds objects. A plan that would destroy or
  replace either is never accepted.
- **IAM only through `_member` resources**, never `_binding` or `_policy`, so Terraform cannot remove a
  binding it was not told about (both buckets keep the default legacy project bindings).
- **Every setting that differs from a provider default is stated**, for example soft delete 0 seconds
  (the provider default is 7 days), so the first plan was imports only.
- **`cdc-consumer` gets `objectCreator` and `objectViewer` on `cdc_landing`, and nothing else.** The consumer
  writes with "create only if absent" and, when the object already exists, reads it back to compare. That read
  needs `storage.objects.get`, which `objectCreator` does not include. The account can never update or delete
  an object, so a landed object cannot be overwritten through it.
- **Terraform runs from the administrator's own login**, with `GOOGLE_APPLICATION_CREDENTIALS` unset. If the
  variable were set to the `bronze-writer` file, Terraform would run as that account.
- **CI checks the code, not the cloud.** A `terraform` job runs `fmt -check`, `init -backend=false` with the
  lock file read only, and `validate` on Terraform 1.17.1. It has no credentials. Nothing is planned or
  applied in CI.
- **Every apply is a saved plan** whose summary line is checked before it is applied, and each apply needs an
  explicit go-ahead for that plan.

## Alternatives considered

- **Keep the hand-made setup and write it down.** Cheaper, but a note cannot show drift.
- **`gcloud` scripts.** They describe steps, not the state that should hold, and have no plan.
- **OpenTofu.** Not needed: Terraform 1.17.1 runs on the Intel MacBook Air (macOS 12) that the project is
  built on.
- **A bootstrap configuration for the state bucket.** It would need its own state, which is the same problem
  one level down.
- **Authoritative IAM (`_binding`, `_policy`).** Rejected for the removal risk above.
- **A plan or apply in CI.** Needs cloud credentials in GitHub for a project of this size. Rejected.

## Consequences

- A plan on the Air now shows any change made outside Terraform. Nothing runs it on a schedule, so drift is seen
  only when someone runs a plan.
- CI proves the code is formatted and valid and that the lock file can install the provider on a Linux runner.
  It does not prove the real infrastructure matches the code.
- `cdc_landing` carries three encryption enforcement settings that were not part of the original design:
  customer-supplied keys are refused for new objects, set two minutes after the bucket was created. Their
  cause is unknown. The code states them as found, because the alternative is changing the landing bucket to
  quieten a plan.
- `deletion_policy = "PREVENT"` was added to the buckets after the import was clean, so the import plan stayed
  free of changes. `gcloud` and the console can still delete a bucket.
- The CDC consumer runs as `cdc-consumer` by impersonation (since 2026-10-29): it needs a credentials file in its own
  gcloud directory, a `CDC_PROJECT` setting (the file carries no project) and a guard that refuses to start without
  `GOOGLE_APPLICATION_CREDENTIALS`. The guard proves a file is named, not whose it is, and `python -m cdc.permissions`
  asks Google what the account may do. The administrator's own login stays unrestricted for any command without a guard.
- A real smoke run as the account (83 objects created, then a replay that found all 83 already there) is what showed
  `objectCreator` and `objectViewer` are enough. A first run of the smoke test on code without the guard silently used
  the administrator's login and landed the objects as that, which is why the guard exists.
