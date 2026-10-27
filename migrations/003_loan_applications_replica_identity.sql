-- migrations/003_loan_applications_replica_identity.sql
-- Publish the full old row for updates to loan_applications.
--
-- Run with (against any database that will have a CDC slot, such as fintech_cdc):
--   docker exec -i fintech_pg psql -U sim -d fintech_cdc < migrations/003_loan_applications_replica_identity.sql
--
-- loan_applications is the only table the generator updates (the disbursement sets
-- disbursed_at and disbursement_amount_minor). With the default replica identity a logical
-- decoding update message carries only the new row, because the primary key does not change.
-- REPLICA IDENTITY FULL makes it carry the old row too, so a CDC consumer gets a true
-- "before" image and can tell a replayed disbursement (before equal to after) from a real one.
--
-- The other four tables are INSERT-only and keep the default identity: FULL there would add
-- WAL and change nothing. This changes no column, constraint or index, and it is not an
-- expand-contract example.
--
-- Applying it again is harmless: setting the same identity twice is a no-op.

ALTER TABLE loan_applications REPLICA IDENTITY FULL;
