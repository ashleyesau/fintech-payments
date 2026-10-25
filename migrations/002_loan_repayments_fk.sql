-- migrations/002_loan_repayments_fk.sql
-- Declare the foreign key from loan_repayments.loan_id to loan_applications.application_id.
--
-- Run with:
--   docker exec -i fintech_pg psql -U sim -d fintech < migrations/002_loan_repayments_fk.sql
--
-- In V1 a loan is its application: loan_id carries the application's ID, and no
-- separate loans table exists (Stream Field Provenance: loan_id is inherited from
-- the application). The FK therefore targets application_id.
--
-- Not covered: the FK cannot check that the application was approved and
-- disbursed. The generator only creates repayments for disbursed loans.
--
-- Write order: a loan_applications row must be committed before its first
-- loan_repayments row. Repayments start after disbursement, which is 0 to 3 days
-- after the application INSERT, so flushing loan_applications before
-- loan_repayments within each simulated day satisfies it.
--
-- merchant_id is still undeclared on every table: it cannot reference the
-- composite primary key (merchant_id, snapshot_at) on merchants.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'loan_repayments_loan_id_fkey'
          AND conrelid = 'loan_repayments'::regclass
    ) THEN
        ALTER TABLE loan_repayments
            ADD CONSTRAINT loan_repayments_loan_id_fkey
            FOREIGN KEY (loan_id) REFERENCES loan_applications (application_id);
    END IF;
END
$$;
