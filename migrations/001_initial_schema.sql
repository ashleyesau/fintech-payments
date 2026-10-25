-- migrations/001_initial_schema.sql
-- Phase 1: Create the five V1 tables and the CDC publication.
--
-- Run with:
--   psql -h localhost -U sim -d fintech -f migrations/001_initial_schema.sql
--
-- Conventions (from Master Build Plan §architectural decisions):
--   - TEXT for UUIDs (not native UUID type in V1)
--   - BIGINT for all amounts (minor units / ZAR cents)
--   - TIMESTAMPTZ for all timestamps
--   - TEXT + CHECK for enums
--   - Hard FK constraints and CHECK constraints
--   - wal_level=logical set in docker-compose.yml

-- ---------------------------------------------------------------------------
-- 1. merchants (dimension snapshot, emitted daily by the generator)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id         TEXT        NOT NULL,
    archetype           TEXT        NOT NULL,
    province            TEXT        NOT NULL,
    is_metro            BOOLEAN     NOT NULL DEFAULT FALSE,
    onboarded_at        TIMESTAMPTZ NOT NULL,
    first_transaction_at TIMESTAMPTZ,
    merchant_state      TEXT        NOT NULL DEFAULT 'new',
    risk_score          INT         NOT NULL DEFAULT 10,
    loan_count          INT         NOT NULL DEFAULT 0,
    has_active_loan     BOOLEAN     NOT NULL DEFAULT FALSE,
    snapshot_at         TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (merchant_id, snapshot_at),

    CONSTRAINT merchants_state_check CHECK (
        merchant_state IN ('new','growing','stable','stressed','delinquent','churned')
    ),
    CONSTRAINT merchants_risk_score_check CHECK (risk_score BETWEEN 0 AND 100)
);

-- ---------------------------------------------------------------------------
-- 2. merchant_transactions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS merchant_transactions (
    transaction_id          TEXT        NOT NULL PRIMARY KEY,
    merchant_id             TEXT        NOT NULL REFERENCES merchants (merchant_id, snapshot_at) DEFERRABLE,
    event_type              TEXT        NOT NULL,
    channel                 TEXT        NOT NULL,
    status                  TEXT        NOT NULL,
    entry_mode              TEXT,
    payment_method          TEXT,
    bank_selected           TEXT,
    amount_minor            BIGINT      NOT NULL,
    currency                TEXT        NOT NULL DEFAULT 'ZAR',
    interchange_fee_minor   BIGINT      NOT NULL DEFAULT 0,
    scheme_fee_minor        BIGINT      NOT NULL DEFAULT 0,
    acquirer_markup_minor   BIGINT      NOT NULL DEFAULT 0,
    mcc                     TEXT,
    auth_code               TEXT,
    decline_code            TEXT,
    risk_score              INT,
    risk_action             TEXT,
    created_at              TIMESTAMPTZ NOT NULL,
    bank_selected_at        TIMESTAMPTZ,
    login_succeeded_at      TIMESTAMPTZ,
    otp_requested_at        TIMESTAMPTZ,
    otp_entered_at          TIMESTAMPTZ,
    authorized_at           TIMESTAMPTZ,
    completed_at            TIMESTAMPTZ,
    refunded_at             TIMESTAMPTZ,
    settlement_date         DATE,
    batch_id                TEXT,
    device_id               TEXT,
    pos_version             TEXT,
    is_test                 BOOLEAN     NOT NULL DEFAULT FALSE,
    correlation_id          TEXT,

    CONSTRAINT txn_event_type_check CHECK (
        event_type IN ('payment','refund','chargeback','void')
    ),
    CONSTRAINT txn_channel_check CHECK (
        channel IN ('card_present','card_ecom','instant_eft','payment_link','payshap')
    ),
    CONSTRAINT txn_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT txn_currency_check CHECK (currency = 'ZAR')
);

CREATE INDEX IF NOT EXISTS idx_txn_merchant_id ON merchant_transactions (merchant_id);
CREATE INDEX IF NOT EXISTS idx_txn_created_at  ON merchant_transactions (created_at);
CREATE INDEX IF NOT EXISTS idx_txn_settlement  ON merchant_transactions (settlement_date);

-- ---------------------------------------------------------------------------
-- 3. loan_applications
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS loan_applications (
    application_id              TEXT        NOT NULL PRIMARY KEY,
    merchant_id                 TEXT        NOT NULL,
    applicant_user_id           TEXT        NOT NULL,
    business_id                 TEXT        NOT NULL,
    application_channel         TEXT        NOT NULL,
    submission_ts               TIMESTAMPTZ NOT NULL,
    decision_ts                 TIMESTAMPTZ,
    business_registration       TEXT,
    months_trading              INT,
    declared_monthly_revenue    BIGINT,
    requested_amount            BIGINT      NOT NULL,
    requested_term_months       INT         NOT NULL,
    owner_credit_score          INT,
    owner_bureau                TEXT,
    actioned_by                 TEXT,
    decision                    TEXT,
    decline_reasons             TEXT[],
    approved_amount             BIGINT,
    approved_term_months        INT,
    total_cost_percent          NUMERIC(5,4),
    disbursed_at                TIMESTAMPTZ,
    disbursement_amount_minor   BIGINT,
    model_version_id            TEXT,
    fica_status                 TEXT,

    CONSTRAINT loan_app_decision_check CHECK (
        decision IS NULL OR decision IN ('approved','declined','manual_review','counter_offer')
    ),
    CONSTRAINT loan_app_fica_check CHECK (
        fica_status IS NULL OR fica_status IN ('pass','fail','pending')
    )
);

CREATE INDEX IF NOT EXISTS idx_loan_app_merchant ON loan_applications (merchant_id);
CREATE INDEX IF NOT EXISTS idx_loan_app_submission ON loan_applications (submission_ts);

-- ---------------------------------------------------------------------------
-- 4. loan_repayments
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS loan_repayments (
    repayment_id                    TEXT        NOT NULL PRIMARY KEY,
    loan_id                         TEXT        NOT NULL,
    merchant_id                     TEXT        NOT NULL,
    installment_number              INT         NOT NULL,
    due_date                        DATE        NOT NULL,
    payment_ts                      TIMESTAMPTZ,
    principal_due                   BIGINT      NOT NULL,
    interest_due                    BIGINT      NOT NULL,
    total_due                       BIGINT      NOT NULL,
    amount_paid                     BIGINT      NOT NULL DEFAULT 0,
    outstanding_before              BIGINT      NOT NULL,
    outstanding_after               BIGINT      NOT NULL,
    debit_method                    TEXT,
    mandate_id                      TEXT,
    mandate_status                  TEXT,
    collection_status               TEXT        NOT NULL,
    response_code                   TEXT,
    retry_attempt                   INT         NOT NULL DEFAULT 0,
    days_past_due                   INT         NOT NULL DEFAULT 0,
    par_bucket                      TEXT        NOT NULL DEFAULT 'current',
    is_cured                        BOOLEAN     NOT NULL DEFAULT FALSE,
    write_off_flag                  BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT repayment_collection_status_check CHECK (
        collection_status IN ('success','unpaid_nsf','unpaid_account_closed',
                              'unpaid_no_authority','disputed','reversed','partial')
    ),
    CONSTRAINT repayment_outstanding_check CHECK (outstanding_after >= 0),
    CONSTRAINT repayment_par_bucket_check CHECK (
        par_bucket IN ('current','1-30','31-60','61-90','90+')
    )
);

CREATE INDEX IF NOT EXISTS idx_repayment_loan_id    ON loan_repayments (loan_id);
CREATE INDEX IF NOT EXISTS idx_repayment_merchant   ON loan_repayments (merchant_id);
CREATE INDEX IF NOT EXISTS idx_repayment_due_date   ON loan_repayments (due_date);

-- ---------------------------------------------------------------------------
-- 5. risk_events
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS risk_events (
    alert_id            TEXT        NOT NULL PRIMARY KEY,
    entity_type         TEXT        NOT NULL,
    entity_id           TEXT        NOT NULL,
    merchant_id         TEXT        NOT NULL,
    rule_id             TEXT        NOT NULL,
    rule_type           TEXT        NOT NULL,
    risk_score          INT         NOT NULL,
    risk_score_delta    INT         NOT NULL DEFAULT 0,
    action              TEXT        NOT NULL,
    action_by           TEXT,
    action_ts           TIMESTAMPTZ NOT NULL,
    resolved_ts         TIMESTAMPTZ,
    outcome             TEXT,

    CONSTRAINT risk_entity_type_check CHECK (
        entity_type IN ('transaction','merchant','loan')
    ),
    CONSTRAINT risk_rule_type_check CHECK (
        rule_type IN ('refund_spike','velocity_breach','chargeback_accumulation',
                      'scheme_warning','large_txn_flag','otp_abuse')
    ),
    CONSTRAINT risk_action_check CHECK (
        action IN ('allow','flag_review','block')
    )
);

CREATE INDEX IF NOT EXISTS idx_risk_merchant ON risk_events (merchant_id);
CREATE INDEX IF NOT EXISTS idx_risk_action_ts ON risk_events (action_ts);

-- ---------------------------------------------------------------------------
-- CDC publication (ready for V2 consumer)
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'fintech_cdc'
    ) THEN
        CREATE PUBLICATION fintech_cdc FOR TABLE
            merchants,
            merchant_transactions,
            loan_applications,
            loan_repayments,
            risk_events;
        RAISE NOTICE 'Created publication fintech_cdc';
    ELSE
        RAISE NOTICE 'Publication fintech_cdc already exists';
    END IF;
END
$$;
