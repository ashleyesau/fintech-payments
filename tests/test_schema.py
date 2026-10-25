"""
tests/test_schema.py: Schema compliance tests for all five Pydantic event models.

Validates that the models enforce the Event Schemas canonical contract:
field types, required fields, enum constraints, and cross-field rules.
Phase 8 will extend these with generated-data volume tests.
"""
import pytest
from pydantic import ValidationError

from simulator.models import (
    LoanApplication,
    LoanRepayment,
    MerchantSnapshot,
    MerchantTransaction,
    RiskEvent,
)

# ---------------------------------------------------------------------------
# Fixtures: minimal valid payloads
# ---------------------------------------------------------------------------

CARD_TXN = {
    "transaction_id": "txn-card-001",
    "merchant_id": "merch-001",
    "event_type": "payment",
    "channel": "card_ecom",
    "status": "completed",
    "entry_mode": "ecom",
    "payment_method": "mastercard",
    "amount_minor": 2599,
    "interchange_fee_minor": 32,
    "scheme_fee_minor": 8,
    "acquirer_markup_minor": 30,
    "auth_code": "A1B2C3",
    "risk_score": 15,
    "risk_action": "allow",
    "created_at": "2026-05-10T09:15:27Z",
    "authorized_at": "2026-05-10T09:15:45Z",
    "completed_at": "2026-05-10T09:15:50Z",
    "settlement_date": "2026-05-12",
    "batch_id": "batch-2026-05-12-001",
}

EFT_TXN = {
    "transaction_id": "txn-eft-001",
    "merchant_id": "merch-001",
    "event_type": "payment",
    "channel": "instant_eft",
    "status": "completed",
    "payment_method": "eft",
    "bank_selected": "FNB",
    "amount_minor": 5000,
    "created_at": "2026-05-10T09:15:27Z",
    "bank_selected_at": "2026-05-10T09:15:35Z",
    "login_succeeded_at": "2026-05-10T09:15:40Z",
    "otp_requested_at": "2026-05-10T09:15:42Z",
    "otp_entered_at": "2026-05-10T09:15:44Z",
    "authorized_at": "2026-05-10T09:15:45Z",
    "completed_at": "2026-05-10T09:15:50Z",
}

LOAN_APP = {
    "application_id": "app-001",
    "merchant_id": "merch-001",
    "applicant_user_id": "user-001",
    "business_id": "biz-001",
    "application_channel": "web",
    "submission_ts": "2026-05-01T10:00:00Z",
    "requested_amount": 1000000,
    "requested_term_months": 6,
}

REPAYMENT = {
    "repayment_id": "rep-001",
    "loan_id": "loan-001",
    "merchant_id": "merch-001",
    "installment_number": 1,
    "due_date": "2026-06-01",
    "principal_due": 1300000,
    "interest_due": 70000,
    "total_due": 1370000,
    "outstanding_before": 10000000,
    "outstanding_after": 8630000,
    "collection_status": "success",
}

RISK_EVENT = {
    "alert_id": "alert-001",
    "entity_type": "transaction",
    "entity_id": "txn-card-001",
    "merchant_id": "merch-001",
    "rule_id": "rule-velocity-1h",
    "rule_type": "velocity_breach",
    "risk_score": 64,
    "risk_score_delta": 12,
    "action": "flag_review",
    "action_ts": "2026-05-10T09:15:46Z",
}

SNAPSHOT = {
    "merchant_id": "merch-001",
    "archetype": "spaza_shop",
    "province": "gauteng",
    "onboarded_at": "2025-09-12T10:00:00Z",
    "snapshot_at": "2026-05-15T23:59:00Z",
}


# ---------------------------------------------------------------------------
# MerchantTransaction
# ---------------------------------------------------------------------------

def test_card_transaction_valid():
    txn = MerchantTransaction(**CARD_TXN)
    assert txn.channel == "card_ecom"
    assert txn.status == "completed"
    assert txn.bank_selected is None


def test_eft_transaction_valid():
    txn = MerchantTransaction(**EFT_TXN)
    assert txn.channel == "instant_eft"
    assert txn.bank_selected == "FNB"
    assert txn.bank_selected_at is not None


def test_card_transaction_rejects_eft_fields():
    """EFT-only timestamp fields must be None for card channels."""
    bad = {**CARD_TXN, "bank_selected_at": "2026-05-10T09:15:35Z"}
    with pytest.raises(ValidationError, match="EFT-only fields"):
        MerchantTransaction(**bad)


def test_card_transaction_rejects_bank_selected():
    bad = {**CARD_TXN, "bank_selected": "FNB"}
    with pytest.raises(ValidationError, match="EFT-only fields"):
        MerchantTransaction(**bad)


def test_failed_transaction_rejects_auth_code():
    bad = {**CARD_TXN, "status": "failed", "auth_code": "X99"}
    with pytest.raises(ValidationError, match="auth_code must be None"):
        MerchantTransaction(**bad)


def test_completed_transaction_rejects_decline_code():
    bad = {**CARD_TXN, "decline_code": "51"}
    with pytest.raises(ValidationError, match="decline_code must be None"):
        MerchantTransaction(**bad)


def test_transaction_rejects_zero_amount():
    bad = {**CARD_TXN, "amount_minor": 0}
    with pytest.raises(ValidationError):
        MerchantTransaction(**bad)


def test_transaction_rejects_invalid_channel():
    bad = {**CARD_TXN, "channel": "crypto"}
    with pytest.raises(ValidationError):
        MerchantTransaction(**bad)


def test_transaction_rejects_out_of_range_risk_score():
    bad = {**CARD_TXN, "risk_score": 150}
    with pytest.raises(ValidationError):
        MerchantTransaction(**bad)


# ---------------------------------------------------------------------------
# LoanApplication
# ---------------------------------------------------------------------------

def test_loan_application_valid():
    app = LoanApplication(**LOAN_APP)
    assert app.disbursed_at is None
    assert app.disbursement_amount_minor is None


def test_loan_application_rejects_invalid_term():
    bad = {**LOAN_APP, "requested_term_months": 7}
    with pytest.raises(ValidationError):
        LoanApplication(**bad)


def test_loan_application_rejects_invalid_decision():
    bad = {**LOAN_APP, "decision": "maybe"}
    with pytest.raises(ValidationError):
        LoanApplication(**bad)


# ---------------------------------------------------------------------------
# LoanRepayment
# ---------------------------------------------------------------------------

def test_loan_repayment_valid():
    rep = LoanRepayment(**REPAYMENT)
    assert rep.retry_attempt == 0
    assert rep.par_bucket == "current"


def test_loan_repayment_rejects_invalid_collection_status():
    bad = {**REPAYMENT, "collection_status": "bounced"}
    with pytest.raises(ValidationError):
        LoanRepayment(**bad)


def test_loan_repayment_rejects_invalid_par_bucket():
    bad = {**REPAYMENT, "par_bucket": "120+"}
    with pytest.raises(ValidationError):
        LoanRepayment(**bad)


# ---------------------------------------------------------------------------
# RiskEvent
# ---------------------------------------------------------------------------

def test_risk_event_valid():
    event = RiskEvent(**RISK_EVENT)
    assert event.resolved_ts is None
    assert event.outcome is None


def test_risk_event_rejects_invalid_rule_type():
    bad = {**RISK_EVENT, "rule_type": "unknown_rule"}
    with pytest.raises(ValidationError):
        RiskEvent(**bad)


def test_risk_event_rejects_invalid_action():
    bad = {**RISK_EVENT, "action": "ignore"}
    with pytest.raises(ValidationError):
        RiskEvent(**bad)


def test_risk_event_rejects_out_of_range_score():
    bad = {**RISK_EVENT, "risk_score": -1}
    with pytest.raises(ValidationError):
        RiskEvent(**bad)


# ---------------------------------------------------------------------------
# MerchantSnapshot
# ---------------------------------------------------------------------------

def test_merchant_snapshot_valid():
    snap = MerchantSnapshot(**SNAPSHOT)
    assert snap.merchant_state == "new"
    assert snap.loan_count == 0
    assert snap.has_active_loan is False


def test_merchant_snapshot_rejects_invalid_province():
    bad = {**SNAPSHOT, "province": "limpopo_north"}
    with pytest.raises(ValidationError):
        MerchantSnapshot(**bad)


def test_merchant_snapshot_rejects_invalid_state():
    bad = {**SNAPSHOT, "merchant_state": "active"}
    with pytest.raises(ValidationError):
        MerchantSnapshot(**bad)


def test_merchant_snapshot_rejects_out_of_range_risk_score():
    bad = {**SNAPSHOT, "risk_score": 101}
    with pytest.raises(ValidationError):
        MerchantSnapshot(**bad)
