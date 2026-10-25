"""
simulator/models.py: Pydantic V2 event models for all five streams.

One class per stream, mirroring the Event Schemas canonical contract exactly.
All generated event dicts are validated against these models before Postgres
insert. Enums are Literal types so type-checkers catch invalid values at edit
time.

Cross-field rules enforced here:
  - EFT-only timestamp fields must be None for card channels.
  - auth_code must be None when status is 'failed'.
  - decline_code must be None when status is 'completed'.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator

# Channels that use the card path (EFT-only fields must be None).
_CARD_CHANNELS = {"card_present", "card_ecom", "payment_link"}

# Fields that only apply to EFT transactions.
_EFT_ONLY_FIELDS = (
    "bank_selected",
    "bank_selected_at",
    "login_succeeded_at",
    "otp_requested_at",
    "otp_entered_at",
)


# ---------------------------------------------------------------------------
# Stream 1: merchant_transactions
# ---------------------------------------------------------------------------

class MerchantTransaction(BaseModel):
    transaction_id: str
    merchant_id: str
    event_type: Literal["payment", "refund", "chargeback", "void"]
    channel: Literal["card_present", "card_ecom", "instant_eft", "payment_link", "payshap"]
    status: Literal[
        "initiated", "authorized", "completed", "failed", "refunded",
        "bank_selected", "login_succeeded", "otp_requested", "otp_entered",
        "abandoned", "expired",
    ]
    entry_mode: Optional[str] = None
    payment_method: Optional[str] = None
    bank_selected: Optional[str] = None
    amount_minor: int = Field(gt=0)
    currency: Literal["ZAR"] = "ZAR"
    interchange_fee_minor: int = Field(default=0, ge=0)
    scheme_fee_minor: int = Field(default=0, ge=0)
    acquirer_markup_minor: int = Field(default=0, ge=0)
    mcc: Optional[str] = None
    auth_code: Optional[str] = None
    decline_code: Optional[str] = None
    risk_score: Optional[int] = Field(default=None, ge=0, le=100)
    risk_action: Optional[Literal["allow", "review", "block"]] = None
    created_at: datetime
    bank_selected_at: Optional[datetime] = None
    login_succeeded_at: Optional[datetime] = None
    otp_requested_at: Optional[datetime] = None
    otp_entered_at: Optional[datetime] = None
    authorized_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    refunded_at: Optional[datetime] = None
    settlement_date: Optional[date] = None
    batch_id: Optional[str] = None
    device_id: Optional[str] = None
    pos_version: Optional[str] = None
    is_test: bool = False
    correlation_id: Optional[str] = None

    @model_validator(mode="after")
    def check_channel_consistency(self) -> "MerchantTransaction":
        if self.channel in _CARD_CHANNELS:
            populated = [f for f in _EFT_ONLY_FIELDS if getattr(self, f) is not None]
            if populated:
                raise ValueError(
                    f"EFT-only fields must be None for channel={self.channel!r}: {populated}"
                )
        if self.status == "failed" and self.auth_code is not None:
            raise ValueError("auth_code must be None when status is 'failed'")
        if self.status == "completed" and self.decline_code is not None:
            raise ValueError("decline_code must be None when status is 'completed'")
        return self


# ---------------------------------------------------------------------------
# Stream 2: loan_applications
# ---------------------------------------------------------------------------

class LoanApplication(BaseModel):
    application_id: str
    merchant_id: str
    applicant_user_id: str
    business_id: str
    application_channel: str
    submission_ts: datetime
    decision_ts: Optional[datetime] = None
    business_registration: Optional[str] = None
    months_trading: Optional[int] = Field(default=None, ge=0)
    declared_monthly_revenue: Optional[int] = Field(default=None, ge=0)
    requested_amount: int = Field(gt=0)
    requested_term_months: Literal[3, 6, 9, 12]
    owner_credit_score: Optional[int] = Field(default=None, ge=0, le=1000)
    owner_bureau: Optional[Literal["transunion", "xds", "experian"]] = None
    actioned_by: Optional[str] = None
    decision: Optional[Literal["approved", "declined", "manual_review", "counter_offer"]] = None
    decline_reasons: list[str] = Field(default_factory=list)
    approved_amount: Optional[int] = Field(default=None, ge=0)
    approved_term_months: Optional[int] = Field(default=None, ge=0)
    total_cost_percent: Optional[float] = Field(default=None, ge=0.0)
    # Null at INSERT time; populated via UPDATE when funds are disbursed.
    disbursed_at: Optional[datetime] = None
    disbursement_amount_minor: Optional[int] = Field(default=None, ge=0)
    model_version_id: Optional[str] = None
    fica_status: Optional[Literal["pass", "fail", "pending"]] = None


# ---------------------------------------------------------------------------
# Stream 3: loan_repayments
# ---------------------------------------------------------------------------

class LoanRepayment(BaseModel):
    repayment_id: str
    loan_id: str
    merchant_id: str
    installment_number: int = Field(ge=1)
    due_date: date
    payment_ts: Optional[datetime] = None
    principal_due: int = Field(ge=0)
    interest_due: int = Field(ge=0)
    total_due: int = Field(ge=0)
    amount_paid: int = Field(default=0, ge=0)
    outstanding_before: int = Field(ge=0)
    outstanding_after: int = Field(ge=0)
    debit_method: Optional[Literal["debicheck", "eft", "holdback_card"]] = None
    mandate_id: Optional[str] = None
    mandate_status: Optional[Literal["ACCP", "RJCT", "PDNG", "CANC"]] = None
    collection_status: Literal[
        "success", "unpaid_nsf", "unpaid_account_closed",
        "unpaid_no_authority", "disputed", "reversed", "partial",
    ]
    response_code: Optional[str] = None
    retry_attempt: int = Field(default=0, ge=0)
    days_past_due: int = Field(default=0, ge=0)
    par_bucket: Literal["current", "1-30", "31-60", "61-90", "90+"] = "current"
    is_cured: bool = False
    write_off_flag: bool = False


# ---------------------------------------------------------------------------
# Stream 4: risk_events
# ---------------------------------------------------------------------------

class RiskEvent(BaseModel):
    alert_id: str
    entity_type: Literal["transaction", "merchant", "loan"]
    entity_id: str
    merchant_id: str
    rule_id: str
    rule_type: Literal[
        "refund_spike", "velocity_breach", "chargeback_accumulation",
        "scheme_warning", "large_txn_flag", "otp_abuse",
    ]
    risk_score: int = Field(ge=0, le=100)
    risk_score_delta: int = 0
    action: Literal["allow", "flag_review", "block"]
    action_by: Optional[str] = None
    action_ts: datetime
    # Null for open/unresolved alerts; populated when reviewed.
    resolved_ts: Optional[datetime] = None
    outcome: Optional[Literal["cleared", "fraud", "false_positive"]] = None


# ---------------------------------------------------------------------------
# Stream 5 (dimension): merchants
# ---------------------------------------------------------------------------

class MerchantSnapshot(BaseModel):
    merchant_id: str
    archetype: str
    province: Literal[
        "gauteng", "western_cape", "kwazulu_natal", "eastern_cape",
        "limpopo", "mpumalanga", "north_west", "free_state", "northern_cape",
    ]
    is_metro: bool = False
    onboarded_at: datetime
    first_transaction_at: Optional[datetime] = None
    merchant_state: Literal[
        "new", "growing", "stable", "stressed", "delinquent", "churned"
    ] = "new"
    risk_score: int = Field(default=10, ge=0, le=100)
    # Stored as loan_count in Postgres (Event Schemas uses the legacy name mca_count).
    loan_count: int = Field(default=0, ge=0)
    has_active_loan: bool = False
    snapshot_at: datetime
