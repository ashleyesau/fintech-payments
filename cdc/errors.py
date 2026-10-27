"""
errors.py: what stops the consumer.

Every one of these means "stop without confirming the batch": the batch stays in the slot,
so nothing is lost (decisions D5 and D6, contract section 8).
"""


class CdcError(Exception):
    """Base class: the consumer must stop and leave the slot position where it is."""


class DecodeError(CdcError):
    """A pgoutput message is malformed, truncated, of an unknown kind, or holds a value the
    contract does not allow (an unchanged TOAST marker, a binary value, an unparseable
    timestamp, an integer outside the safe JSON range)."""


class UnknownTypeError(DecodeError):
    """A relation has a column whose type OID is not one of the eight the contract maps."""


class UnsupportedChange(CdcError):
    """A well-formed change the contract does not allow: a delete, a truncate, an update on
    a table other than loan_applications, or an update without the full old row."""
