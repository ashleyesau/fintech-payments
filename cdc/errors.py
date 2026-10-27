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


class SchemaDriftError(CdcError):
    """A table's columns differ from the column list of its Pydantic model (decision D6), or the
    table is not one of the five. The fix is a new migration, an updated model and a deliberate
    decision, never an envelope change (contract section 10)."""


class SinkError(CdcError):
    """The sink could not write or read an object after its own retries (exit code 3). The batch
    is not confirmed, so the next run replays it."""


class ObjectMismatch(CdcError):
    """An object already exists under the name this batch would write, and its events are not
    the events this batch holds (exit code 2). The same name means the same changes, so this is
    an integrity failure: the batch boundaries or the data changed. Never overwrite."""
