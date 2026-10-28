"""
errors.py: what stops a Bronze run.

Every one of these means "stop with nothing committed to Bronze". The landed objects are never
touched by this package, so a stop never loses anything.
"""


class BronzeError(Exception):
    """Base class: the Bronze command must stop."""


class ConfigError(BronzeError):
    """A setting or credential the command needs is missing or unusable (exit code 1)."""


class ContractError(BronzeError):
    """What was landed breaks the CDC contract, so the run stops with nothing committed (exit code 2)."""


class SchemaError(BronzeError):
    """The Bronze schema cannot be generated from the model column lists: an integer column with no entry in
    the width table, a type with no Bronze mapping, a column name that clashes with another (case-insensitive)
    or an unknown table. Raised when a schema is built, so a change to a migration or a model fails at the
    point of use."""


class LandingError(ContractError):
    """The landing prefix holds something the contract does not allow, or a request would leave the
    prefix: an object name that does not follow contract section 2, a listed key outside the run's
    prefix, a read of a key outside it."""


class EventError(ContractError):
    """An event in a landed object breaks the envelope contract (section 3): not valid gzipped NDJSON,
    an unknown table, a wrong envelope_version, columns that differ from the model, a before image where
    the contract allows none, a transaction position outside its size, or an event that does not belong
    to the object that holds it. The message names the object and the event."""


class DisagreementError(ContractError):
    """The same change (commit_lsn, position) landed twice and the copies differ in more than ts_ms and
    ingested_at. Replays can only differ in the consumer's clock, so the landing contract was broken."""


class IncompleteTransactions(ContractError):
    """Some transaction is missing changes (positions 0 to size - 1, contract section 7): the consumer is
    mid-flush, or an object is missing. Writing now would leave Bronze with half a transaction."""


class MappingError(ContractError):
    """A value in an event (or a Bronze row) has the wrong shape for its column, a field is missing or
    unexpected, or two fields disagree (bronze/mapping.py). The message names the table, the column and
    what was expected and found, for example `merchant_transactions.created_at: expected a timestamp
    'YYYY-MM-DDTHH:MM:SS.ffffffZ', got "2025-03-01 10:00:00"`. `where` is a prefix that pass 1 sets to the
    object and event number before it re-raises, so the message says which line of which object."""

    def __init__(self, table, column: str, problem: str, *, field=None) -> None:
        self.table, self.column, self.problem, self.field, self.where = table, column, problem, field, ""
        super().__init__(self._text())

    def _text(self) -> str:
        subject = f"{self.table}.{self.column}" if self.table else self.column
        suffix = f" (event field {self.field})" if self.field else ""
        return f"{self.where}{subject}: {self.problem}{suffix}"

    def __str__(self) -> str:
        return self._text()
