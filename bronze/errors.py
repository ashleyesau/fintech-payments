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


class RoundTripError(ContractError):
    """A landed event did not come back equal from the pass 2 conversion (events_to_arrow, then to_pylist, then
    row_to_event): Bronze would not hold what landed. The message names the object, the event number, the table
    and the first field that differs, with what was expected and found."""


class TableDriftError(ContractError):
    """An existing Bronze table differs from what the contract generates: a column, a field id, a type or a
    required flag, the partition spec, one of the four table properties, or the location (bronze/tables.py).
    Raised at the start of a run, before pass 1 and before any write, so a table nobody should have changed
    is never appended to. The message names the table and every difference (the first ten are shown)."""


class DuplicateBronzeKeys(ContractError):
    """A Bronze table holds the same key (commit_lsn, transaction_position) in more than one row, though the
    contract allows one row per key (bronze/present.py). Appending on top of it would make it worse, so the run
    stops. The message names the table, how many rows repeat an earlier row's key and the first such key."""


class UnknownBronzeKeys(ContractError):
    """A Bronze table holds a key that the landing does not hold for that table (bronze/present.py): an object
    was deleted from the landing, or the warehouse was built from another landing prefix, or the table was
    written by something else. The message names the table, how many rows and the first key."""


class LandingChanged(ContractError):
    """What pass 2 read is not what pass 1 indexed (bronze/run.py): an object that parsed in pass 1 no longer
    does, holds an event the index does not know, or gives a different number of rows than the index said it
    should. The landing is immutable and the Bronze account cannot write to it, so this means the objects
    changed under the run or the index is wrong. The message names the table or object and how many batches
    were already committed; a re-run starts from what Bronze then holds."""


class MarkerError(ContractError):
    """A run marker (bronze/marker.py) cannot be trusted or does not fit what is there: an object under _runs/ that
    is not a marker or whose content does not follow Bronze contract section 8, a marker name already taken, a
    marker that names a table or snapshot the catalog does not have (the catalog is behind the marker: rebuild it
    from the marker, do not create the tables again), or a rebuild whose registered tables do not match the marker.
    The message names the marker and what is wrong."""
