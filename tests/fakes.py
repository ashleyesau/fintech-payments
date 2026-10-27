"""tests/fakes.py: stand-ins for the parts of the consumer that talk to the outside world.

MemorySink follows the same rules as the real sink (cdc/sink.py): create if absent, never
overwrite, safe from several threads. It also fails on demand, which is why it exists: the
delivery tests need "the 4th upload fails" and "the process dies before the confirm", and the
real bucket cannot do that on request, costs free-tier operations, and adds network noise.
"""
import threading
from typing import Callable, Optional

from cdc.errors import SinkError, SlotLost
from cdc.lsn import format_lsn
from cdc.reader import SlotStatus


class MemorySink:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.attempts = 0   # put_if_absent calls, counting the ones that failed
        self.reads = 0
        self.fail_put: Optional[Callable[[int, str], bool]] = None   # (attempt number from 1, key)
        self.fail_read: Optional[Callable[[str], bool]] = None
        self._lock = threading.Lock()

    def put_if_absent(self, key: str, data: bytes) -> bool:
        with self._lock:
            self.attempts += 1
            if self.fail_put is not None and self.fail_put(self.attempts, key):
                raise SinkError(f"injected failure writing {key}")
            if key in self.objects:
                return False
            self.objects[key] = data
            return True

    def read(self, key: str) -> bytes:
        with self._lock:
            self.reads += 1
            if self.fail_read is not None and self.fail_read(key):
                raise SinkError(f"injected failure reading {key}")
            if key not in self.objects:
                raise SinkError(f"{key} does not exist")
            return self.objects[key]


class SimulatedCrash(Exception):
    """The process dying at an awkward moment (for example just before the confirm)."""


class FakeReader:
    """A reader over a fixed list of captured messages, following the real reader's rules: a fetch
    returns whole transactions (at least `limit` messages unless the slot has fewer) that are not
    yet confirmed, repeats the Relation messages seen so far (Postgres sends them again on each
    read), only a position that was fetched can be confirmed, and going backwards is an error.

    `messages` is a list of (lsn, bytes) in slot order, a Begin to a Commit per transaction.
    `on_confirm` runs just before a confirm takes effect (to look at what had landed by then);
    `fail_confirm` raises instead. `statuses` is a list of SlotStatus to return in turn (the last
    one repeats); by default the slot is healthy.
    """

    def __init__(self, messages, slot_name="cdc_fake", statuses=None, on_confirm=None, fail_confirm=None,
                 max_fetches=100):
        self.slot_name = slot_name
        self.max_fetches = max_fetches     # a loop that never advances fails the test instead of hanging it
        self.position = 0
        self.fetches = 0
        self.confirmed: list[int] = []
        self.on_confirm, self.fail_confirm = on_confirm, fail_confirm
        self._statuses = list(statuses or [])
        self._read_to = 0
        self._transactions = []          # (end_lsn, [messages], relations known before it)
        relations: dict[int, tuple[int, bytes]] = {}
        current = []
        for lsn, data in messages:
            current.append((lsn, data))
            if data[:1] == b"R":
                relations[int.from_bytes(data[1:5], "big")] = (lsn, data)
            if data[:1] == b"C":
                self._transactions.append((lsn, current, dict(relations)))
                current = []
        assert not current, "the messages must end with a Commit"

    def fetch(self, limit):
        self.fetches += 1
        if self.fetches > self.max_fetches:
            raise AssertionError(f"more than {self.max_fetches} fetches: the loop is not making progress")
        rows = []
        for end_lsn, group, known in self._transactions:
            if end_lsn <= self.position:
                continue
            if not rows:
                own = {int.from_bytes(d[1:5], "big") for _, d in group if d[:1] == b"R"}
                rows += [m for oid, m in known.items() if oid not in own]
            rows += group
            if len(rows) >= limit:
                break
        if rows:
            self._read_to = max(self._read_to, rows[-1][0])
        return rows

    def confirm(self, end_lsn):
        if end_lsn > self._read_to:
            raise RuntimeError(f"cannot confirm {format_lsn(end_lsn)}: only read up to {format_lsn(self._read_to)}")
        if end_lsn < self.position:
            raise RuntimeError("cannot advance replication slot backwards")
        if self.on_confirm is not None:
            self.on_confirm(end_lsn)
        if self.fail_confirm is not None:
            raise self.fail_confirm
        self.position = end_lsn
        self.confirmed.append(end_lsn)

    def status(self):
        if not self._statuses:
            return SlotStatus("reserved", 3_000_000_000, 3_221_225_472, self.position, self.position)
        return self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]

    def lost_error(self):
        return SlotLost(f"Slot {self.slot_name} is lost (fake)")

