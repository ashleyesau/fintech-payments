"""tests/fakes.py: stand-ins for the parts of the consumer that talk to the outside world.

MemorySink follows the same rules as the real sink (cdc/sink.py): create if absent, never
overwrite, safe from several threads. It also fails on demand, which is why it exists: the
delivery tests need "the 4th upload fails" and "the process dies before the confirm", and the
real bucket cannot do that on request, costs free-tier operations, and adds network noise.
"""
import threading
from typing import Callable, Optional

from cdc.errors import SinkError


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
