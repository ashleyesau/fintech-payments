"""tests/file_sink.py: a sink that writes a directory, for the one test that kills a real process.

Test-only (the product writes no local files, decision D3). It follows the Sink rules: the object is
written to a temporary name and then linked into place, so it appears whole or not at all and an
existing name is never overwritten; a process killed mid-write leaves only a temporary file that no
object name points to. `delay` makes each upload slow, so a test has time to kill the process in the
middle of a flush.
"""
import os
import time
from pathlib import Path

from cdc.errors import SinkError


class FileSink:
    def __init__(self, directory, delay: float = 0.0) -> None:
        self.directory, self.delay = Path(directory), delay

    def put_if_absent(self, key: str, data: bytes) -> bool:
        if self.delay:
            time.sleep(self.delay)
        target = self.directory / key
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.parent / f".{target.name}.{os.getpid()}.{time.monotonic_ns()}.tmp"
        temporary.write_bytes(data)
        try:
            os.link(temporary, target)        # atomic, and fails if the name is taken
        except FileExistsError:
            return False
        finally:
            temporary.unlink()
        return True

    def read(self, key: str) -> bytes:
        try:
            return (self.directory / key).read_bytes()
        except FileNotFoundError as error:
            raise SinkError(f"{key} does not exist") from error

    def list_keys(self, prefix: str) -> list[str]:
        """The names of the landed objects under `prefix`, sorted, as GcsSink.list_keys gives them: what
        LandingReader needs. A temporary file of a write in progress is not an object and is not listed."""
        found = (str(path.relative_to(self.directory)) for path in self.directory.rglob("*.ndjson.gz"))
        return sorted(key for key in found if key.startswith(prefix))

    def objects(self) -> dict[str, bytes]:
        return {str(path.relative_to(self.directory)): path.read_bytes()
                for path in sorted(self.directory.rglob("*.ndjson.gz"))}
