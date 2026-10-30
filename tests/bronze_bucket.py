"""tests/bronze_bucket.py: the Bronze bucket as a directory, for the orphan tests.

The directory is also the warehouse root of a local Iceberg warehouse (tests/bronze_env.py Env), so bronze/<table>/data/...
and bronze/<table>/metadata/... are the real files PyIceberg wrote, and _runs/ and _catalog/ sit beside them as they do in
gs://fintech_bronze. BronzeStore, the real guard, is put over it, so the tests of the orphans command run the real
guard and the real marker and catalog copy code, not a fake of them. Test only; nothing here touches a bucket."""
import os
import shutil
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bronze.store import BronzeStore
from cdc.gcs import ObjectInfo
from tests.bronze_env import Env
from tests.file_sink import FileSink


class DirBucket(FileSink):
    """The calls GcsSink offers the store, over a directory. An object's creation time is its file's modification time."""

    def _paths(self):
        return [p for p in sorted(self.directory.rglob("*")) if p.is_file() and not (p.name.startswith(".") and p.name.endswith(".tmp"))]

    def _key(self, path: Path) -> str:
        return str(path.relative_to(self.directory))

    def list_keys(self, prefix: str) -> list[str]:
        return sorted(k for k in map(self._key, self._paths()) if k.startswith(prefix))

    def list_objects(self, prefix: str) -> list[ObjectInfo]:
        self.listings = getattr(self, "listings", 0) + 1
        return sorted((ObjectInfo(self._key(p), p.stat().st_size, datetime.fromtimestamp(p.stat().st_mtime, timezone.utc))
                       for p in self._paths() if self._key(p).startswith(prefix)), key=lambda info: info.key)

    def put_if_absent(self, key: str, data: bytes, content_type=None) -> bool:
        return super().put_if_absent(key, data)

    def overwrite(self, key: str, data: bytes, content_type=None) -> None:
        target = self.directory / key
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.parent / f".{target.name}.{os.getpid()}.tmp"
        temporary.write_bytes(data)
        os.replace(temporary, target)

    def delete(self, key: str) -> bool:
        self.deleted = getattr(self, "deleted", []) + [key]
        try:
            (self.directory / key).unlink()
        except FileNotFoundError:
            return False
        return True


def bucket_env(path):
    """(Env, DirBucket, BronzeStore): a local warehouse whose root is also the bucket."""
    warehouse = path / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    bucket = DirBucket(warehouse)
    store = BronzeStore(bucket)
    return Env(path, store=store), bucket, store


def age(path: Path, delta: timedelta) -> Path:
    """Make a file `delta` old."""
    moment = (datetime.now(timezone.utc) - delta).timestamp()
    os.utime(path, (moment, moment))
    return path


def table_dir(env, table: str, folder: str) -> Path:
    return env.dir / "warehouse" / "bronze" / table / folder


def leave(env, table: str, kind: str, old: timedelta = timedelta(days=3)) -> str:
    """Plant what a crash leaves: a copy of a real file of that table under a new name. kind is one of
    'data' (written, never committed), 'manifest', 'manifest list' and 'metadata' (a metadata file with the number of the
    real one: the third crash point). Returns its key. The file is made `old` old."""
    data, metadata = table_dir(env, table, "data"), table_dir(env, table, "metadata")
    if kind == "data":
        source = sorted(data.rglob("*.parquet"))[0]
        target = source.parent / f"00000-9-{uuid.uuid4()}.parquet"
    elif kind == "manifest":
        source = sorted(p for p in metadata.glob("*.avro") if not p.name.startswith("snap-"))[0]
        target = metadata / f"{uuid.uuid4()}-m0.avro"
    elif kind == "manifest list":
        source = sorted(metadata.glob("snap-*.avro"))[0]
        target = metadata / f"snap-{uuid.uuid4().int % 10**18}-1-{uuid.uuid4()}.avro"
    elif kind == "metadata":
        source = sorted(metadata.glob("*.metadata.json"))[-1]
        target = metadata / f"{source.name.split('-')[0]}-{uuid.uuid4()}.metadata.json"
    else:
        raise ValueError(kind)
    shutil.copy(source, target)
    age(target, old)
    return str(target.relative_to(env.dir / "warehouse"))
