"""tests/pgcluster.py: a throwaway Postgres cluster with a small WAL cap, for the lost-slot tests.

max_slot_wal_keep_size is passed to the server as a `-c` flag, and a flag overrides ALTER SYSTEM, so a
slot can only be lost on purpose by starting a cluster with a small cap. This starts one from the local
Postgres binaries (initdb and pg_ctl, found on PATH or under /usr/lib/postgresql), on a free port and in
a temporary directory, as the `postgres` user when the tests run as root (the sandbox). No binaries:
the tests skip, except under CI, where they fail (decision D9: skipping would keep CI green without
running them).
"""
import contextlib
import glob
import os
import pwd
import shlex
import shutil
import socket
import subprocess
import tempfile
from pathlib import Path

import psycopg
import pytest

CAP = "16MB"
CAP_BYTES = 16 * 1024 * 1024


def find_bindir():
    """The directory holding initdb and pg_ctl, or None."""
    found = shutil.which("initdb")
    if found:
        return Path(found).parent
    candidates = sorted(glob.glob("/usr/lib/postgresql/*/bin/initdb"), key=lambda p: int(Path(p).parts[-3]))
    return Path(candidates[-1]).parent if candidates else None


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@contextlib.contextmanager
def small_cap_cluster(migrations):
    """Start the cluster, create a database from `migrations` and yield its DSN; stop and delete it afterwards."""
    bindir = find_bindir()
    if bindir is None:
        message = "no Postgres binaries (initdb, pg_ctl) found, so the small-cap cluster cannot be started"
        if os.environ.get("CI") == "true":
            pytest.fail(message + " (CI is set, so this fails rather than skips)")
        pytest.skip(message)
    root = os.geteuid() == 0
    base = Path(tempfile.mkdtemp(prefix="cdc_cap_"))
    if root:
        owner = pwd.getpwnam("postgres")
        os.chown(base, owner.pw_uid, owner.pw_gid)
    data, log, port = base / "data", base / "log", free_port()

    def pg(*args):
        command = [str(bindir / args[0]), *args[1:]]
        if root:
            command = ["su", "postgres", "-c", shlex.join(command)]
        return subprocess.run(command, capture_output=True, text=True, timeout=120)

    try:
        init = pg("initdb", "-D", str(data), "-U", "sim", "-A", "trust")
        assert init.returncode == 0, init.stderr
        options = (f"-c wal_level=logical -c port={port} -c listen_addresses=127.0.0.1 -c unix_socket_directories={base} "
                   f"-c max_replication_slots=4 -c max_wal_senders=4 -c max_slot_wal_keep_size={CAP} -c fsync=off")
        start = pg("pg_ctl", "-D", str(data), "-o", options, "-l", str(log), "-w", "start")
        assert start.returncode == 0, start.stderr + (log.read_text() if log.exists() else "")
        base_dsn = f"host=127.0.0.1 port={port} dbname=postgres user=sim password=x"
        with psycopg.connect(base_dsn, autocommit=True) as admin:
            admin.execute("CREATE DATABASE capdb")
        dsn = base_dsn.replace("dbname=postgres", "dbname=capdb")
        with psycopg.connect(dsn, autocommit=True) as setup:
            for path in migrations:
                setup.execute(path.read_text())
        yield dsn
    finally:
        pg("pg_ctl", "-D", str(data), "-m", "immediate", "-w", "stop")
        shutil.rmtree(base, ignore_errors=True)
