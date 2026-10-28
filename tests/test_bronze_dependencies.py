"""tests/test_bronze_dependencies.py: G1 step 5, the Bronze pins install and work.

Nothing in G1 uses PyIceberg yet. These tests are the first proof, on every machine that runs the suite
(CI on Linux, the Intel Air on macOS 12, the sandbox), that the exact set verified in G0 is what is
installed and that the pieces which differ by platform (pyarrow's native code, pyiceberg-core's Rust
transforms) run. They import at the top and never skip: a missing or broken wheel is an error, in CI
too (GCS Plan (V2) section 15, acceptance 9).
"""
import datetime
import re
from importlib.metadata import version
from pathlib import Path

import pyarrow as pa
import pyarrow.fs
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import DateType, LongType, NestedField, StringType

ROOT = Path(__file__).resolve().parent.parent
PINNED = {"pyiceberg": "0.12.0", "pyiceberg-core": "0.10.1", "pyarrow": "26.0.0"}


def requirements() -> dict[str, str]:
    """name -> specifier, from requirements.txt (comments and blank lines left out)."""
    found = {}
    for line in (ROOT / "requirements.txt").read_text().splitlines():
        line = line.split("#")[0].strip()
        match = re.match(r"^([A-Za-z0-9_.\-]+)(\[.*\])?(.*)$", line)
        if match:
            found[match.group(1).lower().replace("_", "-")] = match.group(3).strip()
    return found


@pytest.mark.parametrize("name,pin", PINNED.items())
def test_the_pins_are_exact_in_requirements_txt(name, pin):
    assert requirements()[name] == f"=={pin}"


def test_sqlalchemy_is_bounded_the_way_pyiceberg_bounds_it_for_its_sqlite_catalog():
    assert requirements()["sqlalchemy"] == ">=2.0.18,<3"


@pytest.mark.parametrize("name,pin", PINNED.items())
def test_the_installed_version_is_the_pinned_one(name, pin):
    assert version(name) == pin


def test_the_installed_sqlalchemy_satisfies_the_bound():
    major, minor, patch = (int(part) for part in re.match(r"(\d+)\.(\d+)\.(\d+)", version("sqlalchemy")).groups())
    assert (2, 0, 18) <= (major, minor, patch) < (3, 0, 0)


def test_pyarrow_has_the_gcs_filesystem_the_writer_will_use():
    assert hasattr(pyarrow.fs, "GcsFileSystem")


def test_a_month_partitioned_table_is_written_and_read_back_through_a_sqlite_catalog(tmp_path):
    """The G0 probe in miniature, local only: create, append across two months, read back. The month
    transform runs in pyiceberg-core's native code, which is what differs between platforms."""
    catalog = SqlCatalog("bronze", uri=f"sqlite:///{tmp_path / 'catalog.db'}", warehouse=f"file://{tmp_path / 'warehouse'}")
    catalog.create_namespace("bronze")
    schema = Schema(NestedField(1, "id", LongType()), NestedField(2, "event_date", DateType()),
                    NestedField(3, "note", StringType()))
    spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="event_month"))
    table = catalog.create_table("bronze.t", schema=schema, partition_spec=spec,
                                 properties={"write.parquet.compression-codec": "zstd"})
    rows = pa.table({"id": pa.array([1, 2, 3], pa.int64()),
                     "event_date": pa.array([datetime.date(2025, 1, 31), datetime.date(2025, 2, 1), datetime.date(2025, 2, 28)]),
                     "note": ["a", "b", "c"]})
    table.append(rows)
    back = catalog.load_table("bronze.t").scan().to_arrow().sort_by("id")
    assert back.column("id").to_pylist() == [1, 2, 3]
    assert back.column("event_date").to_pylist() == rows.column("event_date").to_pylist()
    assert back.column("note").to_pylist() == ["a", "b", "c"]
    assert len(list(table.scan().plan_files())) == 2     # January and February: two partitions, two data files
