"""tests/test_bronze_synth.py: the synthetic landings are what they claim to be.

The eager Landing is held to the real envelope in test_bronze_events.py. This file holds the lazy one to
the eager one, line for line, so the memory test measures the index over data shaped like the real thing.
"""
import gzip

import pytest

from bronze.index import build_index
from bronze.landing import LandingReader
from tests.bronze_synth import PREFIX, SOURCE_DB, Landing, LazyLanding, line_template

T, M = "merchant_transactions", "merchants"


def eager(lazy):
    land = Landing()
    for i in range(lazy.transactions):
        land.transaction(*lazy.changes(), date=lazy.date(i))
        land.flush()
    return land


@pytest.mark.parametrize("shape", [(3, 10, 2), (4, 7, 3), (2, 2000, 50)])
def test_the_lazy_landing_has_the_same_names_and_lines_as_the_eager_one(shape):
    lazy = LazyLanding(shape[0], tx_size=shape[1], merchants=shape[2])
    land = eager(lazy)
    assert lazy.list_keys(PREFIX) == sorted(land.sink.objects)
    for key in lazy.list_keys(PREFIX):
        assert gzip.decompress(lazy.read(key)) == gzip.decompress(land.sink.objects[key]), key


def test_the_lazy_landing_holds_nothing_and_lists_by_prefix():
    lazy = LazyLanding(5, tx_size=10, merchants=2)
    assert len(lazy.list_keys(PREFIX)) == 10 and lazy.list_keys("cdc/other/") == []
    assert lazy.read(lazy.list_keys(PREFIX)[0]) == lazy.read(lazy.list_keys(PREFIX)[0])
    assert vars(lazy).keys() == {"transactions", "tx_size", "merchants", "prefix", "events", "_names"}


def test_two_transactions_share_a_day_and_the_days_run_on_from_the_first_of_january():
    assert [LazyLanding.date(i) for i in range(5)] == ["2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02", "2025-01-03"]
    assert LazyLanding.date(600) == "2025-10-28"


def test_the_index_over_a_lazy_landing_has_the_counts_it_was_built_with():
    lazy = LazyLanding(6, tx_size=100, merchants=5)
    summary = build_index(LandingReader(lazy, PREFIX), source_db=SOURCE_DB).summary()
    assert (summary.objects, summary.events_read, summary.unique_events, summary.transactions) == (12, 600, 600, 6)
    assert summary.by_table[T]["c"] == 570 and summary.by_table[M]["c"] == 30
    assert summary.duplicates == 0 and summary.incomplete == 0 and lazy.events == 600


def test_a_line_template_has_a_place_for_every_value_that_changes():
    for table in (T, M):
        text = line_template(table, "c")
        for token in ("%(lsn)d", "%(change)d", "%(pos)d", "%(size)d", "%(xid)d", "%(serial)d", "%(date)s"):
            assert token in text, (table, token)
