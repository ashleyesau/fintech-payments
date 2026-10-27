"""tests/roundtrip.py: helpers for the round-trip tests.

The replay itself (blank database, `apply_events`, `in_commit_order`) now lives in cdc/reconcile.py, because the
definition of done runs it against the real bucket (M6-Q2); this module re-exports it and keeps the two helpers that
read a sink and check the strict order of a run with no crashes. The rows are rebuilt by Postgres itself from each
event's `after` image (json_populate_record), so a mistake in the decoder's conversion of a timestamp, a numeric or
an array shows up as a difference instead of being undone by a matching reverse conversion in the test.
"""
from cdc.dedup import events_in, key
from cdc.reconcile import apply_events, blank_database, in_commit_order  # noqa: F401  (the shipped implementation, re-exported)

def landed_events(sink) -> list[dict]:
    """Every event in every object, in object-name order (so not necessarily commit order, and with
    any duplicates that landed)."""
    return [event for name in sorted(sink.objects) for event in events_in(sink.objects[name])]


def partition_order_problems(sink) -> list[str]:
    """Contract section 7 on order: within a table and event date the objects' LSN ranges follow one another
    and the events across them ascend strictly by (commit_lsn, position). Returns what breaks it."""
    partitions: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for name in sink.objects:
        _, _, table, date_part, file = name.split("/")
        first = int(file.split("_")[1], 16)
        partitions.setdefault((table, date_part), []).append((first, name))
    problems = []
    for (table, date_part), objects in sorted(partitions.items()):
        previous = None
        for _, name in sorted(objects):
            events = events_in(sink.objects[name])
            keys = [key(e) for e in events]
            if keys != sorted(set(keys)):
                problems.append(f"{name}: events are not strictly ascending inside the object")
            if previous is not None and keys and keys[0] <= previous:
                problems.append(f"{name}: starts at {keys[0]}, not after the previous object's last {previous}")
            previous = keys[-1] if keys else previous
    return problems
