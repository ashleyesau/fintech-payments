#!/usr/bin/env bash
# The bronze_verify step of the Airflow DAG: run `python -m bronze.check` unless the landing has not changed
# since a run marker that already passed it.
#
#   LAST_VERIFIED   run id of the newest marker that passed bronze.check (empty if none is recorded)
#   FORCE_VERIFY    1 runs the check whatever is recorded
#   BRONZE_PYTHON   the pipeline interpreter (default /opt/pipeline-venv/bin/python)
#
# Exit 0: the check ran and passed. Exit 99: skipped (Airflow shows the task as skipped). Anything else: failed.
#
# "Unchanged" means the newest run marker is the one recorded as verified. bronze_write runs first in the DAG, so
# if anything new had landed, it would have written a newer marker, and the check would run. Landed objects are
# never overwritten and the Bronze writer cannot change or delete them (the same trust as the write step's skip).
# The check reads every landed object, so it is the slow part of a run; the marker costs one listing and one read.
# The recorded id lives in the Airflow Variable below. If it is lost, the check just runs again.
set -u

PYTHON="${BRONZE_PYTHON:-/opt/pipeline-venv/bin/python}"
VARIABLE="bronze_last_verified_marker"
LAST="${LAST_VERIFIED:-}"
FORCE="${FORCE_VERIFY:-0}"

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

NEWEST="$("$PYTHON" -m bronze.check --newest-marker)" || exit 1

if [ "$FORCE" = "1" ]; then
    echo "bronze_verify: force_verify is set, so every landed object is checked"
elif [ -n "$NEWEST" ] && [ "$NEWEST" = "$LAST" ]; then
    echo "bronze_verify skipped: the landing has not changed since run marker $NEWEST, which already passed bronze.check."
    echo "Trigger the DAG with force_verify set to true to check it again."
    exit 99
fi

"$PYTHON" -m bronze.check || exit $?

if [ -n "$NEWEST" ]; then
    if airflow variables set "$VARIABLE" "$NEWEST"; then
        echo "bronze_verify: run marker $NEWEST recorded as verified"
    else
        echo "warning: could not record run marker $NEWEST as verified; the next run will check again" >&2
    fi
fi
