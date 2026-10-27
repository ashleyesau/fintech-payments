"""tests/consume_with_file_sink.py: `python -m tests.consume_with_file_sink DIRECTORY`, the consumer with a
directory for a sink, so a test can kill the real process. Settings come from the CDC_ environment
variables; FILE_SINK_DELAY (seconds per upload) slows it down."""
import os
import sys

from cdc.config import CdcSettings
from cdc.main import execute
from tests.file_sink import FileSink

if __name__ == "__main__":
    sink = FileSink(sys.argv[1], float(os.environ.get("FILE_SINK_DELAY", "0")))
    sys.exit(execute(CdcSettings(), follow=False, sink_factory=lambda settings: sink))
