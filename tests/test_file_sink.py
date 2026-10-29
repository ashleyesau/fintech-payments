"""tests/test_file_sink.py: the directory sink's listing, which the Bronze rehearsal reads the landing through."""
from tests.file_sink import FileSink


def test_list_keys_gives_the_objects_under_the_prefix_sorted_and_nothing_else(tmp_path):
    sink = FileSink(tmp_path)
    for key in ("cdc/cdc_r01/b/x.ndjson.gz", "cdc/cdc_r01/a/y.ndjson.gz", "cdc/cdc_r02/a/z.ndjson.gz"):
        assert sink.put_if_absent(key, b"data")
    (tmp_path / "cdc/cdc_r01/a/.y.ndjson.gz.123.456.tmp").write_bytes(b"half written")
    (tmp_path / "cdc/cdc_r01/a/notes.txt").write_bytes(b"not an object")
    assert sink.list_keys("cdc/cdc_r01/") == ["cdc/cdc_r01/a/y.ndjson.gz", "cdc/cdc_r01/b/x.ndjson.gz"]
    assert sink.list_keys("cdc/cdc_r02/") == ["cdc/cdc_r02/a/z.ndjson.gz"]
    assert sink.list_keys("cdc/cdc_r03/") == []


def test_list_keys_agrees_with_what_the_reader_can_read(tmp_path):
    sink = FileSink(tmp_path)
    sink.put_if_absent("cdc/cdc_r01/t/obj.ndjson.gz", b"abc")
    assert [sink.read(key) for key in sink.list_keys("cdc/cdc_r01/")] == [b"abc"]
