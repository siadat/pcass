import io
import tempfile

import utils
import sstable_construct
import positioned_construct


def test(db_filepath):
    with open(db_filepath, "rb") as f:
        original_bytes = f.read()
    parsed = sstable_construct.format.parse(original_bytes)
    got = sstable_construct.format.build(parsed)
    utils.assert_equal(original_bytes, got)


test("test_data/simple-3-rows-me-1-big-Data.db")
utils.print_test_stats()
