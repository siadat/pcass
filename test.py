import io
import tempfile

import utils
import sstable_db
import sstable_statistics
import positioned_construct


def test(struct, db_filepath):
    with open(db_filepath, "rb") as f:
        original_bytes = f.read()
    parsed = struct.parse(original_bytes)
    got = struct.build(parsed)
    utils.assert_equal(original_bytes, got)


test(sstable_db.db_format, "test_data/simple-3-rows-me-1-big-Data.db")
test(sstable_statistics.statistics_format, "test_data/simple-3-rows-me-1-big-Statistics.db")

utils.print_test_stats()
