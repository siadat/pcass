import io
import tempfile

import utils
import sstable_db
import sstable_statistics
import positioned_construct


def test_encode_and_decode():
    # Statistics.db
    with open("test_data/simple-3-rows-me-1-big-Statistics.db", "rb") as f:
        statistics_bytes = f.read()
    statistics_parsed = sstable_statistics.statistics_format.parse(statistics_bytes)
    statistics_bytes_got = sstable_statistics.statistics_format.build(statistics_parsed)
    utils.assert_equal(statistics_bytes, statistics_bytes_got)

    # Data.db
    with open("test_data/simple-3-rows-me-1-big-Data.db", "rb") as f:
        db_bytes = f.read()
    data_parsed = sstable_db.db_format.parse(db_bytes, sstable_statistics=statistics_parsed)
    data_bytes_got = sstable_db.db_format.build(data_parsed, sstable_statistics=statistics_parsed)
    utils.assert_equal(db_bytes, data_bytes_got)


test_encode_and_decode()

utils.print_test_stats()
