import io
import subprocess
import tempfile

import utils
import sstable_construct
import positioned_construct

def test(db_filepath):
    parsed = sstable_construct.format.parse_stream(open(db_filepath, "rb"))
    got = sstable_construct.format.build(parsed)
    want = open(db_filepath, "rb").read()
    utils.assert_equal(want, got)

test("test_data/simple-3-rows-me-1-big-Data.db")
