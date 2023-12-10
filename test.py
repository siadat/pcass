import io
import tempfile

import utils
import sstable_data
import sstable_statistics
import positioned_construct
import dump


class MockWriter():
    def __init__(self):
        self.partition_key_value = []
        self.clustering_column_names = []
        self.clustering_column_values = []
        self.regular_column_names = []
        self.regular_column_values = []
    def write_header(self, clustering_column_names, regular_column_names):
        self.clustering_column_names = clustering_column_names
        self.regular_column_names = regular_column_names
    def write_row(self, partition_key_value, clustering_column_values, regular_column_values):
        self.partition_key_value.append(partition_key_value)
        self.clustering_column_values += clustering_column_values
        self.regular_column_values += regular_column_values

def test_encode_and_decode():
    # Statistics.db
    with open("test_data/me-1-big-Statistics.db", "rb") as f:
        statistics_bytes = f.read()
    statistics_parsed = sstable_statistics.statistics_format.parse(statistics_bytes)
    statistics_bytes_got = sstable_statistics.statistics_format.build(statistics_parsed)
    utils.assert_equal(statistics_bytes, statistics_bytes_got)

    # Data.db
    with open("test_data/me-1-big-Data.db", "rb") as f:
        db_bytes = f.read()
    data_parsed = sstable_data.data_format.parse(db_bytes, sstable_statistics=statistics_parsed)
    data_bytes_got = sstable_data.data_format.build(data_parsed, sstable_statistics=statistics_parsed)
    utils.assert_equal(db_bytes, data_bytes_got)
    mock_writer = MockWriter()

    # dump
    dump.dump("test_data", mock_writer)
    utils.assert_equal(['clustering_column_1'], mock_writer.clustering_column_names)
    utils.assert_equal(['aboutme'], mock_writer.regular_column_names)
    utils.assert_equal([1, 2, 3], mock_writer.partition_key_value)
    utils.assert_equal(['sina', 'soheil', 'sara'], mock_writer.clustering_column_values)
    utils.assert_equal(['hi my name is sina!', 'hi my name is soheil!', 'hi my name is sara!'], mock_writer.regular_column_values)

test_encode_and_decode()

utils.print_test_stats()
