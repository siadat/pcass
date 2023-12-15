import io
import os
import tempfile

import sstable.utils
import sstable.sstable_data
import sstable.sstable_statistics
import sstable.positioned_construct
import sstable.dump


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
    statistics_parsed = sstable.sstable_statistics.statistics_format.parse(statistics_bytes)
    statistics_bytes_got = sstable.sstable_statistics.statistics_format.build(statistics_parsed)
    assert statistics_bytes == statistics_bytes_got

    # Data.db
    with open("test_data/me-1-big-Data.db", "rb") as f:
        db_bytes = f.read()
    data_parsed = sstable.sstable_data.data_format.parse(db_bytes, sstable_statistics=statistics_parsed)
    data_bytes_got = sstable.sstable_data.data_format.build(data_parsed, sstable_statistics=statistics_parsed)
    assert db_bytes == data_bytes_got
    mock_writer = MockWriter()

    # dump
    with open(os.path.join("test_data", "me-1-big-Statistics.db"), "rb") as statistics_file:
        with open(os.path.join("test_data", "me-1-big-Data.db"), "rb") as data_file:
            sstable.dump.dump(statistics_file, data_file,  mock_writer)
    assert ['clustering_column_1'] == mock_writer.clustering_column_names
    assert ['aboutme'] == mock_writer.regular_column_names
    assert [b'\x00\x00\x00\x01', b'\x00\x00\x00\x02', b'\x00\x00\x00\x03'] == mock_writer.partition_key_value
    assert ['sina', 'soheil', 'sara'] == mock_writer.clustering_column_values
    assert ['hi my name is sina!', 'hi my name is soheil!', 'hi my name is sara!'] == mock_writer.regular_column_values

test_encode_and_decode()

sstable.utils.print_test_stats()
