import json
import csv
import io
import os
import argparse

import sstable.utils
import sstable.sstable_data
import sstable.sstable_statistics


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        # If the object is bytes, decode it to a string
        if isinstance(obj, bytes):
            return "".join([sstable.utils.hex(byt) for byt in obj])
        # For all other types, use the standard handling
        return json.JSONEncoder.default(self, obj)


class CsvWriter:
    def __init__(self, writer):
        self.writer = csv.writer(writer)
    def write_header(self, clustering_column_names, regular_column_names):
        self.writer.writerow([f"partition_key_type"] + clustering_column_names + regular_column_names)
    def write_row(self, partition_key_value, clustering_column_values, regular_column_values):
        self.writer.writerow([partition_key_value] + clustering_column_values + regular_column_values)

class JsonWriter:
    def __init__(self, writer):
        self.writer = writer
    def write_header(self, clustering_column_names, regular_column_names):
        self.clustering_column_names = clustering_column_names
        self.regular_column_names = regular_column_names
    def write_row(self, partition_key_value, clustering_column_values, regular_column_values):
        # for value in regular_column_values:
        #     print(f"{type(value)} = {value}")


        # old:
        # print(json.dumps({
        #     "partition_key_value": partition_key_value,
        #     "clustering_column_values": clustering_column_values,
        #     "regular_column_values": regular_column_values,
        # }, cls=CustomJSONEncoder))

        # new:
        row = {
            "partition_key_value": partition_key_value,
            "cells": [],
        }
        for i in range(len(clustering_column_values)):
            row["cells"].append({
                "name": self.clustering_column_names[i],
                "value": clustering_column_values[i],
            })
            # row[self.clustering_column_names[i]] = clustering_column_values[i]
        for i in range(len(regular_column_values)):
            row["cells"].append({
                "name": self.regular_column_names[i],
                "value": regular_column_values[i],
            })
            # row[self.regular_column_names[i]] = regular_column_values[i]
        self.writer.write(json.dumps(row, cls=CustomJSONEncoder) + "\n")

def dump(statistics_stream, data_stream, writer):
    parsed_statistics = sstable.sstable_statistics.statistics_format.parse_stream(statistics_stream)
    parsed_data = sstable.sstable_data.data_format.parse_stream(data_stream, sstable_statistics=parsed_statistics)

    # header:
    clustering_column_names = [f"clustering_column_{i+1}" for i, typ in enumerate(parsed_statistics.serialization_header.clustering_key_types)]
    regular_column_names = [column.name for column in parsed_statistics.serialization_header.regular_columns]

    writer.write_header(list(clustering_column_names), list(regular_column_names))

    for partition in parsed_data.partitions:
        partition_key_value = partition.partition_header.key
        for unfiltered in partition.unfiltereds:
            if unfiltered.row_flags & 0x01:
                continue
            if unfiltered.row.clustering_block:
                clustering_column_values = map(lambda cell: cell.key.cell_value, unfiltered.row.clustering_block.clustering_cells)
            else:
                clustering_column_values = []

            # We check cell_flags to handle cells where the value is empty
            regular_column_values = map(lambda cell: cell.cell.cell_value if not cell.cell_flags & 0x04 else None, unfiltered.row.row_body.cells)
            writer.write_row(partition_key_value, list(clustering_column_values), list(regular_column_values))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', type=str)
    parser.add_argument('--format', type=str, default="json")
    args = parser.parse_args()

    if args.format == "csv":
        writer = CsvWriter(os.sys.stdout)
    else:
        writer = JsonWriter(os.sys.stdout)
    # dump(args.dir, writer)
    with open(os.path.join(args.dir, "me-1-big-Statistics.db"), "rb") as statistics_file:
        with open(os.path.join(args.dir, "me-1-big-Data.db"), "rb") as data_file:
            dump(statistics_file, data_file, writer)
