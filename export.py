import csv
import io
import os
import argparse

import sstable_db
import sstable_statistics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dir', type=str)
    args = parser.parse_args()

    with open(os.path.join(args.dir, "me-1-big-Statistics.db"), "rb") as f:
        parsed_statistics = sstable_statistics.statistics_format.parse_stream(f)
    with open(os.path.join(args.dir, "me-1-big-Data.db"), "rb") as f:
        parsed_data = sstable_db.db_format.parse_stream(f, sstable_statistics=parsed_statistics)


    # header:
    clustering_column_names = [f"clustering_column_{i+1}" for i, typ in enumerate(parsed_statistics.serialization_header.clustering_key_types)]
    regular_column_names = [column.name for column in parsed_statistics.serialization_header.regular_columns]

    writer = csv.writer(os.sys.stdout)
    writer.writerow([f"partition_key"] + list(clustering_column_names) + list(regular_column_names))

    for partition in parsed_data.partitions:
        partition_key = partition.partition_header.key.cell_value
        for unfiltered in partition.unfiltereds:
            if unfiltered.row_flags & 0x01:
                continue
            clustering_column_values = map(lambda cell: cell.key.cell_value, unfiltered.row.clustering_block.clustering_cells)
            regular_column_values = map(lambda cell: cell.cell.cell_value, unfiltered.row.cells)
            writer.writerow([partition_key] + list(clustering_column_values) + list(regular_column_values))
    # print(parsed_statistics)

main()
