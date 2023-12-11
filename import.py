import io
import os
import box
import utils

import dump
import sstable_data
import sstable_statistics

# import Python object and print its sstable byte representation

def main():
    statistics_dict = {
        "metadata_count": 1,
        "toc": [
            {
                "type": sstable_statistics.SERIALIZATION_METADATA,
                "offset": (1 + 2) * 4,
            },
        ],
        "validation_metadata": None,
        "compaction_metadata": None,
        "statistics_metadata": None,
        "serialization_header": {
            "min_timestamp": 0,
            "min_local_deletion_time": 0,
            "min_ttl": 0,
            "partition_key_type": {
                "name_length": 41,
                "name": "org.apache.cassandra.db.marshal.Int32Type",
            },
            "clustering_key_count": 0,
            "clustering_key_types": [],
            "static_column_count": 0,
            "static_columns": [],
            "regular_column_count": 1,
            "regular_columns": [
                {
                    "name_length": 4,
                    "name": "col1",
                    "type": {
                        "name_length": 41,
                        "name": "org.apache.cassandra.db.marshal.Int32Type",
                    },
                },
            ],
        },
    }
    statistics_bytes = sstable_statistics.statistics_format.build(statistics_dict)
    statistics_got = sstable_statistics.statistics_format.parse(statistics_bytes)
    utils.assert_equal(1, statistics_got.metadata_count)

    print("Statistics.db:\t", statistics_bytes)

    row_body_format = sstable_data.row_body(0)
    row_body1 = {
        "row_body_start": 0,
        "previous_unfiltered_size": 0,
        "timestamp_diff": 0,
        "cells": [
            {
                "cell_flags": 0x08,
                "cell": {
                    "cell_value": 42,
                },
            },
        ],
    }
    row_body1_size = len(row_body_format.build(row_body1, sstable_statistics=statistics_got))
    data_bytes = sstable_data.data_format.build({
            "partitions": [
                {
                    "partition_header": {
                        "key_len": 2,
                        "key": {
                            "cell_value": 0x01,
                        },
                        "deletion_time": {
                            "local_deletion_time": 0,
                            "marked_for_delete_at": 0,
                        },
                    },
                    "unfiltereds": [
                        box.Box({
                            "row_flags": 0x24,
                            "row": {
                                "clustering_block": None,
                                "serialized_row_body_size": row_body1_size,
                                "row_body": row_body1,
                            },
                        }),
                        # we need to box this, because the lambda in the
                        # construct's conditional structs references it like
                        # `.row_flags`, not like `["row_flags"]`
                        box.Box({
                            "row_flags": 0x01,
                            "row": None,
                        }),
                    ],
                },
            ],
        },
        # We also need to box statistics_dict because it is used like an object, not like a dict.
        sstable_statistics=box.Box(statistics_dict),
    )
    data_got = sstable_data.data_format.parse(data_bytes, sstable_statistics=statistics_got)
    utils.assert_equal(2, data_got.partitions[0].partition_header.key_len)

    print("Data.db:\t", data_bytes)

    dump.dump(
        io.BytesIO(statistics_bytes),
        io.BytesIO(data_bytes),
        dump.JsonWriter(os.sys.stdout),
    )

    with open(f"out/me-1-big-Statistics.db", "wb") as f:
        f.write(statistics_bytes)
    with open(f"out/me-1-big-Data.db", "wb") as f:
        f.write(data_bytes)

main()
