import box
import construct
import sstable.utils
import sstable.sstable_data

simple_cell_example = {
        "construct_struct": sstable.sstable_data.simple_cell,
        "bytes": b"\x08" # cell_flags
            + b"\x00\x00\x00\x01",
        "obj": construct.Container({
            "cell_flags": 0x08,
            "cell": construct.Container({
                "cell_value": 1,
            }),
        }),
        "parsing_kwargs": {
            "cell_index": 0,
            "missing_columns": None,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.Int32Type",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

simple_cell_third_column_example = {
        "construct_struct": sstable.sstable_data.simple_cell,
        "bytes": b"\x08" # cell_flags
            + b"\x00\x00\x00\x01",
        "obj": construct.Container({
            "cell_flags": 0x08,
            "cell": construct.Container({
                "cell_value": 1,
            }),
        }),
        "parsing_kwargs": {
            "cell_index": 2, # third column
            "missing_columns": None,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.AsciiType",
                            }),
                        }),
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.AsciiType",
                            }),
                        }),
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.Int32Type", # third column
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

complex_cell_list_item_example = {
        "construct_struct": sstable.sstable_data.complex_cell_item,
        "bytes": b"\x08" # cell_flags
                + b"\x10" # cell_path_length
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff" # cell_path
                + b"\x04" # cell_value_len
                + b"\x00\x00\x00\x01", # cell_value
        "obj": construct.Container({
            "cell_flags": 0x08,
            "cell": construct.Container({
                "cell_path_length": 16,
                "cell_path": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff",
                "cell_value_len": 4,
                "cell_value": construct.Container({
                    "cell_value": 1,
                }),
            }),
        }),
        "parsing_kwargs": {
            "cell_index": 0,
            "missing_columns": None,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

complex_cell_set_item_example = {
        "construct_struct": sstable.sstable_data.complex_cell_item,
        "bytes": b"\x0C" # cell_flags
                + b"\x04" # cell_value_len
                + b"\x00\x00\x00\x01", # cell_value
        "obj": construct.Container({
            # NOTE: For SetType the cell_flags has CellFlag.HAS_EMPTY_VALUE
            # however, I am encoding and decoding the value and interpreting it
            # as path being empty.
            "cell_flags": sstable.sstable_data.CellFlag.HAS_EMPTY_VALUE | sstable.sstable_data.CellFlag.USE_ROW_TIMESTAMP,
            "cell": construct.Container({
                "cell_val_len": 4,
                "cell_val": construct.Container({
                    "cell_value": 1,
                }),
            }),
        }),
        "parsing_kwargs": {
            "cell_index": 0,
            "missing_columns": None,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

complex_cell_set_of_boolean_item_example = {
        "construct_struct": sstable.sstable_data.complex_cell_item,
        "bytes": b"\x0C" # cell_flags
                + b"\x01" # cell_value_len
                + b"\x01", # cell_value
        "obj": construct.Container({
            # NOTE: For SetType the cell_flags has CellFlag.HAS_EMPTY_VALUE
            # however, I am encoding and decoding the value and interpreting it
            # as path being empty.
            "cell_flags": sstable.sstable_data.CellFlag.HAS_EMPTY_VALUE | sstable.sstable_data.CellFlag.USE_ROW_TIMESTAMP,
            "cell": construct.Container({
                "cell_val_len": 1,
                "cell_val": construct.Container({
                    "cell_value": 1,
                }),
            }),
        }),
        "parsing_kwargs": {
            "cell_index": 0,
            "missing_columns": None,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.BooleanType)",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }
complex_cell_list_example = {
        "construct_struct": sstable.sstable_data.complex_cell,
        "bytes": b"\x00" # delta_mark_for_delete_at
                + b"\x00" # delta_local_deletion_time
                + b"\x02" # items_count
                + complex_cell_list_item_example["bytes"] # items[0]
                + complex_cell_list_item_example["bytes"], # items[1]
        "obj": construct.Container({
            "complex_deletion_time": construct.Container({
                "delta_mark_for_delete_at": 0,
                "delta_local_deletion_time": 0,
            }),
            "items_count": 2,
            "items": [
                complex_cell_list_item_example["obj"],
                complex_cell_list_item_example["obj"],
            ],
        }),
        "parsing_kwargs": {
            "cell_index": 0,
            "missing_columns": None,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

complex_row_body_example = {
        "construct_struct": sstable.sstable_data.row_body_format,
        "bytes": b"\x00" # previous_unfiltered_size
                + b"\x00" # timestamp_diff
                + complex_cell_list_example["bytes"],
        "obj": construct.Container({
              "row_body_start": 0,
              "previous_unfiltered_size": 0,
              "timestamp_diff": 0,
              "missing_columns": None,
              "cells": [
                  complex_cell_list_example["obj"],
              ],
        }),
        "parsing_kwargs": {
            "overridden_row_flags": sstable.sstable_data.RowFlag.HAS_ALL_COLUMNS | sstable.sstable_data.RowFlag.HAS_COMPLEX_DELETION,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

simple_row_body_example = {
        "construct_struct": sstable.sstable_data.row_body_format,
        "bytes": b"\x00" # previous_unfiltered_size
                + b"\x00" # timestamp_diff
                + simple_cell_example["bytes"],
        "obj": construct.Container({
              "row_body_start": 0,
              "previous_unfiltered_size": 0,
              "timestamp_diff": 0,
              "missing_columns": None,
              "cells": [
                  simple_cell_example["obj"],
              ],
        }),
        "parsing_kwargs": {
            "overridden_row_flags": sstable.sstable_data.RowFlag.HAS_ALL_COLUMNS,
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.Int32Type",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

simple_unfiltered_example = {
        "construct_struct": sstable.sstable_data.unfiltered,
        "bytes": b"\x24" # row_flags
                + b"" # no clustering_block
                + bytes([len(simple_row_body_example["bytes"])]) # serialized_row_body_size
                + simple_row_body_example["bytes"],
        "obj": construct.Container({
              "row_flags": sstable.sstable_data.RowFlag.HAS_ALL_COLUMNS | sstable.sstable_data.RowFlag.HAS_TIMESTAMP,
              "row": {
                  "clustering_block": None,
                  "serialized_row_body_size": len(simple_row_body_example["bytes"]),
                  "row_body": {**simple_row_body_example["obj"], "row_body_start": 2},
              },
        }),
        "parsing_kwargs": {
            "sstable_statistics": construct.Container({
                "serialization_header": construct.Container({
                    "clustering_key_count": 0,
                    "regular_columns": [
                        construct.Container({
                            "type": construct.Container({
                                "name": "org.apache.cassandra.db.marshal.Int32Type",
                            }),
                        }),
                    ],
                }),
            }),
        },
    }

def test_cells():
    test_cells = [simple_cell_example, complex_cell_list_item_example, complex_cell_set_item_example, complex_cell_set_of_boolean_item_example, complex_cell_list_example, complex_row_body_example, simple_row_body_example, simple_unfiltered_example, simple_cell_third_column_example]
    for i, cell in enumerate(test_cells):
        sstable.utils.assert_equal(
            cell["obj"],
            cell["construct_struct"].parse(
            cell["bytes"],
            **cell["parsing_kwargs"]),
        )

        sstable.utils.assert_equal(
            cell["bytes"],
            cell["construct_struct"].build(cell["obj"],
            **cell["parsing_kwargs"],
            ),
        )

