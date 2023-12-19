import box
import construct
import sstable.utils
import sstable.sstable_data

simple_cell_example = {
        "construct_struct": sstable.sstable_data.simple_cell,
        "bytes": b"\x08"
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

complex_cell_item_example = {
        "construct_struct": sstable.sstable_data.complex_cell_item,
        "bytes": b"\x08"
                + b"\x10"
                + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff"
                + b"\x04"
                + b"\x00\x00\x00\x01",
        "obj": construct.Container({
            "cell_flags": 0x08,
            "path": construct.Container({
                "length": 16,
                "path": b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff",
            }),
            "cell": construct.Container({
                "cell_value_len": 4,
                "cell_value": {
                    "cell_value": 1,
                },
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

def test_cells():
    test_cells = [simple_cell_example, complex_cell_item_example]
    for cell in test_cells:
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

