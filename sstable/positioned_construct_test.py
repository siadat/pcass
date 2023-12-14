import os
import io

import sstable.varint
import construct
import sstable.positioned_construct


def inspect(byts):
    for (k, v) in sstable.positioned_construct.global_position_map.items():
        print(k, v)
    sstable.positioned_construct.pretty_hexdump("ok", io.BytesIO(byts), len(byts), os.sys.stdout, index=True)

####
typ = construct.Struct(
    "name_length" / sstable.varint.VarInt(),
    "name" / construct.Bytes(construct.this.name_length),
)
column = construct.Struct(
    "name_length" / construct.Int8ub,
    "name" / construct.Bytes(construct.this.name_length),
    "type" / typ,
    "second" / construct.Struct(
        "flag" / construct.Bytes(1),
    ),
    "third" / construct.Struct(
        "flag1" / construct.Bytes(1),
        "flag2" / construct.Bytes(1),
    ),
    "fourth" / construct.Struct(
        "flags" / construct.Array(2, construct.Bytes(1)),
    ),
)

####
# typ2 = construct.Struct(
#     "name" / construct.PascalString(sstable.varint.VarInt(), "ascii"),
# )
# column2 = construct.Struct(
#     "name" / construct.PascalString(construct.Int8ub, "ascii"),
#     "type" / typ2,
# )
# 
####
sstable.positioned_construct.init()
byts = column.build({
    "name_length": 3,
    "name": b"hii",
    "type": {
        "name_length": 5,
        "name": b"hello",
    },
    "second": {
        "flag": b"\x01",
    },
    "third": {
        "flag1": b"\x01",
        "flag2": b"\x02",
    },
    "fourth": {
        "flags": b"\x01\x02",
    },
})
column.parse_stream(io.BytesIO(byts))
# inspect(byts)

# ####
# print()
# print(">>>>", 100 * "-", "<<<<")
# print(">>>>", "DONT USE PascalString, because positionings will be relative hence wrong", "<<<<")
# print(">>>>", 100 * "-", "<<<<")
# print()
# sstable.positioned_construct.init()
# byts = column2.build({"name": "hii", "type": {"name": "hello"}})
# column2.parse_stream(io.BytesIO(byts))
# inspect(byts)
