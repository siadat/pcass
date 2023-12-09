import os
import io

import varint
import construct
import positioned_construct


def inspect(byts):
    for (k, v) in positioned_construct.global_position_map.items():
        print(k, v)
    positioned_construct.pretty_hexdump("ok", io.BytesIO(byts), len(byts), os.sys.stdout, index=True)

####
typ = construct.Struct(
    "name_length" / varint.VarInt(),
    "name" / construct.Bytes(construct.this.name_length),
)
column = construct.Struct(
    "name_length" / construct.Int8ub,
    "name" / construct.Bytes(construct.this.name_length),
    "type" / typ,
)

####
typ2 = construct.Struct(
    "name" / construct.PascalString(varint.VarInt(), "ascii"),
)
column2 = construct.Struct(
    "name" / construct.PascalString(construct.Int8ub, "ascii"),
    "type" / typ2,
)

####
positioned_construct.init()
byts = column.build({"name_length": 3, "name": b"hii", "type": {"name_length": 5, "name": b"hello"}})
column.parse_stream(io.BytesIO(byts))
inspect(byts)

####
print()
print(">>>>", 100 * "-", "<<<<")
print(">>>>", "DONT USE PascalString, because positionings will be relative hence wrong", "<<<<")
print(">>>>", 100 * "-", "<<<<")
print()
positioned_construct.init()
byts = column2.build({"name": "hii", "type": {"name": "hello"}})
column2.parse_stream(io.BytesIO(byts))
inspect(byts)
