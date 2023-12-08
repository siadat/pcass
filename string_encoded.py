import io
import os

import construct

import varint
import positioned_construct

class StringEncoded(construct.Adapter):
    def __init__(self, subcon, encoding):
        super().__init__(subcon)
        self.encoding = encoding

    def _decode(self, obj, context, path):
        return obj.decode(self.encoding)

    def _encode(self, obj, context, path):
        return obj
        # return obj.encode()

# typ = construct.Struct(
#     "name_length" / varint.VarInt(),
#     "name" / construct.Bytes(construct.this.name_length),
# )
# column = construct.Struct(
#     "name_length" / construct.Int8ub,
#     "name" / StringEncoded(construct.Bytes(construct.this.name_length), "ascii"),
#     "type" / typ,
# )
#
# positioned_construct.init()
# byts = column.build({"name_length": 3, "name": b"hii", "type": {"name_length": 5, "name": b"hello"}})
# column.parse_stream(io.BytesIO(byts))
# positioned_construct.pretty_hexdump("ok", io.BytesIO(byts), len(byts), os.sys.stdout, index=True)
