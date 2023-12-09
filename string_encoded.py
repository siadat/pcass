import io
import os

import construct

import utils
import varint
import positioned_construct

# We don't use PascalString or Prefixed etc, because we prefer to have a name
# for every byte when debugging.
class StringEncoded(construct.Adapter):
    def __init__(self, subcon, encoding):
        super().__init__(subcon)
        self.encoding = encoding

    def _decode(self, obj, context, path):
        return obj.decode(self.encoding)

    def _encode(self, obj, context, path):
        return bytes(obj, self.encoding)


utils.assert_equal("abc", StringEncoded(construct.Bytes(3), "ascii").parse(b"abc"))
utils.assert_equal(b"abc", StringEncoded(construct.Bytes(3), "ascii").build("abc"))
