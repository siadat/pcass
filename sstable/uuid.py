import construct
import sstable.utils


Uuid = construct.Hex(construct.Bytes(16))
sstable.utils.assert_equal(b'\x00' * 16, Uuid.build(b'\x00' * 16))
