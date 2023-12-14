import sstable.varint
import io

import sstable.utils
import sstable.sstable_data

def test_sstable():
    # sstable.utils.assert_equal(b"\x04\x61\x62\x63\x64", sstable.sstable_data.text_cell_value.build({"cell_value_len": 4, "cell_value": "abcd"}))

    sstable.utils.assert_equal(0, sstable.varint.VarInt().parse(bytes([0b00000000])))
    sstable.utils.assert_equal(1, sstable.varint.VarInt().parse(bytes([0b00000001])))
    sstable.utils.assert_equal(127, sstable.varint.VarInt().parse(bytes([0b01111111])))
    sstable.utils.assert_equal(128, sstable.varint.VarInt().parse(bytes([0b10000000, 0b10000000])))
    sstable.utils.assert_equal(129, sstable.varint.VarInt().parse(bytes([0b10000000, 0b10000001])))
    sstable.utils.assert_equal(640, sstable.varint.VarInt().parse(bytes([0b10000010, 0b10000000])))
    sstable.utils.assert_equal(32773, sstable.varint.VarInt().parse(bytes([0b11000000, 0b10000000, 0b00000101])))

    sstable.utils.assert_equal(bytes([0b00000000]), sstable.varint.VarInt().build(0))
    sstable.utils.assert_equal(bytes([0b00000001]), sstable.varint.VarInt().build(1))
    sstable.utils.assert_equal(bytes([0b01111111]), sstable.varint.VarInt().build(127))
    sstable.utils.assert_equal(bytes([0b10000000, 0b10000000]), sstable.varint.VarInt().build(128))
    sstable.utils.assert_equal(bytes([0b10000000, 0b10000001]), sstable.varint.VarInt().build(129))
    sstable.utils.assert_equal(bytes([0b10000010, 0b10000000]), sstable.varint.VarInt().build(640))
    sstable.utils.assert_equal(bytes([0b11000000, 0b10000000, 0b00000101]), sstable.varint.VarInt().build(32773))
    sstable.utils.assert_equal(bytes([0b10000010, 0b10000000]), sstable.varint.VarInt().build(640))
    assert bytes([0b11111111, 0b10000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000]) == sstable.varint.VarInt().build(1<<63)
    assert bytes([0b11111110, 0b10000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000]) == sstable.varint.VarInt().build(1<<55)

    assert bytes([0b11000000, 0b10100000, 0b00000000]) == sstable.varint.VarInt().build(0b10100000_00000000)

    assert bytes([0b10100000, 0b00000000]) == sstable.varint.VarInt().build(1<<13)
    assert bytes([0b11111101, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0x00000000]) == sstable.varint.VarInt().build(1<<48)
