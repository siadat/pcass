import sstable.utils
import sstable.sstable_data

def test_sstable():
    sstable.utils.assert_equal(b"\x04\x61\x62\x63\x64", sstable.sstable_data.text_cell_value.build({"cell_value_len": 4, "cell_value": "abcd"}))
