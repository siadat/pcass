## Cassandra 3.0 SSTable unsigned varint
Cassandra'a unsigned varint is an efficient way to encode uint64 (long) values.
It provides an efficient way to serialize and deserialize small values in as
small as a single byte (u8), while allowing uint64 values to be encoded in 9
bytes (8 value bytes and one flag byte)

## Examples

              |                | prefix bits  |
              |                | including    |
              |                | start of the | rest of the
        value | hex            | actual value | value bit(s)
        ------+----------------+--------------+------------------
            1 | 0x01           |  <NO PREFIX> | 00000001
            2 | 0x02           |  <NO PREFIX> | 00000010
          ... | ...            |          ... | ...
          127 | 0x7f           |  <NO PREFIX> | 01111111
          128 | 0x80 0x80      |     10000000 | 10000000
          129 | 0x80 0x81      |     10000000 | 10000001
          130 | 0x80 0x82      |     10000000 | 10000010
          131 | 0x80 0x83      |     10000000 | 10000011
          ... | ...            |          ... | ...
          254 | ...            |     10000000 | 11111110
          255 | ...            |     10000000 | 11111111
          256 | ...            |     10000001 | 00000000
          257 | ...            |     10000001 | 00000001
          ... | ...            |          ... | ...
          520 | ...            |     10000010 | 00001000 (NOTE: the prefix is "100000" which is 7 bits and the actual value is "10_00001000")
          ... | ...            |          ... | ...
          640 | ...            |     10000010 | 10000000 (NOTE: the prefix is "100000" which is 7 bits and the actual value is "10_10000000")
          ... | ...            |          ... | ...
        32773 | 0xc0 0x80 0x05 |     11000000 | 10000000 00000101
          ... | ...            |          ... | ...
    1<<64 - 2 | NoSpaceHereLol |     11111111 | 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110
    1<<64 - 1 | LotsOfFFFFF... |     11111111 | 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111
    1<<64     | OVERFLOW       |     OVERFLOW | OVERFLOW


Another view of endoing 640:

    10000010 10000000
    *<--->{---------}

    Legend:
    *           = 1 byte will follow
    <--->       = 0s paddings
    {---------} = actual value

In other words:

- `0xxxxxxx` means that this byte is the only value, the whole varint is only 1 byte
- `10xxxxxx xxxxxxxx` means actual value is 1 byte, the whole varint is 2 bytes. Note that the actual value (`x`s) may be prefixed with zeros
- `110xxxxx xxxxxxxx xxxxxxxx` means actual value is 2 byte, the whole varint is 3 bytes
- `11111111 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx` means actual value is 8 byte, the whole varint is 9 bytes

BUT ALSO:

- `10010000` ... means actual value is 1 byte, the actual number starts after the zeros after 1, ie after "100", so, "1000" is the start of the actual value

## Play

```
$ poetry run python
>>> from sstable import utils
>>> from sstable import varint
>>> utils.bins(varint.build(1))
['00000001']
```


## Resources:

* https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#
* https://haaawk.github.io/2018/02/26/sstables-variant-integers.html
* https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/utils/vint/VIntCoding.java#L220
