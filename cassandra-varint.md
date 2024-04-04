## Cassandra 3.0 SSTable unsigned varint
Cassandra'a unsigned varint is an efficient way to encode uint64 (long) values in 1 to 9 bytes.

I could describe it, but seeing some examples is usually more helpful:

## Examples

|     Value | Hex            | Unsigned varint encoding
|-----------|----------------|--------------------
|         1 | 0x01           |  `00000001`
|         2 | 0x02           |  `00000010`
|       ... | ...            |  ...
|       127 | 0x7f           |  `01111111`
|       128 | 0x80 0x80      |  `10000000` `10000000`
|       129 | 0x80 0x81      |  `10000000` `10000001`
|       130 | 0x80 0x82      |  `10000000` `10000010`
|       131 | 0x80 0x83      |  `10000000` `10000011`
|       ... | ...            |  ...
|       254 | ...            |  `10000000` `11111110`
|       255 | ...            |  `10000000` `11111111`
|       256 | ...            |  `10000001` `00000000`
|       257 | ...            |  `10000001` `00000001`
|       ... | ...            |  ...
|       520 | ...            |  `10000010` `00001000` (NOTE: the prefix is `100000` which is 7 bits and the actual value is `10` `00001000`)
|       ... | ...            |  ...
|       640 | ...            |  `10000010` `10000000` (NOTE: the prefix is `100000` which is 7 bits and the actual value is `10` `10000000`)
|       ... | ...            |  ...
|     32773 | 0xc0 0x80 0x05 |  `11000000` `10000000` `00000101`
|       ... | ...            |  ...
| 1<<64 - 2 | NoSpaceHereLol |  `11111111` `11111111` `11111111` `11111111` `11111111` `11111111` `11111111` `11111111` `11111110`
| 1<<64 - 1 | LotsOfFFFFF... |  `11111111` `11111111` `11111111` `11111111` `11111111` `11111111` `11111111` `11111111` `11111111`
| 1<<64     | OVERFLOW       |  OVERFLOW


Another view of a number represented as a varint:

    11000010 00100000 00101000
    **&===== ======== ========

    Legend:
    *: bits used to count the number of bytes that will follow
    &: the separator 0
    =: bits used for representing the actual value

In other words:

- `0xxxxxxx` means that this byte is the only value, the whole varint is only 1 byte
- `10xxxxxx xxxxxxxx` means actual value is 1 byte, the whole varint is 2 bytes. Note that the actual value (`x`s) may be prefixed with zeros
- `110xxxxx xxxxxxxx xxxxxxxx` means actual value is 2 byte, the whole varint is 3 bytes
- `11111111 xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx` means actual value is 8 byte, the whole varint is 9 bytes

Another example:

- `1001xxxx xxxxxxxx` ... prefix is `10`, actual value is `01xxxx xxxxxxxx`

## Play

```
$ poetry run python
>>> from sstable import utils
>>> from sstable import varint
>>> import io
>>> utils.bins(varint.build(1))
['00000001']
>>> varint.parse(io.BytesIO(bytes([0b11100010, 0b00001000, 0b00000000, 0b00000000])))
34078720
```


## Resources:

* https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#
* https://haaawk.github.io/2018/02/26/sstables-variant-integers.html
* https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/utils/vint/VIntCoding.java#L220
