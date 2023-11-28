## Unsigned varint
value hex            | prefix   | actual number
---------------------+----------+---------------------
    1 0x01           | x        |          00000001
    2 0x02           | x        |          00000010
      ...            |          |
  127 0x7f           | x        |          01111111
  128 0x80 0x80      | 10000000 |          10000000
  129 0x80 0x81      | 10000000 |          10000001
  130 0x80 0x82      | 10000000 |          10000010
  131 0x80 0x83      | 10000000 |          10000011
      ...            |
32773 0xc0 0x80 0x05 | 11000000 | 10000000 00000101
      ...

Note:
- 0xxxxxxx means that this byte is the only value, the whole varint is only 1 byte
- 10000000 prefix means actual value is 1 byte, the whole var int is 2 bytes
- 11000000 prefix means actual value is 2 byte, the whole var int is 3 bytes
- 11100000 prefix means actual value is 3 byte, the whole var int is 4 bytes
- 11110000 prefix means actual value is 4 byte, the whole var int is 5 bytes
- 11111000 prefix means actual value is 5 byte, the whole var int is 6 bytes
- 11111100 prefix means actual value is 6 byte, the whole var int is 7 bytes
- 11111110 prefix means actual value is 7 byte, the whole var int is 8 bytes
- 11111111 prefix means actual value is 8 byte, the whole var int is 9 bytes

## Resources:

* https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#
* https:https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#//haaawk.github.io/2018/02/26/sstables-variant-integers.html
* https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/utils/vint/VIntCoding.java#L220
