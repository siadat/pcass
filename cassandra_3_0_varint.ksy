# vim: ft=yaml
meta:
  id: cassandra_3_0_varint
  ks-opaque-types: true
seq:
  - id: first_byte
    type: u1
  - id: additional_bytes
    if: first_byte & 0x80 != 0
    type: blahblah
