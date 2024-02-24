# vim: ft=yaml
meta:
  id: sstable
  endian: be
  imports:
    - vlq_base128_le
    - vlq_base128_be
seq:
  - id: partition
    type: partition
types:
  partition:
    seq:
      - id: partition_header
        type: partition_header
      - id: row
        type: row
  clustering_block_header:
    seq:
      - type: u1 # cassandra_3_0_varint # 1-9 bytes of varint
  simple_cell:
    seq:
      - id: cell_value
        type: cell_value
  cell_value:
    seq:
      - id: len_clustering_value
        type: u1 # cassandra_3_0_varint
      - id: clustering_value
        type: str
        size: len_clustering_value
        encoding: UTF-8
  clustering_block:
    seq:
      - id: clustering_block_header
        type: clustering_block_header
      - id: clustering_cells
        type: simple_cell
  partition_header:
    seq:
      - id: len_key
        type: u2be
      - id: key
        size: len_key
      - id: deletion_time
        type: deletion_time
  cell_flags:
    seq:
      - size: 1
  row_flags:
    seq:
      - size: 1
  row:
    seq:
      - type: row_flags
      - id: clustering_block
        type: clustering_block # 1-9 bytes of varint
  local_deletion_time:
    seq:
      - id: local_deletion_time
        type: u4 # == 32/8
  marked_for_delete_at:
    seq:
      - id: marked_for_delete_at
        type: u8 # == 64/8
  deletion_time:
    seq:
      - id: local_deletion_time
        type: local_deletion_time
      - id: marked_for_delete_at
        type: marked_for_delete_at
