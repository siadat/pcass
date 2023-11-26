# vim: ft=yaml
meta:
  id: sstable
  endian: be
  imports:
    - vlq_base128_le
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
      - type: vlq_base128_le # 1-9 bytes of varint
  simple_cell:
    seq:
      - id: len_clustering_value
        type: u1 # or u1?
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
  row_flag:
    seq:
      - size: 1
  row:
    seq:
      - type: row_flag
      # - id: extended_flags
      #   if: row_flag | 0x80
      - id: clustering_block
        type: clustering_block # 1-9 bytes of varint

      ## - id: undecided_2
      ##   size: 1
      ## - id: undecided_3_always_12
      ##   contents: [0x12]
      ## - id: undecided_4
      ##   size: 2

      # - id: undecided_5_always_08
      #   size: 1
      #   # contents: [0x08] # == 8
      #   # contents: [0x25] # == 37
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
