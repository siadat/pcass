import construct

import sstable.utils
import sstable.varint
import sstable.with_context

import sstable.greedy_range
import sstable.string_encoded
import sstable.sstable_decimal
import sstable.dynamic_switch
import sstable.uuid

construct.setGlobalPrintFullStrings(sstable.utils.PRINT_FULL_STRING)

# https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#

ROW_FLAG__HAS_ALL_COLUMNS = 0x20
ROW_FLAG__HAS_COMPLEX_DELETION = 0x40

def cell_empty_func(obj):
    ret = obj.cell_flags & 0x04 == 0
    return ret

def get_partition_key_type_func(ctx):
    name = ctx._root._.sstable_statistics.serialization_header.partition_key_type.name
    return name

def get_cell_type_func(ctx):
    col = None
    cols = ctx._root._.sstable_statistics.serialization_header.regular_columns
    index = ctx._.cell_index

    if ctx._.missing_columns is not None:
        col = cols[ctx._.missing_columns[index]]
    else:
        col = cols[index]

    return col.type.name

def get_clustering_key_type_func(ctx):
    cols = ctx._root._.sstable_statistics.serialization_header.clustering_key_types
    name = cols[ctx._index].name
    return name

def get_clustering_key_count_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_count

def has_clustering_columns_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_count > 0

simple_cell = construct.Struct(
    "cell_flags" / construct.Hex(construct.Int8ub), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L230-L234
                                                    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L258
    # NOTE: ctx._index seems ok, I used to think it incremented globally
    "cell" / construct.If(
        # TODO: 0x04 means empty value, e.g. empty '' string (and probably used for tombstones as well?)
        cell_empty_func,
        sstable.dynamic_switch.DynamicSwitch(get_cell_type_func),
    ),
)

cell_path = construct.Struct(
    "length" / sstable.varint.VarInt(),
    "path" / construct.Bytes(construct.this.length),
)

complex_cell_item = construct.Struct(
    "cell_flags" / construct.Hex(construct.Int8ub), # see simple_cell
    "path" / cell_path,
    "cell" / construct.If(
        # TODO: 0x04 means empty value, e.g. empty '' string (and probably used for tombstones as well?)
        cell_empty_func,
        sstable.dynamic_switch.DynamicSwitch(get_cell_type_func),
    ),
)

delta_deletion_time = construct.Struct(
    "delta_mark_for_delete_at" / sstable.varint.VarInt(),
    "delta_local_deletion_time" / sstable.varint.VarInt(),
)

complex_cell = construct.Struct(
    "complex_deletion_time" / delta_deletion_time,
    "items_count" / sstable.varint.VarInt(),
    "items" / construct.Array(construct.this.items_count, sstable.with_context.WithContext(complex_cell_item, missing_columns=lambda ctx: ctx._.missing_columns, cell_index=lambda ctx: ctx._.cell_index)),
)
clustering_cell = construct.Struct(
    # "cell_value_len" / sstable.varint.VarInt(),
    # "cell_value" / construct.Bytes(construct.this.cell_value_len),

    # NOTE: ctx._index seems ok, I used to think it incremented globally
    # "key" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.clustering_key_types[ctx._index].name, java_type_to_construct),
    ### "key" / construct.Switch(get_clustering_key_type_func, java_type_to_construct),
    "key" / sstable.dynamic_switch.DynamicSwitch(get_clustering_key_type_func),
)

def has_complex_deletion(x):
    row_flags = x._.overridden_row_flags
    ret = row_flags & ROW_FLAG__HAS_COMPLEX_DELETION == 0x40
    return ret

def has_missing_columns_func(x):
    row_flags = x._.overridden_row_flags
    ret = row_flags & ROW_FLAG__HAS_ALL_COLUMNS == 0x00
    return ret

# Source: https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#:~:text=We%20have%20a%20_superset_%20of%20columns%2C
class EnabledColumns(construct.Construct):
    def __init__(self, columns_count_predicate):
        super().__init__()
        self.columns_count_predicate = columns_count_predicate

    def _parse(self, stream, context, path):
        columns_count = self.columns_count_predicate(context)

        if columns_count < 64:
            mask = sstable.varint.VarInt().parse_stream(stream)
            enabled_col_indexes = []
            for i in range(columns_count):
                disabled = (1 << i) & mask
                if not disabled:
                    enabled_col_indexes.append(i)

            return enabled_col_indexes
        else:
            disabled_count = sstable.varint.VarInt().parse_stream(stream)

            indexes = []
            for i in range(columns_count - disabled_count):
                index = sstable.varint.VarInt().parse_stream(stream)
                indexes.append(index)

            if disabled_count >= columns_count/2:
                # there are fewer enabled colums, so the listed indexes are for
                # enabled colums to be more space efficient.
                # We want to return list of enabled indexes.
                return indexes
            else:
                # there are fewer disabled colums, so the listed indexes are
                # for disabled colums to be more space efficient.
                # We still want to return the index of enabled columns though
                return list(set(range(columns_count)) - set(indexes))

    def _build(self, obj, stream, context, path):
        raise Exception("TODO: please implement _build for EnabledColumns")

sstable.utils.assert_equal([3, 4, 6, 7, 8, 9], EnabledColumns(lambda context: 10).parse(bytes([0b00100111])))
sstable.utils.assert_equal([10], EnabledColumns(lambda context: 11).parse(bytes([0b10000011, 0b11111111])))
sstable.utils.assert_equal([], EnabledColumns(lambda context: 66).parse(bytes([66])))
sstable.utils.assert_equal([7, 8], EnabledColumns(lambda context: 66).parse(bytes([64, 7, 8])))

row_body_format = construct.Struct(
  "row_body_start" / construct.Tell,
  "previous_unfiltered_size" / sstable.varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L170
  "timestamp_diff" / sstable.varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L174
                                      # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/SerializationHeader.java#L195

  # Not: https://sourcegraph.com/github.com/scylladb/scylladb@scylla-5.4.0/-/blob/sstables/mx/writer.cc?L1150
  # https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/db/Columns.java?L446-455
  # Encoding: https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/db/Columns.java?L520-522
  "missing_columns" / construct.If(
        has_missing_columns_func,
        EnabledColumns(lambda context: len(context._root._.sstable_statistics.serialization_header.regular_columns)),
        # construct.Switch(get_missing_columns_encoding_fun, missing_columns_encoding_to_construct),
        # sstable.varint.VarInt(),
  ),
  # cells are repeated until the row body size is serialized_row_body_size
  # "cells" / construct.RepeatUntil(get_cell_repeat_until_func, simple_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L211
  "cells" / construct.Array(lambda ctx: len(ctx.missing_columns) if ctx.missing_columns is not None else len(ctx._root._.sstable_statistics.serialization_header.regular_columns), construct.Switch(has_complex_deletion, {
      True: sstable.with_context.WithContext(complex_cell, missing_columns=lambda ctx: ctx.missing_columns, cell_index=lambda ctx: ctx._index),
      False: sstable.with_context.WithContext(simple_cell, missing_columns=lambda ctx: ctx.missing_columns, cell_index=lambda ctx: ctx._index),
  }),
  ), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L211
)
unfiltered = construct.Struct(
    "row_flags" / construct.Hex(construct.Int8ub), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L78-L85
    "row" / construct.If(
        # If flags has 0x01, then it is the end of the partition and nothing will follow (no actual row)
        # Called in https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java#L163
        # Defined in https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L348-L351
        construct.this.row_flags & 0x01 != 0x01,
        construct.Struct(
            # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L125
            # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L165
            # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L40-L41

            # TODO: support composite primary key.
            # > "Note that we don’t store the number of clustering cells as we take this information from table schema."
            # > https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#:~:text=Note%20that%20we%20don%E2%80%99t%20store%20the%20number%20of%20clustering%20cells%20as%20we%20take%20this%20information%20from%20table%20schema.
            "clustering_block" / construct.If(
                # If there are no clustering columns then we should skip the clustering block:
                has_clustering_columns_func,
                construct.Struct(
                    "clustering_block_header" / construct.Int8ub, # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L305
                    "clustering_cells" / construct.Array(get_clustering_key_count_func, clustering_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L310
                ),
            ),
            "serialized_row_body_size" / sstable.varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L169
            "row_body" / sstable.with_context.WithContext(row_body_format, overridden_row_flags=lambda ctx: ctx._.row_flags),
        ),
    ),
)

partition_header = construct.Struct(
    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java#L98
    "key_len" / construct.Int16ub,
    "key" / construct.Bytes(construct.this.key_len),
    "deletion_time" / construct.Struct(
        # Looks similar https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/SerializationHeader.java#L210-L211
        "local_deletion_time" / construct.Int32ub,
        "marked_for_delete_at" / construct.Int64ub,
    ),
)
partition = construct.Struct(
    # YES: https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java#L106
    # Similar? https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/partitions/PartitionUpdate.java#L685-L685
    # Even though this has a guard against sstable, it looks similar: https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/rows/UnfilteredRowIteratorSerializer.java#L110
    "partition_header" / partition_header,
    # "unfiltereds" / unfiltered, # construct.GreedyRange(unfiltered),
    "unfiltereds" / construct.RepeatUntil(lambda obj, lst, ctx: (obj.row_flags & 0x01) == 0x01, unfiltered),
)

data_format = construct.Struct("partitions" / sstable.greedy_range.GreedyRangeWithExceptionHandling(partition))
