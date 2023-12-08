import utils
import varint
import string_encoded

import construct


construct.setGlobalPrintFullStrings(utils.PRINT_FULL_STRING)

# https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#

class WithIndex(construct.Adapter):
    def __init__(self, subcon):
        super(WithIndex, self).__init__(subcon)
        self.index = 0

    def _decode(self, obj, context, path):
        result = (self.index, obj)
        self.index += 1
        return result

def get_partition_key_type_func(ctx):
    # lambda ctx: ctx._root._.sstable_statistics.serialization_header.partition_key_type.name
    name = ctx._root._.sstable_statistics.serialization_header.partition_key_type.name
    print("get_partition_key_type_func", type(name), name)
    return name
def get_cell_type_func(ctx):
    # lambda ctx: ctx._root._.sstable_statistics.serialization_header.regular_columns[ctx._index].type.name
    name = ctx._root._.sstable_statistics.serialization_header.regular_columns[ctx._index].type.name
    print("get_cell_type_func", type(name), name)
    return name
def get_clustering_key_count_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_count

def get_clustering_key_type_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_types[ctx._index].name

def has_clustering_columns_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_count > 0

text_cell_value = construct.Struct(
    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L272
    "cell_value_len" / varint.VarInt(),
    "cell_value" / string_encoded.StringEncoded(construct.Bytes(construct.this.cell_value_len), "utf-8"),
    # "cell_value" / construct.PascalString(varint.VarInt(), "utf-8"),
)
# utils.assert_equal(b"\x04\x61\x62\x63\x64", text_cell_value.build({"cell_value": "abcd"}))

int_cell_value = construct.Struct(
    "cell_value" / construct.Int32sb,
)
utils.assert_equal(b"\x00\x00\x00\x04", int_cell_value.build({"cell_value": 4}))

# Not tested with Cassnadra:
float_cell_value = construct.Struct(
    "cell_value" / construct.Float32b,
)
utils.assert_equal(b"\x00\x00\x00\x00", float_cell_value.build({"cell_value": 0}))

# Not tested with Cassnadra:
ascii_cell_value = construct.Struct(
    "length" / varint.VarInt(),
    "cell_value" / string_encoded.StringEncoded(construct.Bytes(construct.this.length), "ascii"),
    # "cell_value" / construct.PascalString(varint.VarInt(), "ascii"),
)
# utils.assert_equal(b"\x04\x61\x62\x63\x64", ascii_cell_value.build({"cell_value": "abcd"}))

# Not tested with Cassnadra:
boolean_cell_value = construct.Struct(
    "cell_value" / construct.OneOf(construct.Byte, [0, 1]),
)
utils.assert_equal(b"\x00", boolean_cell_value.build({"cell_value": False}))
utils.assert_equal(False, boolean_cell_value.parse(b"\x00").cell_value)


# TODO this might be a CompositeType as well
java_type_to_construct = {
    # Sources:
    # - https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.29/-/tree/src/java/org/apache/cassandra/db/marshal
    # - https://cassandra.apache.org/doc/stable/cassandra/cql/types.html
    "org.apache.cassandra.db.marshal.UTF8Type": text_cell_value,
    "org.apache.cassandra.db.marshal.Int32Type": int_cell_value,
    "org.apache.cassandra.db.marshal.AsciiType": ascii_cell_value,
    "org.apache.cassandra.db.marshal.BooleanType": boolean_cell_value,
    "org.apache.cassandra.db.marshal.FloatType": boolean_cell_value,
}

simple_cell = construct.Struct(
    "cell_flags" / construct.Hex(construct.Int8ub), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L230-L234
                                                    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L258
    # NOTE: ctx._index is unfortunately globally incremented, so if this construct is used else here _index is incremented and never reset to 0!
    "cell" / construct.Switch(get_cell_type_func, java_type_to_construct),
)
clustering_cell = construct.Struct(
    # "cell_value_len" / varint.VarInt(),
    # "cell_value" / construct.Bytes(construct.this.cell_value_len),

    # NOTE: ctx._index is unfortunately globally incremented, so if this construct is used else here _index is incremented and never reset to 0!
    # "key" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.clustering_key_types[ctx._index].name, java_type_to_construct),
    "key" / construct.Switch(get_clustering_key_type_func, java_type_to_construct),
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
            construct.If(
                # If there are no clustering columns then we should skip the clustering block:
                has_clustering_columns_func,
                "clustering_block" / construct.Struct(
                    "clustering_block_header" / construct.Int8ub, # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L305
                    "clustering_cells" / construct.Array(get_clustering_key_count_func, clustering_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L310
                ),
            ),

            "serialized_row_body_size" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L169
            "row_body_start" / construct.Tell,
            "previous_unfiltered_size" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L170
            "timestamp_diff" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L174
                                                # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/SerializationHeader.java#L195
            # cells are repeated until the row body size is serialized_row_body_size
            "cells" / construct.RepeatUntil(lambda obj, lst, ctx: ctx._io.tell()-ctx.row_body_start+1 >= ctx.serialized_row_body_size, WithIndex(simple_cell)), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L211
        ),
    ),
)
partition_header = construct.Struct(
    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java#L98
    "key_len" / construct.Int16ub,
    # "key" / construct.Bytes(construct.this.key_len),
    # "key" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.partition_key_type.name, java_type_to_construct),
    "key" / construct.Switch(get_partition_key_type_func, java_type_to_construct),
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
data_format = construct.Struct("partitions" / construct.GreedyRange(partition))
# data_format = construct.Struct("partitions" / partition) # construct.Debugger() ... construct.Probe(lookahead=32))
