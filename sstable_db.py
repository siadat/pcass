import construct
import varint

# https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#


text_cell_value = construct.Struct(
    "cell_value_len" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L272
    "cell_value" / construct.Bytes(construct.this.cell_value_len),
)
int_cell_value = construct.Struct(
    "cell_value" / construct.Int32sb,
)

# TODO this might be a CompositeType as well
java_type_to_construct = {
    b"org.apache.cassandra.db.marshal.UTF8Type": text_cell_value,
    b"org.apache.cassandra.db.marshal.Int32Type": int_cell_value,
}

simple_cell = construct.Struct(
    "cell_flags" / construct.Hex(construct.Int8ub), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L230-L234
                                                    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L258
    # NOTE: ctx._index is unfortunately globally incremented, so if this construct is used else here _index is incremented and never reset to 0!
    "cell" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.regular_columns[ctx._index].type.name, java_type_to_construct),
)
clustering_cell = construct.Struct(
    # "cell_value_len" / varint.VarInt(),
    # "cell_value" / construct.Bytes(construct.this.cell_value_len),

    # NOTE: ctx._index is unfortunately globally incremented, so if this construct is used else here _index is incremented and never reset to 0!
    "key" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.clustering_key_types[ctx._index].name, java_type_to_construct),
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
            "clustering_block" / construct.Struct(
                "clustering_block_header" / construct.Int8ub, # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L305
                "clustering_cells" / construct.RepeatUntil(lambda obj, lst, ctx: ctx._root._.sstable_statistics.serialization_header.clustering_key_count, clustering_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ClusteringPrefix.java#L310
            ),

            "serialized_row_body_size" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L169
            "row_body_start" / construct.Tell,
            "previous_unfiltered_size" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L170
            "timestamp_diff" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L174
                                                # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/SerializationHeader.java#L195
            # cells are repeated until the row body size is serialized_row_body_size
            "cells" / construct.RepeatUntil(lambda obj, lst, ctx: ctx._io.tell()-ctx.row_body_start+1 >= ctx.serialized_row_body_size, simple_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L211
        ),
    ),
)
partition_header = construct.Struct(
    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/ColumnIndex.java#L98
    "key_len" / construct.Int16ub,
    # "key" / construct.Bytes(construct.this.key_len),
    "key" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.partition_key_type.name, {
        b"org.apache.cassandra.db.marshal.UTF8Type": text_cell_value,
        b"org.apache.cassandra.db.marshal.Int32Type": int_cell_value,
    }),
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
db_format = construct.Struct("partitions" / construct.GreedyRange(partition))

# assert partition_header.build({
#         "key_len": 4,
#         "key": bytes([0xff, 0xff, 0xff, 0xff]),
#         "deletion_time": {
#             "local_deletion_time": int.from_bytes([0x7f, 0xff, 0xff, 0xff], 'big'),
#             "marked_for_delete_at": int.from_bytes([0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 'big'),
#         },
#     }) == bytes([0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
