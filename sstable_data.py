import utils
import varint

import construct

import greedy_range
import string_encoded
import sstable_decimal
import uuid

construct.setGlobalPrintFullStrings(utils.PRINT_FULL_STRING)

# https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#

def cell_empty_func(obj):
    ret = obj.cell_flags & 0x04 != 0x4
    return ret

def get_cell_count_func(ctx): 
    cols_count = len(ctx._root._.sstable_statistics.serialization_header.regular_columns)
    if ctx.missing_columns is not None:
        return len(ctx.missing_columns)
    else:
        return cols_count

def get_partition_key_type_func(ctx):
    name = ctx._root._.sstable_statistics.serialization_header.partition_key_type.name
    if name not in java_type_to_construct:
        raise Exception(f"Unhandled type {name}, please add to java_type_to_construct")
    return name

def get_cell_type_func(ctx):
    cols = ctx._root._.sstable_statistics.serialization_header.regular_columns
    if ctx._.missing_columns is not None:
        col = cols[ctx._.missing_columns[ctx._index]]
        name = col.type.name
    else:
        col = cols[ctx._index]
        name = col.type.name

    if name not in java_type_to_construct:
        raise Exception(f"Unhandled type {name}, please add to java_type_to_construct")
    return name

def get_clustering_key_type_func(ctx):
    cols = ctx._root._.sstable_statistics.serialization_header.clustering_key_types
    name = cols[ctx._index].name
    if name not in java_type_to_construct:
        raise Exception(f"Unhandled type {name}, please add to java_type_to_construct")
    return name

def get_clustering_key_count_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_count

def has_clustering_columns_func(ctx):
    return ctx._root._.sstable_statistics.serialization_header.clustering_key_count > 0

text_cell_value = construct.Struct(
    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L272
    "cell_value_len" / varint.VarInt(),
    "cell_value" / string_encoded.StringEncoded(construct.Bytes(construct.this.cell_value_len), "utf-8"),
)
utils.assert_equal(b"\x04\x61\x62\x63\x64", text_cell_value.build({"cell_value_len": 4, "cell_value": "abcd"}))

int_cell_value = construct.Struct(
    "cell_value" / construct.Int32sb,
)
utils.assert_equal(b"\x00\x00\x00\x04", int_cell_value.build({"cell_value": 4}))

# The IntegerType is used for CQL varint type
# It is a Java BigInteger https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/serializers/IntegerSerializer.java?L35
integer_cell_value = construct.Struct(
    "length" / varint.VarInt(),
    "cell_value" / construct.BytesInteger(construct.this.length),
)
utils.assert_equal(b"\x01\x09", integer_cell_value.build({"length": 1, "cell_value": 9}))

# https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/db/marshal/ShortType.java?L54
# https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html
short_cell_value = construct.Struct(
    "length" / varint.VarInt(), # TODO: now sure why short needs a length? it should always be 2?
    "cell_value" / construct.BytesInteger(construct.this.length), # I think this is probably always 2 bytes, i.e. construct.Int16sb
)
utils.assert_equal(b"\x02\x00\x04", short_cell_value.build({"length": 2, "cell_value": 4}))

long_cell_value = construct.Struct(
    "cell_value" / construct.Int64sb,
)
utils.assert_equal(b"\x00\x00\x00\x00\x00\x00\x00\x04", long_cell_value.build({"cell_value": 4}))

# https://github.com/openjdk/jdk/blob/jdk8-b120/jdk/src/share/classes/java/math/BigInteger.java#L3697-L3726
# https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/serializers/DecimalSerializer.java?L45-59
# Note that this ^ serialize() method doesn't include the length of the value.
decimal_cell_value = construct.Struct(
    "cell_value" / sstable_decimal.DecimalNumber,
)

# Not tested with Cassnadra:
float_cell_value = construct.Struct(
    "cell_value" / construct.Float32b,
)
utils.assert_equal(b"\x00\x00\x00\x00", float_cell_value.build({"cell_value": 0}))

double_cell_value = construct.Struct(
    "cell_value" / construct.Float64b,
)
utils.assert_equal(b"\x00\x00\x00\x00\x00\x00\x00\x00", double_cell_value.build({"cell_value": 0}))


# Not tested with Cassnadra:
ascii_cell_value = construct.Struct(
    "length" / varint.VarInt(),
    "cell_value" / string_encoded.StringEncoded(construct.Bytes(construct.this.length), "ascii"),
)
utils.assert_equal(b"\x04\x61\x62\x63\x64", ascii_cell_value.build({"length": 4, "cell_value": "abcd"}))

bytes_cell_value = construct.Struct(
    "length" / varint.VarInt(),
    "cell_value" / construct.Bytes(construct.this.length),
)
utils.assert_equal(b"\x04\x61\x62\x63\x64", bytes_cell_value.build({"length": 4, "cell_value": b"abcd"}))

# The ByteType seems to be used for tinyint AND it has a length! WTF :shrug:
byte_cell_value = construct.Struct(
    "length" / varint.VarInt(),
    "cell_value" / construct.Bytes(construct.this.length),
)

# Not tested with Cassnadra:
boolean_cell_value = construct.Struct(
    "cell_value" / construct.OneOf(construct.Byte, [0, 1]),
)
utils.assert_equal(b"\x00", boolean_cell_value.build({"cell_value": False}))
utils.assert_equal(False, boolean_cell_value.parse(b"\x00").cell_value)

# https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/serializers/TimestampSerializer.java?L122
# Note that getTime() returns a Java `long` and it represents milliseconds
timestamp_cell_value = construct.Struct(
    "cell_value" / construct.Int64sb,
)
utils.assert_equal(b"\x00\x00\x00\x00\x00\x00\x00\x04", timestamp_cell_value.build({"cell_value": 4}))
utils.assert_equal(4, timestamp_cell_value.parse(b"\x00\x00\x00\x00\x00\x00\x00\x04").cell_value)

uuid_cell_value = construct.Struct(
    "cell_value" / uuid.Uuid,
)

# TODO this might be a CompositeType as well
java_type_to_construct = {
    # Sources:
    # - https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.29/-/tree/src/java/org/apache/cassandra/db/marshal
    # - https://cassandra.apache.org/doc/stable/cassandra/cql/types.html
    "org.apache.cassandra.db.marshal.UTF8Type": text_cell_value,
    "org.apache.cassandra.db.marshal.ShortType": short_cell_value,
    "org.apache.cassandra.db.marshal.IntegerType": integer_cell_value,
    "org.apache.cassandra.db.marshal.Int32Type": int_cell_value,
    "org.apache.cassandra.db.marshal.LongType": long_cell_value,
    "org.apache.cassandra.db.marshal.DecimalType": decimal_cell_value,
    "org.apache.cassandra.db.marshal.AsciiType": ascii_cell_value,
    "org.apache.cassandra.db.marshal.ByteType": byte_cell_value,
    "org.apache.cassandra.db.marshal.BytesType": bytes_cell_value,
    "org.apache.cassandra.db.marshal.BooleanType": boolean_cell_value,
    "org.apache.cassandra.db.marshal.FloatType": float_cell_value,
    "org.apache.cassandra.db.marshal.DoubleType": double_cell_value,
    "org.apache.cassandra.db.marshal.TimestampType": timestamp_cell_value,
    "org.apache.cassandra.db.marshal.UUIDType": uuid_cell_value,
}

simple_cell = construct.Struct(
    "cell_flags" / construct.Hex(construct.Int8ub), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L230-L234
                                                    # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L258
    # NOTE: ctx._index seems ok, I used to think it incremented globally
    "cell" / construct.If(
        # TODO: 0x04 means empty value, e.g. empty '' string (and probably usef for tombstones as well?)
        cell_empty_func,
        construct.Switch(get_cell_type_func, java_type_to_construct),
    ),
)
clustering_cell = construct.Struct(
    # "cell_value_len" / varint.VarInt(),
    # "cell_value" / construct.Bytes(construct.this.cell_value_len),

    # NOTE: ctx._index seems ok, I used to think it incremented globally
    # "key" / construct.Switch(lambda ctx: ctx._root._.sstable_statistics.serialization_header.clustering_key_types[ctx._index].name, java_type_to_construct),
    "key" / construct.Switch(get_clustering_key_type_func, java_type_to_construct),
)
def has_missing_columns_func(x):
    if hasattr(x._, "overridden_row_flags"):
        row_flags = x._.overridden_row_flags
    else:
        row_flags = x._._.row_flags
    ret = row_flags & 0x20 == 0x00
    return ret

# Source: https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-data-file-format.html#:~:text=We%20have%20a%20_superset_%20of%20columns%2C
class EnabledColumns(construct.Construct):
    def __init__(self, columns_count_predicate):
        super().__init__()
        self.columns_count_predicate = columns_count_predicate

    def _parse(self, stream, context, path):
        columns_count = self.columns_count_predicate(context)

        if columns_count < 64:
            mask = varint.VarInt().parse_stream(stream)
            enabled_col_indexes = []
            for i in range(columns_count):
                disabled = (1 << i) & mask
                if not disabled:
                    enabled_col_indexes.append(i)

            return enabled_col_indexes
        else:
            disabled_count = varint.VarInt().parse_stream(stream)

            indexes = []
            for i in range(columns_count - disabled_count):
                index = varint.VarInt().parse_stream(stream)
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

utils.assert_equal([3, 4, 6, 7, 8, 9], EnabledColumns(lambda context: 10).parse(bytes([0b00100111])))
utils.assert_equal([10], EnabledColumns(lambda context: 11).parse(bytes([0b10000011, 0b11111111])))
utils.assert_equal([], EnabledColumns(lambda context: 66).parse(bytes([66])))
utils.assert_equal([7, 8], EnabledColumns(lambda context: 66).parse(bytes([64, 7, 8])))

row_body_format = construct.Struct(
  "row_body_start" / construct.Tell,
  "previous_unfiltered_size" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L170
  "timestamp_diff" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L174
                                      # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/SerializationHeader.java#L195

  # Not: https://sourcegraph.com/github.com/scylladb/scylladb@scylla-5.4.0/-/blob/sstables/mx/writer.cc?L1150
  # https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/db/Columns.java?L446-455
  # Encoding: https://sourcegraph.com/github.com/apache/cassandra@cassandra-3.0.0/-/blob/src/java/org/apache/cassandra/db/Columns.java?L520-522
  "missing_columns" / construct.If(
        has_missing_columns_func,
        EnabledColumns(lambda context: len(context._root._.sstable_statistics.serialization_header.regular_columns)),
        # construct.Switch(get_missing_columns_encoding_fun, missing_columns_encoding_to_construct),
        # varint.VarInt(),
  ),
  # cells are repeated until the row body size is serialized_row_body_size
  # "cells" / construct.RepeatUntil(get_cell_repeat_until_func, simple_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L211
  "cells" / construct.Array(get_cell_count_func, simple_cell), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/BufferCell.java#L211
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
            "serialized_row_body_size" / varint.VarInt(), # https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java#L169
            "row_body" / row_body_format,
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

data_format = construct.Struct("partitions" / greedy_range.GreedyRangeWithExceptionHandling(partition))
