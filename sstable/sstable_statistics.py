import sstable.utils
import construct

import sstable.varint
import sstable.string_encoded
import sstable.string_encoded
import sstable.uuid

construct.setGlobalPrintFullStrings(sstable.utils.PRINT_FULL_STRING)

# https://opensource.docs.scylladb.com/stable/architecture/sstable/sstable3/sstables-3-statistics.html

# vint == sstable.varint: https://sourcegraph.com/github.com/scylladb/scylladb@01e54f5b12e72a2976f973d23ae0c61ce19ba914/-/blob/vint-serialization.hh

# https://github.com/apache/cassandra/blob/cassandra-3.0/src/java/org/apache/cassandra/io/sstable/metadata/MetadataType.java#L28
modified_utf8 = construct.Struct(
    "length" / construct.Int16ub,

    "utf8_string" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
    # "utf8_bytes" / construct.Bytes(construct.this.utf8_length),
)
bucket = construct.Struct(
    "prev_bucket_offset" / construct.Int64ub,
    "name" / construct.Int64ub,
)

estimated_histogram = construct.Struct(
    "length" / construct.Int32ub,
    "bucket" / construct.Array(construct.this.length, bucket),
)

streaming_histogram = construct.Struct(
    "bucket_number_limit" / construct.Int32ub,
    "buckets" / estimated_histogram, # construct.Array(construct.this.length, bucket),
)

commit_log_position = construct.Struct(
    "segment_id" / construct.Int64sb,
    "position_in_segment" / construct.Int32ub,
)

commit_log_interval = construct.Struct(
    "start" / commit_log_position,
    "end" / commit_log_position,
)

clustering_column = construct.Struct(
    "length" / construct.Int16ub,
    "name" / construct.Bytes(construct.this.length),
)
clustering_bound = construct.Struct(
    "length" / construct.Int32ub,
    "column" / construct.Array(construct.this.length, clustering_column),
)
typ = construct.Struct(
    "name_length" / sstable.varint.VarInt(),
    "name" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.name_length), "ascii"),
)
column = construct.Struct(
    "name_length" / construct.Int8ub,
    "name" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.name_length), "ascii"),

    "type" / typ,
)

VALIDATION_METADATA = 0
COMPACTION_METADATA = 1
STATISTICS_METADATA = 2
SERIALIZATION_METADATA = 3

def metadata_exists(typ):
    def fn(ctx):
        return typ in [x.type for x in ctx.toc]
    return fn

statistics_format = construct.Struct(
    "metadata_count" / construct.Int32ub,
    "toc" / construct.Array(construct.this.metadata_count, construct.Struct(
        "type" / construct.Int32ub,
        "offset" / construct.Int32ub,
    )),
    "validation_metadata" / construct.If(metadata_exists(VALIDATION_METADATA), construct.Struct(
        "partition_name" / modified_utf8,
        "bloom_filter_fp_chance" / construct.Float64b,
    )),
    "compaction_metadata" / construct.If(metadata_exists(COMPACTION_METADATA), construct.Struct(
        "length" / construct.Int32ub,
        "bytes" / construct.Bytes(construct.this.length),
    )),
    "statistics_metadata" / construct.If(metadata_exists(STATISTICS_METADATA), construct.Struct(
        "parition_sizes" / estimated_histogram,
        "column_counts" / estimated_histogram,
        "commit_log_upper_bound" / commit_log_position,
        "min_timestamp" / construct.Int64ub,
        "max_timestamp" / construct.Int64ub,
        "min_local_deletion_time" / construct.Hex(construct.Int32ub),
        "max_local_deletion_time" / construct.Hex(construct.Int32ub),
        "min_ttl" / construct.Int32ub,
        "max_ttl" / construct.Int32ub,

        "compression_rate" / construct.Float64b,
        "tombstones" / streaming_histogram,
        "level" / construct.Int32ub,
        "repaired_at" / construct.Int64ub,

        "min_clustering_key" / clustering_bound,
        "max_clustering_key" / clustering_bound,
        "has_legacy_counters" / construct.Int8ub,

        "number_of_columns" / construct.Int64ub,
        "number_of_rows" / construct.Int64ub,

        "commit_log_lower_bound" / commit_log_position,

        "commit_log_intervals_length" / construct.Int32ub,
        "commit_log_intervals" / construct.Array(construct.this.commit_log_intervals_length, commit_log_interval),

        "TODO_WHY_IS_THIS_NEEDED" / construct.Bytes(1),
        "host_id" / sstable.uuid.Uuid,
    )),
    # "serialization_header_start" / construct.Tell,
    "serialization_header" / construct.If(metadata_exists(SERIALIZATION_METADATA), construct.Struct(
        # https://github.com/scylladb/scylladb/blob/scylla-5.4.0/sstables/types.hh#L400
        # "min_timestamp" / construct.Int64ub,
        # "min_local_deletion_time" / construct.Int32ub,
        # "min_ttl" / construct.Int32ub,
        # "partition_key_type" / typ,
        "min_timestamp" / sstable.varint.VarInt(),
        "min_local_deletion_time" / sstable.varint.VarInt(),
        "min_ttl" / sstable.varint.VarInt(),
        "partition_key_type" / typ,

        "clustering_key_count" / sstable.varint.VarInt(),
        "clustering_key_types" / construct.Array(construct.this.clustering_key_count, typ),

        "static_column_count" / sstable.varint.VarInt(),
        "static_columns" / construct.Array(construct.this.static_column_count, column),

        "regular_column_count" / sstable.varint.VarInt(),
        # NOTE: regular_columns doesn't include all columns in the table. It
        # only includes the columns set in any of the rows in this specific
        # SSTable (memtable). i.e. if all rows in this sstblae are missing
        # value for column3, then column3 is not mentioned here (therefore no
        # information about it being a missing_column needs to be encoded in
        # Data.db as well).
        "regular_columns" / construct.Array(construct.this.regular_column_count, column),
    )),
)
