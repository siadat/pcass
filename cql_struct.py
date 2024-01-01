import construct
import sstable.string_encoded
import sstable.utils
import sstable.greedy_range

construct.setGlobalPrintFullStrings(sstable.utils.PRINT_FULL_STRING)

# This is used in the version field of the frame header to indicate that the
# frame is a response.
RESPONSE_FLAG = 0b10000000


short_int = construct.Int16ub

long_string = construct.Struct(
    "length" / construct.Int32ub,
    "string" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
)

string_list = construct.Struct(
    "count" / short_int,
    "strings" / construct.Array(construct.this.count, long_string),
)

#  The first element of the body of a RESULT message is an [int] representing the
#  `kind` of result. The rest of the body depends on the kind. The kind can be
#  one of:
#    0x0001    Void: for results carrying no information.
#    0x0002    Rows: for results to select queries, returning a set of rows.
#    0x0003    Set_keyspace: the result to a `use` query.
#    0x0004    Prepared: result to a PREPARE message.
#    0x0005    Schema_change: the result to a schema altering query.
class ResultKind:
    VOID          = 0x0001
    ROWS          = 0x0002
    SET_KEYSPACE  = 0x0003
    PREPARED      = 0x0004
    SCHEMA_CHANGE = 0x0005

result_void = construct.Struct(
    "kind" / construct.Int32ub,
    "body" / construct.Bytes(0),
)

#  Indicates a set of rows. The rest of the body of a Rows result is:
#    <metadata><rows_count><rows_content>
#  where:
#    - <metadata> is composed of:
#        <flags><columns_count>[<paging_state>][<global_table_spec>?<col_spec_1>...<col_spec_n>]
#      where:
#        - <flags> is an [int]. The bits of <flags> provides information on the
#          formatting of the remaining information. A flag is set if the bit
#          corresponding to its `mask` is set. Supported flags are, given their
#          mask:
#            0x0001    Global_tables_spec: if set, only one table spec (keyspace
#                      and table name) is provided as <global_table_spec>. If not
#                      set, <global_table_spec> is not present.
#            0x0002    Has_more_pages: indicates whether this is not the last
#                      page of results and more should be retrieved. If set, the
#                      <paging_state> will be present. The <paging_state> is a
#                      [bytes] value that should be used in QUERY/EXECUTE to
#                      continue paging and retrieve the remainder of the result for
#                      this query (See Section 8 for more details).
#            0x0004    No_metadata: if set, the <metadata> is only composed of
#                      these <flags>, the <column_count> and optionally the
#                      <paging_state> (depending on the Has_more_pages flag) but
#                      no other information (so no <global_table_spec> nor <col_spec_i>).
#                      This will only ever be the case if this was requested
#                      during the query (see QUERY and RESULT messages).
#        - <columns_count> is an [int] representing the number of columns selected
#          by the query that produced this result. It defines the number of <col_spec_i>
#          elements in and the number of elements for each row in <rows_content>.
#        - <global_table_spec> is present if the Global_tables_spec is set in
#          <flags>. It is composed of two [string] representing the
#          (unique) keyspace name and table name the columns belong to.
#        - <col_spec_i> specifies the columns returned in the query. There are
#          <column_count> such column specifications that are composed of:
#            (<ksname><tablename>)?<name><type>
#          The initial <ksname> and <tablename> are two [string] and are only present
#          if the Global_tables_spec flag is not set. The <column_name> is a
#          [string] and <type> is an [option] that corresponds to the description
#          (what this description is depends a bit on the context: in results to
#          selects, this will be either the user chosen alias or the selection used
#          (often a colum name, but it can be a function call too). In results to
#          a PREPARE, this will be either the name of the corresponding bind variable
#          or the column name for the variable if it is "anonymous") and type of
#          the corresponding result. The option for <type> is either a native
#          type (see below), in which case the option has no value, or a
#          'custom' type, in which case the value is a [string] representing
#          the fully qualified class name of the type represented. Valid option
#          ids are:
#            0x0000    Custom: the value is a [string], see above.
#            0x0001    Ascii
#            0x0002    Bigint
#            0x0003    Blob
#            0x0004    Boolean
#            0x0005    Counter
#            0x0006    Decimal
#            0x0007    Double
#            0x0008    Float
#            0x0009    Int
#            0x000B    Timestamp
#            0x000C    Uuid
#            0x000D    Varchar
#            0x000E    Varint
#            0x000F    Timeuuid
#            0x0010    Inet
#            0x0011    Date
#            0x0012    Time
#            0x0013    Smallint
#            0x0014    Tinyint
#            0x0020    List: the value is an [option], representing the type
#                            of the elements of the list.
#            0x0021    Map: the value is two [option], representing the types of the
#                           keys and values of the map
#            0x0022    Set: the value is an [option], representing the type
#                            of the elements of the set
#            0x0030    UDT: the value is <ks><udt_name><n><name_1><type_1>...<name_n><type_n>
#                           where:
#                              - <ks> is a [string] representing the keyspace name this
#                                UDT is part of.
#                              - <udt_name> is a [string] representing the UDT name.
#                              - <n> is a [short] representing the number of fields of
#                                the UDT, and thus the number of <name_i><type_i> pairs
#                                following
#                              - <name_i> is a [string] representing the name of the
#                                i_th field of the UDT.
#                              - <type_i> is an [option] representing the type of the
#                                i_th field of the UDT.
#            0x0031    Tuple: the value is <n><type_1>...<type_n> where <n> is a [short]
#                             representing the number of values in the type, and <type_i>
#                             are [option] representing the type of the i_th component
#                             of the tuple
#
#    - <rows_count> is an [int] representing the number of rows present in this
#      result. Those rows are serialized in the <rows_content> part.
#    - <rows_content> is composed of <row_1>...<row_m> where m is <rows_count>.
#      Each <row_i> is composed of <value_1>...<value_n> where n is
#      <columns_count> and where <value_j> is a [bytes] representing the value
#      returned for the jth column of the ith row. In other words, <rows_content>
#      is composed of (<rows_count> * <columns_count>) [bytes].
class ResultRowsFlags:
    GLOBAL_TABLES_SPEC = 0x0001
    HAS_MORE_PAGES     = 0x0002
    NO_METADATA        = 0x0004

result_rows = construct.Struct(
    "kind" / construct.Int32ub,
    "metadata" / construct.Struct(
        "flags" / construct.Int32ub,
        "columns_count" / construct.Int32ub,
        "paging_state" / construct.If(lambda ctx: ctx.flags & ResultRowsFlags.HAS_MORE_PAGES, long_string),
        "global_table_spec" / construct.If(lambda ctx: ctx.flags & ResultRowsFlags.GLOBAL_TABLES_SPEC and not ctx.flags & ResultRowsFlags.NO_METADATA, string_list),
        "column_specs" / construct.If(lambda ctx: not ctx.flags & ResultRowsFlags.NO_METADATA,
            construct.Array(construct.this.columns_count, construct.Struct(
                "keyspace" / construct.If(lambda ctx: not ctx.flags & ResultRowsFlags.GLOBAL_TABLES_SPEC, long_string),
                "table" / construct.If(lambda ctx: not ctx.flags & ResultRowsFlags.GLOBAL_TABLES_SPEC, long_string),
                "name" / long_string,
                "type" / construct.Int16ub,
            ),
        )),
    ),
    "rows_count" / construct.Int32ub,
    "rows_content" / sstable.greedy_range.GreedyRangeWithExceptionHandling(construct.Byte),
)

#   Flags applying to this frame. The flags have the following meaning (described
#   by the mask that allows selecting them):
#     0x01: Compression flag. If set, the frame body is compressed. The actual
#           compression to use should have been set up beforehand through the
#           Startup message (which thus cannot be compressed; Section 4.1.1).
#     0x02: Tracing flag. For a request frame, this indicates the client requires
#           tracing of the request. Note that only QUERY, PREPARE and EXECUTE queries
#           support tracing. Other requests will simply ignore the tracing flag if 
#           set. If a request supports tracing and the tracing flag is set, the response
#           to this request will have the tracing flag set and contain tracing
#           information.
#           If a response frame has the tracing flag set, its body contains
#           a tracing ID. The tracing ID is a [uuid] and is the first thing in
#           the frame body.
#     0x04: Custom payload flag. For a request or response frame, this indicates
#           that a generic key-value custom payload for a custom QueryHandler
#           implementation is present in the frame. Such a custom payload is simply
#           ignored by the default QueryHandler implementation.
#           Currently, only QUERY, PREPARE, EXECUTE and BATCH requests support
#           payload.
#           Type of custom payload is [bytes map] (see below). If either or both
#           of the tracing and warning flags are set, the custom payload will follow
#           those indicated elements in the frame body. If neither are set, the custom
#           payload will be the first value in the frame body.
#     0x08: Warning flag. The response contains warnings which were generated by the
#           server to go along with this response.
#           If a response frame has the warning flag set, its body will contain the
#           text of the warnings. The warnings are a [string list] and will be the
#           first value in the frame body if the tracing flag is not set, or directly
#           after the tracing ID if it is.

class FrameFlags:
    COMPRESSION_FLAG = 0x01
    TRACING_FLAG     = 0x02
    CUSTOM_PAYLOAD   = 0x04
    WARNING_FLAG     = 0x08

# 9. Error codes
# 
#   Let us recall that an ERROR message is composed of <code><message>[...]
#   (see 4.2.1 for details). The supported error codes, as well as any additional
#   information the message may contain after the <message> are described below:
#     0x0000    Server error: something unexpected happened. This indicates a
#               server-side bug.
#     0x000A    Protocol error: some client message triggered a protocol
#               violation (for instance a QUERY message is sent before a STARTUP
#               one has been sent)
#     0x0100    Authentication error: authentication was required and failed. The
#               possible reason for failing depends on the authenticator in use,
#               which may or may not include more detail in the accompanying
#               error message.
#     0x1000    Unavailable exception. The rest of the ERROR message body will be
#                 <cl><required><alive>
#               where:
#                 <cl> is the [consistency] level of the query that triggered
#                      the exception.
#                 <required> is an [int] representing the number of nodes that
#                            should be alive to respect <cl>
#                 <alive> is an [int] representing the number of replicas that
#                         were known to be alive when the request had been
#                         processed (since an unavailable exception has been
#                         triggered, there will be <alive> < <required>)
#     0x1001    Overloaded: the request cannot be processed because the
#               coordinator node is overloaded
#     0x1002    Is_bootstrapping: the request was a read request but the
#               coordinator node is bootstrapping
#     0x1003    Truncate_error: error during a truncation error.
#     0x1100    Write_timeout: Timeout exception during a write request. The rest
#               of the ERROR message body will be
#                 <cl><received><blockfor><writeType>
#               where:
#                 <cl> is the [consistency] level of the query having triggered
#                      the exception.
#                 <received> is an [int] representing the number of nodes having
#                            acknowledged the request.
#                 <blockfor> is an [int] representing the number of replicas whose
#                            acknowledgement is required to achieve <cl>.
#                 <writeType> is a [string] that describe the type of the write
#                             that timed out. The value of that string can be one
#                             of:
#                              - "SIMPLE": the write was a non-batched
#                                non-counter write.
#                              - "BATCH": the write was a (logged) batch write.
#                                If this type is received, it means the batch log
#                                has been successfully written (otherwise a
#                                "BATCH_LOG" type would have been sent instead).
#                              - "UNLOGGED_BATCH": the write was an unlogged
#                                batch. No batch log write has been attempted.
#                              - "COUNTER": the write was a counter write
#                                (batched or not).
#                              - "BATCH_LOG": the timeout occurred during the
#                                write to the batch log when a (logged) batch
#                                write was requested.
#                              - "CAS": the timeout occured during the Compare And Set write/update.
#                              - "VIEW": the timeout occured when a write involves
#                                 VIEW update and failure to acqiure local view(MV)
#                                 lock for key within timeout
#                              - "CDC": the timeout occured when cdc_total_space is
#                                 exceeded when doing a write to data tracked by cdc.
#     0x1200    Read_timeout: Timeout exception during a read request. The rest
#               of the ERROR message body will be
#                 <cl><received><blockfor><data_present>
#               where:
#                 <cl> is the [consistency] level of the query having triggered
#                      the exception.
#                 <received> is an [int] representing the number of nodes having
#                            answered the request.
#                 <blockfor> is an [int] representing the number of replicas whose
#                            response is required to achieve <cl>. Please note that
#                            it is possible to have <received> >= <blockfor> if
#                            <data_present> is false. Also in the (unlikely)
#                            case where <cl> is achieved but the coordinator node
#                            times out while waiting for read-repair acknowledgement.
#                 <data_present> is a single byte. If its value is 0, it means
#                                the replica that was asked for data has not
#                                responded. Otherwise, the value is != 0.
#     0x1300    Read_failure: A non-timeout exception during a read request. The rest
#               of the ERROR message body will be
#                 <cl><received><blockfor><numfailures><data_present>
#               where:
#                 <cl> is the [consistency] level of the query having triggered
#                      the exception.
#                 <received> is an [int] representing the number of nodes having
#                            answered the request.
#                 <blockfor> is an [int] representing the number of replicas whose
#                            acknowledgement is required to achieve <cl>.
#                 <numfailures> is an [int] representing the number of nodes that
#                               experience a failure while executing the request.
#                 <data_present> is a single byte. If its value is 0, it means
#                                the replica that was asked for data had not
#                                responded. Otherwise, the value is != 0.
#     0x1400    Function_failure: A (user defined) function failed during execution.
#               The rest of the ERROR message body will be
#                 <keyspace><function><arg_types>
#               where:
#                 <keyspace> is the keyspace [string] of the failed function
#                 <function> is the name [string] of the failed function
#                 <arg_types> [string list] one string for each argument type (as CQL type) of the failed function
#     0x1500    Write_failure: A non-timeout exception during a write request. The rest
#               of the ERROR message body will be
#                 <cl><received><blockfor><numfailures><write_type>
#               where:
#                 <cl> is the [consistency] level of the query having triggered
#                      the exception.
#                 <received> is an [int] representing the number of nodes having
#                            answered the request.
#                 <blockfor> is an [int] representing the number of replicas whose
#                            acknowledgement is required to achieve <cl>.
#                 <numfailures> is an [int] representing the number of nodes that
#                               experience a failure while executing the request.
#                 <writeType> is a [string] that describes the type of the write
#                             that failed. The value of that string can be one
#                             of:
#                              - "SIMPLE": the write was a non-batched
#                                non-counter write.
#                              - "BATCH": the write was a (logged) batch write.
#                                If this type is received, it means the batch log
#                                has been successfully written (otherwise a
#                                "BATCH_LOG" type would have been sent instead).
#                              - "UNLOGGED_BATCH": the write was an unlogged
#                                batch. No batch log write has been attempted.
#                              - "COUNTER": the write was a counter write
#                                (batched or not).
#                              - "BATCH_LOG": the failure occured during the
#                                write to the batch log when a (logged) batch
#                                write was requested.
#                              - "CAS": the failure occured during the Compare And Set write/update.
#                              - "VIEW": the failure occured when a write involves
#                                 VIEW update and failure to acqiure local view(MV)
#                                 lock for key within timeout
#                              - "CDC": the failure occured when cdc_total_space is
#                                 exceeded when doing a write to data tracked by cdc.
# 
#     0x2000    Syntax_error: The submitted query has a syntax error.
#     0x2100    Unauthorized: The logged user doesn't have the right to perform
#               the query.
#     0x2200    Invalid: The query is syntactically correct but invalid.
#     0x2300    Config_error: The query is invalid because of some configuration issue
#     0x2400    Already_exists: The query attempted to create a keyspace or a
#               table that was already existing. The rest of the ERROR message
#               body will be <ks><table> where:
#                 <ks> is a [string] representing either the keyspace that
#                      already exists, or the keyspace in which the table that
#                      already exists is.
#                 <table> is a [string] representing the name of the table that
#                         already exists. If the query was attempting to create a
#                         keyspace, <table> will be present but will be the empty
#                         string.
#     0x2500    Unprepared: Can be thrown while a prepared statement tries to be
#               executed if the provided prepared statement ID is not known by
#               this host. The rest of the ERROR message body will be [short
#               bytes] representing the unknown ID.

class ErrorCode:
    SERVER_ERROR     = 0x0000
    PROTOCOL_ERROR   = 0x000A
    AUTH_ERROR       = 0x0100
    UNAVAILABLE      = 0x1000
    OVERLOADED       = 0x1001
    IS_BOOTSTRAPPING = 0x1002
    TRUNCATE_ERROR   = 0x1003
    WRITE_TIMEOUT    = 0x1100
    READ_TIMEOUT     = 0x1200
    READ_FAILURE     = 0x1300
    FUNCTION_FAILURE = 0x1400
    WRITE_FAILURE    = 0x1500
    SYNTAX_ERROR     = 0x2000
    UNAUTHORIZED     = 0x2100
    INVALID          = 0x2200
    CONFIG_ERROR     = 0x2300
    ALREADY_EXISTS   = 0x2400
    UNPREPARED       = 0x2500


# Opcode is an integer byte that distinguishes the actual message:
# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec#L342-L358
class OpCode:
    ERROR          = 0x00
    STARTUP        = 0x01
    READY          = 0x02
    AUTHENTICATE   = 0x03
    OPTIONS        = 0x05
    SUPPORTED      = 0x06
    QUERY          = 0x07
    RESULT         = 0x08
    PREPARE        = 0x09
    EXECUTE        = 0x0A
    REGISTER       = 0x0B
    EVENT          = 0x0C
    BATCH          = 0x0D
    AUTH_CHALLENGE = 0x0E
    AUTH_RESPONSE  = 0x0F
    AUTH_SUCCESS   = 0x10

# https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L66
frame = construct.Struct(
    # Version is not well documented in the protocol unfortunately.
    # The new client sends version 66 (MAX_SUPPORTED) with OpCodes.OPTIONS as the first every request sent by the client.
    # Sources:
    #   https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/cluster/#:~:text=protocol_version%20%3D%2066
    #   https://docs.datastax.com/en/developer/python-driver/3.29/api/cassandra/#cassandra.ProtocolVersion:~:text=by%20this%20driver.-,MAX_SUPPORTED%20%3D%2066,-Maximum%20protocol%20version
    "version" / construct.Hex(construct.Byte),

    "flags" / construct.Hex(construct.Byte),
    "stream" / construct.Int16ub,
    "opcode" / construct.Hex(construct.Byte),
    "length" / construct.Int32ub,
    "body" / construct.Bytes(construct.this.length),
)

query = construct.Struct(
    "query" / long_string,
    "consistency" / short_int,
    "flags" / construct.Byte,
    # TODO: the rest of the body depends on the flags
)

error = construct.Struct(
    "code" / construct.Int32ub,
    "length" / short_int,
    "message" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
)

string_list = construct.Struct(
    "count" / short_int,
    "strings" / construct.Array(construct.this.count, construct.Struct(
        "length" / short_int,
        "string" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
    )),
)

string_map = construct.Struct(
    "count" / short_int,
    "keyvals" / construct.Array(construct.this.count, construct.Struct(
        "key_length" / short_int,
        "key" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.key_length), "utf-8"),
        "val_length" / short_int,
        "val" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.val_length), "utf-8"),
    )),
)

string_multimap = construct.Struct(
    "count" / short_int,
    "keyvals" / construct.Array(construct.this.count, construct.Struct(
        "length" / short_int,
        "key" / sstable.string_encoded.StringEncoded(construct.Bytes(construct.this.length), "utf-8"),
        "values" / string_list,
    )),
)

string_multimap_example = {
    "bytes": b'\x00\x01'
           + b'\x00\x04' + b'key1'
           + b'\x00\x02'
           + b'\x00\x06value1'
           + b'\x00\x06value2',
    "obj": {
        "count": 1,
        "keyvals": [
            {
                "length": 4,
                "key": "key1",
                "values": {
                    "count": 2,
                    "strings": [
                        {
                            "length": 6,
                            "string": "value1",
                        },
                        {
                            "length": 6,
                            "string": "value2",
                        },
                    ],
                },
            },
        ],
    },
}
sstable.utils.assert_equal(string_multimap_example["bytes"], string_multimap.build(string_multimap_example["obj"]))

error_example = {
    "bytes": b"\x00\x00\x00\x0A"  # code
           + b"\x00\x05" # length
           + b"hello", # message
    "obj": {
        "code": ErrorCode.PROTOCOL_ERROR,
        "length": 5,
        "message": "hello",
    },
}
sstable.utils.assert_equal(error_example["bytes"], error.build(error_example["obj"]))

frame_example = {
    "bytes": b"\x84" # version
           + b"\x00" # flags
           + b"\x00\x00" # stream
           + b"\x00" # opcode
           + len(error_example["bytes"]).to_bytes(4, byteorder='big')
           + error_example["bytes"],
    "obj": {
        "version": 0x04 | RESPONSE_FLAG,
        "flags": 0x00,
        "stream": 0x0000,
        "opcode": OpCode.ERROR,
        "length": len(error_example["bytes"]),
        "body": error_example["bytes"],
    },
}
sstable.utils.assert_equal(frame_example["bytes"], frame.build(frame_example["obj"]))

