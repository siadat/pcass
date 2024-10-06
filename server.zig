const std = @import("std");
const net = std.net;
const tracy = @import("tracy.zig");
const builtin = @import("builtin");

const assert = std.debug.assert;
const ResponseFlag = 0x80; // == 0b10000000
const SupportedNativeCqlProtocolVersion = 0x05;
const CRC32_INITIAL_BYTES = [_]u8{ 0xfa, 0x2d, 0x55, 0xca };

// enum for version 4 and 5:
const Opcode = enum(u8) {
    ERROR = 0x00,
    STARTUP = 0x01,
    READY = 0x02,
    AUTHENTICATE = 0x03,
    OPTIONS = 0x05,
    SUPPORTED = 0x06,
    QUERY = 0x07,
    RESULT = 0x08,
    PREPARE = 0x09,
    EXECUTE = 0x0A,
    REGISTER = 0x0B,
    EVENT = 0x0C,
    BATCH = 0x0D,
    AUTHCHALLENGE = 0x0E,
    AUTHRESPONSE = 0x0F,
    AUTHSUCCESS = 0x10,
};

const ErrorCode = enum(u32) {
    SERVER_ERROR = 0x0000,
    PROTOCOL_ERROR = 0x000A,
    AUTH_ERROR = 0x0100,
    UNAVAILABLE = 0x1000,
    OVERLOADED = 0x1001,
    IS_BOOTSTRAPPING = 0x1002,
    TRUNCATE_ERROR = 0x1003,
    WRITE_TIMEOUT = 0x1100,
    READ_TIMEOUT = 0x1200,
    READ_FAILURE = 0x1300,
    FUNCTION_FAILURE = 0x1400,
    WRITE_FAILURE = 0x1500,
    SYNTAX_ERROR = 0x2000,
    UNAUTHORIZED = 0x2100,
    INVALID = 0x2200,
    CONFIG_ERROR = 0x2300,
    ALREADY_EXISTS = 0x2400,
    UNPREPARED = 0x2500,

    pub fn format(value: ErrorCode, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = options;
        _ = fmt;
        try writer.writeAll(@tagName(value));
    }
};

fn prettyBytes(buf: []const u8, logger: anytype, prefix: []const u8) void {
    for (1.., buf) |i, c| {
        logger.debug("{s} {d: >2}/{d}: 0x{x:0>2} {d: >3} {s}", .{ prefix, i, buf.len, c, c, prettyByte(c) });
    }
}

fn prettyBytesWithAnnotatedStruct(comptime T: type, buf: [sizeOfExcludingPadding(T)]u8, logger: anytype, prefix: []const u8) void {
    const last_field = std.meta.fields(T)[std.meta.fields(T).len - 1];
    comptime var i = 0;
    inline for (std.meta.fields(T)) |f| {
        inline for (1.., buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)]) |fi, c| {
            i += 1;
            logger.debug("{s} {d: >2}/{d}: 0x{x:0>2} {d: >3} {s} {s}.{s} {d}/{d}", .{
                prefix,
                i,
                @offsetOf(T, last_field.name) + @sizeOf(last_field.type),
                c,
                c,
                prettyByte(c),
                @typeName(T),
                f.name,
                fi,
                @sizeOf(f.type),
            });
        }
    }
}

fn prettyBufBytes(
    comptime T: type,
    bytes: []const u8,
    logger: Logger,
    prefix: []const u8,
) void {
    for (bytes, 0..) |c, i| {
        logger.debug("{s} {d: >2}/{d}: 0x{x:0>2} {d: >3} {s} {s}", .{
            prefix,
            i + 1,
            bytes.len,
            c,
            c,
            prettyByte(c),
            @typeName(T),
        });
    }
}

pub fn prettyByte(
    byte: u8,
) [4]u8 {
    switch (byte) {
        '\n' => return [_]u8{ '\'', '\\', 'n', '\'' },
        '\r' => return [_]u8{ '\'', '\\', 'r', '\'' },
        '\t' => return [_]u8{ '\'', '\\', 't', '\'' },
        '\'' => return [_]u8{ '\'', '\\', '\'', '\'' },
        // NOTE: 39 is the ASCII code for the single quote, which is already covered above
        32...38, 40...126 => return [_]u8{
            '\'',
            byte,
            '\'',
            ' ',
        },
        else => return [_]u8{ '-', '-', '-', '-' },
    }
}

const StateMachine = struct {
    //
};

// https://github.com/apache/cassandra/blob/5d4bcc797af/doc/native_protocol_v5.spec#L220-L225
const FrameHeader = packed struct {
    version: u8,
    flags: u8,
    stream: i16,
    opcode: Opcode,
    length: u32,
};

const Result = struct {
    kind: Int,
    result_body: ResultBody,
};

const ResultBody = union {
    rows: unreachable,
    set_keyspace: unreachable,
    prepared: unreachable,
    schema_change: unreachable,
};

const Column = Bytes;

const ResultRowContent = PrefixedSlice(Int, Column);

const ColumnSpec = struct {
    const Self = @This();
    keyspace_and_table: ?KeyspaceAndTable,
    column_name: String,
    column_type: Option,
};

const KeyspaceAndTable = struct {
    keyspace: String,
    table: String,
};

const ResultRows = struct {
    const Self = @This();
    metadata_flags: Int,
    metadata_column_count: Int,
    metadata_paging_state: ?Bytes,
    metadata_new_metadata_id: ?Int,
    metadata_global_table_spec_keyspace_and_table: ?KeyspaceAndTable,
    metadata_col_specs: std.ArrayList(ColumnSpec),
    rows: PrefixedSlice(Int, ResultRowContent), // 'rows' is <rows_count><rows_content>

    pub fn writeStructBytes(
        self: *const Self,
        writer: anytype,
        logger: Logger,
    ) !void {
        try writeBytes(Self, &self.metadata_flags, writer, logger);
        try writeBytes(Self, &self.metadata_column_count, writer, logger);
        if (self.metadata_paging_state) |x| try writeBytes(Bytes, &x, writer, logger);
        if (self.metadata_new_metadata_id) |x| try writeBytes(Int, &x, writer, logger);
        if (self.metadata_global_table_spec_keyspace_and_table) |x| try writeBytes(KeyspaceAndTable, &x, writer, logger);
        for (self.rows.array_list.items) |row| {
            try writeBytes(ResultRowContent, &row, writer, logger);
        }
    }

    pub fn fromStructBytes(
        reader: anytype,
        allocator: std.mem.Allocator,
        logger: Logger,
    ) !Self {
        _ = reader;
        _ = allocator;
        _ = logger;
        unreachable;
    }
};

const V5Frame = struct {
    const Self = @This();

    is_self_contained_flag: u1,

    // TODO: there could be multiple "envelopes" inside the payload. Can we use an std.ArrayList(Frame)?
    //       For example: payload: std.ArrayList(Frame),
    payload_raw_bytes: std.ArrayList(u8),
    payload_frame_header: FrameHeader,

    // https://sourcegraph.com/github.com/datastax/python-driver@6e2ffd4e1ddc/-/blob/cassandra/segment.py?L39-51
    // Called by:
    // root
    // ...
    //   * connect()
    //     * asd()
    //       - asdsdsad()
    //       * asdsad()     ---YOU ARE HERE
    //       asd()
    //         asdsdsad()
    //         asdsad()
    //            ...
    // Calls:
    //   @truncate() --
    //     asd()
    fn computeCrc24Slice(data: []const u8) u24 {
        const CRC24_INIT: u32 = 0x875060;
        const CRC24_POLY: u32 = 0x1974F0B;
        var crc: u32 = CRC24_INIT;
        for (data) |c| {
            const tmp1: u32 = c & 0xff;
            const tmp2: u32 = tmp1 << 16;
            crc ^= tmp2;
            // crc ^= (c & 0xff) << 16;
            for (0..8) |_| {
                crc <<= 1;
                if (crc & 0x1000000 != 0) {
                    crc ^= CRC24_POLY;
                }
            }
        }
        return @truncate(crc);
    }

    fn computeCrc24Int(input: u64, len: usize) u24 {
        const data = std.mem.asBytes(&input)[0..len];
        return computeCrc24Slice(data);
    }

    pub fn writeStructBytes(
        self: *const Self,
        writer: anytype,
        logger: Logger,
    ) !void {
        logger.debug("payload size {d} + {d}", .{ sizeOfExcludingPadding(FrameHeader), self.payload_raw_bytes.items.len });
        const payload_length: u17 = @truncate(sizeOfExcludingPadding(FrameHeader) + self.payload_raw_bytes.items.len);
        const header_padding: u6 = 0;

        // "frame header"
        var header_bytes = [3]u8{ 0, 0, 0 };
        std.mem.writePackedInt(u17, &header_bytes, 0, payload_length, std.builtin.Endian.little);
        std.mem.writePackedInt(u1, &header_bytes, 17, self.is_self_contained_flag, std.builtin.Endian.little);
        std.mem.writePackedInt(u6, &header_bytes, 18, header_padding, std.builtin.Endian.little);
        prettyBytes(header_bytes[0..], logger, "header_bytes");
        prettyBytes(self.payload_raw_bytes.items[0..], logger, "payload_raw_bytes");
        for (header_bytes) |c| {
            logger.debug("first_three_bytes: 0x{x:0>2} 0b{b:0>8}", .{ c, c });
        }

        std.mem.reverse(u8, header_bytes[0..]);
        try writeBytes([3]u8, &header_bytes, writer, logger);

        // crc24
        std.mem.reverse(u8, header_bytes[0..]);
        const crc24 = computeCrc24Slice(header_bytes[0..]);
        const crc24_bytes = std.mem.toBytes(crc24)[0..3];
        try writer.writeAll(crc24_bytes);

        // payload header:
        var payload_header_buf: [sizeOfExcludingPadding(FrameHeader)]u8 = undefined;
        var payload_header_stream = std.io.fixedBufferStream(&payload_header_buf);
        var multiwriter_stream = std.io.multiWriter(.{
            payload_header_stream.writer(),
            writer,
        });
        try writeBytes(
            FrameHeader,
            &self.payload_frame_header,
            multiwriter_stream.writer(),
            logger,
        );
        // payload bytes
        try writer.writeAll(self.payload_raw_bytes.items);

        var c = std.hash.Crc32.init();
        c.update(&CRC32_INITIAL_BYTES);
        c.update(&payload_header_buf);
        c.update(self.payload_raw_bytes.items);
        const payload_crc32 = c.final();

        var payload_crc32_bytes = std.mem.toBytes(payload_crc32);
        try writer.writeAll(payload_crc32_bytes[0..]);
    }

    fn deinit(self: *const Self) void {
        self.payload_raw_bytes.deinit();
    }

    pub fn fromStructBytes(
        reader: anytype,
        allocator: std.mem.Allocator,
        logger: Logger,
    ) !Self {
        // because streams are read byte-by-byte, we read multiple fields at once
        // and because the value is little-endian, we don't use the `fromBytes` function
        // (because fromBytes assumes everything is big-endian and reverses the bytes before converting them to the value)
        var first_three_bytes: [3]u8 = undefined;
        const n = try reader.readAll(&first_three_bytes);
        if (n < first_three_bytes.len) {
            return error.EndOfStream;
        }
        for (first_three_bytes) |c| {
            logger.debug("read_first_three_bytes: 0x{x:0>2} 0b{b:0>8}", .{ c, c });
        }

        // const T = packed struct(u) {}
        // std.mem.bytesAsValue(T, buf[0..]).*;
        const payload_length = std.mem.readPackedInt(u17, &first_three_bytes, 0, std.builtin.Endian.little);
        const is_self_contained_flag = std.mem.readPackedInt(u1, &first_three_bytes, @bitSizeOf(@TypeOf(payload_length)), std.builtin.Endian.little);
        // NTOE: we discard header_padding
        prettyBytes(first_three_bytes[0..], logger, "read_header_bytes");

        logger.debug("length: {d}", .{payload_length});
        logger.debug("is_self_contained_flag: {d}", .{is_self_contained_flag});

        const header_crc24 = try fromBytes(u24, reader, allocator, logger);
        logger.debug("header_crc24: {x}", .{header_crc24});

        const payload_frame_header = try fromBytes(FrameHeader, reader, allocator, logger);
        logger.debug("payload_frame_header: {any}", .{payload_frame_header});

        var payload_raw_bytes = std.ArrayList(u8).init(allocator);
        try payload_raw_bytes.resize(payload_frame_header.length);

        const raw_payload_n = try reader.readAll(payload_raw_bytes.items);
        if (raw_payload_n < payload_frame_header.length) {
            return error.EndOfStream;
        }
        prettyBufBytes(u8, payload_raw_bytes.items, logger, "payload_raw_bytes");

        const payload_crc32 = try fromBytes(u32, reader, allocator, logger);
        _ = payload_crc32;
        return .{
            .is_self_contained_flag = is_self_contained_flag,
            .payload_raw_bytes = payload_raw_bytes,
            .payload_frame_header = payload_frame_header,
        };
    }
};

fn Frame(comptime T: type) type {
    return struct {
        version: u8,
        flags: u8,
        stream: i16,
        opcode: Opcode,
        body: PrefixedTypedBytes(u32, T),
    };
}

// TODO: just found this https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/protocol.py?L127-129
const ErrorBody = struct {
    code: ErrorCode,
    message: String,

    fn byteCount(self: *const ErrorBody) usize {
        return sizeOfExcludingPadding(ErrorCode) + self.message.byteCount();
    }
};

// Spec: "[STARTUP] must be the first message of the connection, except for OPTIONS"
const StartupBody = StringMap;

fn fromBytes(
    comptime T: type,
    reader: anytype,
    allocator: std.mem.Allocator,
    logger: Logger,
) !T {
    return switch (@typeInfo(T)) {
        .Struct => {
            if (std.meta.hasFn(T, "fromStructBytes")) {
                return try T.fromStructBytes(reader, allocator, logger);
            }
            var self: T = undefined;
            inline for (std.meta.fields(T)) |f| {
                logger.debug("field: {s} ({any})", .{ f.name, f.type });
                @field(self, f.name) = try fromBytes(f.type, reader, allocator, logger);
            }
            return self;
        },
        else => {
            // NOTE: we use sizeOfExcludingPadding so that u24 is read as 3 bytes
            // instead of 4 bytes as it would be if we used @sizeOf
            // NOTE: this function only supports byte-by-byte reading, if the
            // bit size of the type is not divisible by 8, then you should either read a multiple
            // of 8 bits and parse your values from that.
            var buf: [sizeOfExcludingPadding(T)]u8 = undefined;

            const n = try reader.readAll(&buf);
            if (n < buf.len) {
                return error.EndOfStream;
            }
            prettyBufBytes(T, buf[0..], logger, "fromBytes");
            std.mem.reverse(u8, buf[0..]);
            return std.mem.bytesAsValue(T, buf[0..]).*;
        },
    };
}

fn sizeOfExcludingPadding(comptime T: type) @TypeOf(@sizeOf(T)) {
    // We are adding 7 bits, in case the result is not a multiple of 8
    return (@bitSizeOf(T) + 7) / 8;
}

test "test sizeOfExcludingPadding" {
    const want = 3;
    const got = sizeOfExcludingPadding(u21);
    try std.testing.expectEqual(want, got);
}

test "test computeCrc24Int and computeCrc24Slice" {
    // >>> from cassandra import segment
    // >>> hex(segment.compute_crc24(0x1_23_45, 3))
    // '0x8bc640'
    try std.testing.expectEqual(0x875060, V5Frame.computeCrc24Int(0x0, 0));
    try std.testing.expectEqual(0x7de777, V5Frame.computeCrc24Int(0x0, 3));
    try std.testing.expectEqual(0x8bc640, V5Frame.computeCrc24Int(0x1_23_45, 3));

    try std.testing.expectEqual(0xf5230f, V5Frame.computeCrc24Int(0xAA_12_34_56, 3));
    try std.testing.expectEqual(0xf5230f, V5Frame.computeCrc24Int(0xBB_12_34_56, 3));

    try std.testing.expectEqual(0x875060, V5Frame.computeCrc24Slice(&[0]u8{}));
    try std.testing.expectEqual(0x7de777, V5Frame.computeCrc24Slice(&[3]u8{ 0, 0, 0 }));
    try std.testing.expectEqual(0xf5230f, V5Frame.computeCrc24Slice(&[3]u8{ 0x56, 0x34, 0x12 }));
}

test "test crc32" {
    // >>> from cassandra import segment
    // >>> hex(segment.compute_crc32(b"\xab\xcd", 0xfa_2d_55_ca))
    // '0xc3eba942'
    {
        var c = std.hash.Crc32.init();
        c.update(&[_]u8{ 0xfa, 0x2d, 0x55, 0xca });
        try std.testing.expectEqual(@as(u32, 0x44777ed3), c.final());
    }
    {
        var c = std.hash.Crc32.init();
        c.update(&[_]u8{ 0xfa, 0x2d, 0x55, 0xca });
        c.update(&[_]u8{0x00});
        try std.testing.expectEqual(@as(u32, 0xcd9c1b9d), c.final());
    }

    {
        var c = std.hash.Crc32.init();
        c.update(&[_]u8{ 0xfa, 0x2d, 0x55, 0xca, 0x00 });
        try std.testing.expectEqual(@as(u32, 0xcd9c1b9d), c.final());
    }
    {
        var c = std.hash.Crc32.init();
        c.update(&[_]u8{ 0xfa, 0x2d, 0x55, 0xca });
        c.update("abc");
        try std.testing.expectEqual(@as(u32, 0xc5367a08), c.final());
    }
}

const Int = i32;
const Long = i64;
const Byte = u8;
const Short = u16;
const String = PrefixedSlice(Short, Byte);
const LongString = PrefixedSlice(Int, Byte);
const UUID = [16]Byte;
const StringList = PrefixedSlice(Short, String);
const Bytes = PrefixedSlice(Int, Byte);
const Value = PrefixedSlice(Int, Byte);
const ShortBytes = PrefixedSlice(Short, Byte);
const UnsignedVint = unreachable;
const Vint = unreachable;
const Option = PrefixedSlice(Short, OptionValue);
const OptionList = PrefixedSlice(Short, Option);
const Inet = unreachable; // one byte more byte (for port number) than size in PrefixedSlice(Byte, Byte);
const InetAddr = PrefixedSlice(Byte, Byte);
const Consistency = Short;
const StringListPair = Pair(String, StringList);
const StringPair = Pair(String, String);
const BytePair = Pair(String, String);
const StringMap = PrefixedSlice(Short, StringPair);
const StringMultimap = PrefixedSlice(Short, StringListPair);
const BytesMap = PrefixedSlice(Short, BytePair);
const Varint = unreachable;
const Duration = unreachable;
const UDT = unreachable;
const Tuple = unreachable;

const OptionValue = union {
    ascii: String,
    bigint: Long,
    blob: Bytes,
    boolean: Byte,
    counter: Long,
    decimal: Bytes,
    double: f64,
    float: f32,
    int: Int,
    timestamp: Long,
    uuid: UUID,
    varchar: String,
    varint: Varint,
    timeuuid: UUID,
    inet: Inet,
    date: Int,
    time: Long,
    smallint: Short,
    tinyint: Byte,
    duration: Duration,
    list: Option,
    map: Pair(Option, Option),
    set: Option,
    udt: UDT,
    tuple: Tuple,
};

const RawBytes = struct {
    const Self = @This();
    bytes: []const u8,

    fn byteCount(self: *const Self) usize {
        return self.bytes.len;
    }

    pub fn writeStructBytes(
        self: *const Self,
        writer: anytype,
        _: Logger,
    ) !void {
        try writer.writeAll(self.bytes);
    }

    pub fn fromStructBytes(
        reader: anytype,
        allocator: std.mem.Allocator,
        _: Logger,
    ) !Self {
        var buf = std.ArrayList(u8).init(allocator);
        defer buf.deinit();
        try reader.readAll(buf.writer());
        return .{
            .bytes = buf.items,
        };
    }
};

fn Pair(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        key: K,
        value: V,

        fn byteCount(self: *const Self) usize {
            return getByteCount(self.key) + getByteCount(self.value);
        }

        pub fn writeStructBytes(
            self: *const Self,
            writer: anytype,
            logger: Logger,
        ) !void {
            try writeBytes(K, &self.key, writer, logger);
            try writeBytes(V, &self.value, writer, logger);
        }

        pub fn fromStructBytes(
            reader: anytype,
            allocator: std.mem.Allocator,
            logger: Logger,
        ) !Self {
            return .{
                .key = try fromBytes(K, reader, allocator, logger),
                .value = try fromBytes(V, reader, allocator, logger),
            };
        }
    };
}

const QueryParameters = struct {
    const QueryMasks = enum(Int) {
        WITH_VALUES = 0x0001, // If set, values are provided for bound variables in the query.
        WITH_SKIP_METADATA = 0x0002, // If set, the result set will have the NO_METADATA flag.
        WITH_RESULT_PAGE_SIZE = 0x0004, // If set, <result_page_size> controls the desired page size.
        WITH_PAGING_STATE = 0x0008, // If set, <paging_state> should be present.
        WITH_SERIAL_CONSISTENCY = 0x0010, // If set, <serial_consistency> should be present.
        WITH_TIMESTAMP = 0x0020, // If set, <timestamp> must be present.
        WITH_NAMES_FOR_VALUES = 0x0040, // Only makes sense if WITH_VALUES is set; values are preceded by names.
        WITH_KEYSPACE = 0x0080, // If set, <keyspace> must be present, overriding the connection's keyspace.
        WITH_NOW_IN_SECONDS = 0x0100, // If set, <now_in_seconds> must be present, used for testing.
    };

    const Self = @This();

    consistency: Consistency,
    flags: Int,

    values: ?PrefixedSlice(Short, Value) = null,
    result_page_size: ?Int = null,
    paging_state: ?Bytes = null,
    serial_consistency: ?Consistency = null,
    timestamp: ?Long = null,
    keyspace: ?String = null,
    now_in_seconds: ?Int = null,

    fn byteCount(self: *const Self) usize {
        var count = 0;
        count += @sizeOf(self.Consistency);
        count += @sizeOf(self.flags);
        if (self.values) |x| count += x.byteCount();
        if (self.result_page_size) |x| count += @sizeOf(x);
        if (self.paging_state) |x| count += x.byteCount();
        if (self.serial_consistency) |x| count += @sizeOf(x);
        if (self.timestamp) |x| count += @sizeOf(x);
        if (self.keyspace) |x| count += x.byteCount();
        if (self.now_in_seconds) |x| count += @sizeOf(x);
        return count;
    }

    pub fn writeStructBytes(
        self: *const Self,
        writer: anytype,
        logger: Logger,
    ) !void {
        try writeBytes(Consistency, &self.consistency, writer, logger);
        try writeBytes(Int, &self.flags, writer, logger);
        if (self.values) |x| try writeBytes(PrefixedSlice(Short, Value), &x, writer, logger);
        if (self.result_page_size) |x| try writeBytes(Int, &x, writer, logger);
        if (self.paging_state) |x| try writeBytes(Bytes, &x, writer, logger);
        if (self.serial_consistency) |x| try writeBytes(Consistency, &x, writer, logger);
        if (self.timestamp) |x| try writeBytes(Long, &x, writer, logger);
        if (self.keyspace) |x| try writeBytes(String, &x, writer, logger);
        if (self.now_in_seconds) |x| try writeBytes(Int, &x, writer, logger);
    }

    pub fn fromStructBytes(
        reader: anytype,
        allocator: std.mem.Allocator,
        logger: Logger,
    ) !Self {
        var self = Self{
            .consistency = try fromBytes(Consistency, reader, allocator, logger),
            .flags = try fromBytes(Int, reader, allocator, logger),
        };
        logger.debug("flags = {any}", .{self.flags});
        if (self.flags & @intFromEnum(QueryMasks.WITH_VALUES) != 0) {
            self.values = try fromBytes(PrefixedSlice(Short, Value), reader, allocator, logger);
        }
        if (self.flags & @intFromEnum(QueryMasks.WITH_RESULT_PAGE_SIZE) != 0) {
            self.result_page_size = try fromBytes(Int, reader, allocator, logger);
        }
        if (self.flags & @intFromEnum(QueryMasks.WITH_PAGING_STATE) != 0) {
            self.paging_state = try fromBytes(Bytes, reader, allocator, logger);
        }
        if (self.flags & @intFromEnum(QueryMasks.WITH_SERIAL_CONSISTENCY) != 0) {
            self.serial_consistency = try fromBytes(Consistency, reader, allocator, logger);
        }
        if (self.flags & @intFromEnum(QueryMasks.WITH_TIMESTAMP) != 0) {
            self.timestamp = try fromBytes(Long, reader, allocator, logger);
        }
        if (self.flags & @intFromEnum(QueryMasks.WITH_KEYSPACE) != 0) {
            self.keyspace = try fromBytes(String, reader, allocator, logger);
        }
        if (self.flags & @intFromEnum(QueryMasks.WITH_NOW_IN_SECONDS) != 0) {
            self.now_in_seconds = try fromBytes(Int, reader, allocator, logger);
        }
        return self;
    }
};

const Query = struct {
    const Self = @This();
    query_string: LongString,
    query_parameters: QueryParameters,

    fn byteCount(self: *const Self) usize {
        return self.query_string.byteCount() + self.query_parameters.byteCount();
    }

    pub fn writeStructBytes(
        self: *const Self,
        writer: anytype,
        logger: Logger,
    ) !void {
        try writeBytes(String, &self.query_string, writer, logger);
        try writeBytes(QueryParameters, &self.query_parameters, writer, logger);
    }

    pub fn fromStructBytes(
        reader: anytype,
        allocator: std.mem.Allocator,
        logger: Logger,
    ) !Self {
        return .{
            .query_string = try fromBytes(LongString, reader, allocator, logger),
            .query_parameters = try fromBytes(QueryParameters, reader, allocator, logger),
        };
    }
};

// TODO: add unit a test for this
fn PrefixedTypedBytes(comptime S: type, comptime T: type) type {
    return struct {
        const NewType = @This();
        // length: S,
        value: T,

        fn byteCount(self: *const NewType) usize {
            // NOTE: this size includes the size of the length field
            // This method is not used anywhere yet as of writing this comment.
            return @sizeOf(S) + getByteCount(self.value);
        }

        pub fn fromValue(value: T) NewType {
            return .{
                .value = value,
            };
        }
        pub fn writeStructBytes(
            self: *const NewType,
            writer: anytype,
            logger: Logger,
        ) !void {
            // NOTE: len does not include the size of the length field itself
            const len = @as(S, @truncate(getByteCount(self.value)));
            try writeBytes(S, &len, writer, logger);
            try writeBytes(T, &self.value, writer, logger);
        }

        pub fn fromStructBytes(
            reader: anytype,
            allocator: std.mem.Allocator,
            logger: Logger,
        ) !NewType {
            _ = try fromBytes(S, reader, allocator, logger);
            return .{
                .value = try fromBytes(T, reader, allocator, logger),
            };
        }
    };
}

fn PrefixedSlice(comptime S: type, comptime T: type) type {
    return struct {
        const NewType = @This();

        allocator: std.mem.Allocator,
        array_list: std.ArrayList(T),

        fn deinit(self: *NewType) void {
            for (self.array_list.items) |item| {
                if (std.meta.hasMethod(T, "deinit")) {
                    item.deinit();
                }
            }
            self.array_list.deinit();
        }
        fn byteCount(self: *const NewType) usize {
            var count: usize = 0;
            for (self.array_list.items) |item| {
                count += getByteCount(item);
            }
            return @sizeOf(S) + count;
        }
        pub fn writeStructBytes(
            self: *const NewType,
            writer: anytype,
            logger: Logger,
        ) !void {
            const len = @as(S, @truncate(self.array_list.items.len));
            try writeBytes(S, &len, writer, logger);
            for (self.array_list.items) |item| {
                try writeBytes(T, &item, writer, logger);
            }
        }
        fn fromSlice(allocator: std.mem.Allocator, items: []const T) !NewType {
            var array_list = std.ArrayList(T).init(allocator);
            for (items) |item| {
                try array_list.append(item);
            }
            return .{
                .allocator = allocator,
                .array_list = array_list,
            };
        }
        pub fn fromStructBytes(
            reader: anytype,
            allocator: std.mem.Allocator,
            logger: Logger,
        ) !NewType {
            const len = try fromBytes(S, reader, allocator, logger);
            var array_list = std.ArrayList(T).init(allocator);
            for (0..@intCast(len)) |_| {
                const item = try fromBytes(T, reader, allocator, logger);
                try array_list.append(item);
            }
            return .{
                .allocator = allocator,
                .array_list = array_list,
            };
        }
        pub fn format(self: NewType, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
            if (T == u8) {
                try std.fmt.format(writer, "\"{s}\"", .{self.array_list.items});
            } else {
                for (self.array_list.items) |item| {
                    try item.format(fmt, options, writer);
                }
            }
        }
    };
}

test "test PrefixedSlice" {
    const logger = Logger.init(std.log.Level.debug, "unit test for PrefixedSlice");

    var s = try String.fromSlice(std.testing.allocator, "hello");
    defer s.deinit();

    try std.testing.expect(std.mem.eql(u8, "hello", s.array_list.items));
    logger.debug("s = {any}", .{s});

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try writeBytes(String, &s, buf.writer(), logger);
    logger.debug("buf.items.len = {d}", .{buf.items.len});
    logger.debug("buf.items     = {x}", .{buf.items});
    try std.testing.expectEqual(@sizeOf(Short) + s.array_list.items.len, buf.items.len);

    const want = [7]u8{ 0, 5, 'h', 'e', 'l', 'l', 'o' };
    try std.testing.expect(std.mem.eql(u8, want[0..], buf.items));

    var string_stream1 = std.io.fixedBufferStream(want[0..]);
    var s1 = try fromBytes(
        String,
        string_stream1.reader(),
        std.testing.allocator,
        logger,
    );
    defer s1.deinit();
    try std.testing.expect(std.mem.eql(u8, "hello", s1.array_list.items));

    var string_stream2 = std.io.fixedBufferStream(want[0..]);
    var s2 = try String.fromStructBytes(
        string_stream2.reader(),
        std.testing.allocator,
        logger,
    );
    defer s2.deinit();
    try std.testing.expect(std.mem.eql(u8, "hello", s2.array_list.items));
}

fn getByteCount(value: anytype) usize {
    return switch (@typeInfo(@TypeOf(value))) {
        .Struct => return value.byteCount(),
        else => return @sizeOf(@TypeOf(value)),
    };
}

fn writeBytes(
    comptime T: type,
    self: *const T,
    writer: anytype,
    logger: Logger,
) !void {

    // In CQL, frame is big-endian (network byte order) https://github.com/apache/cassandra/blob/5d4bcc797af/doc/native_protocol_v5.spec#L232
    // So, we need to convert it to little-endian on little-endian machines

    return switch (@typeInfo(T)) {
        .Pointer => {
            logger.debug("writeBytes:Pointer", .{});
            // TODO: If it is a slice:
            // TODO:   First write the length of the slice
            // TODO:   Then write the elements of the slice
            unreachable;
        },
        .Struct => {
            logger.debug("writeBytes:Struct", .{});
            if (std.meta.hasMethod(T, "writeStructBytes")) {
                return try self.writeStructBytes(writer, logger);
            }
            inline for (std.meta.fields(T)) |f| {
                const field = @field(self, f.name);
                // TODO: maybe optimize bytes slices?
                try writeBytes(f.type, &field, writer, logger);
            }
        },
        else => {
            const size = sizeOfExcludingPadding(T);
            var bytes = std.mem.toBytes(self.*);
            std.mem.reverse(u8, bytes[0..size]);
            try writer.writeAll(bytes[0..]);
            prettyBufBytes(T, bytes[0..], logger, "writeBytes");
        },
    };
}

const ClientState = struct {
    negotiated_protocol_version: ?u8 = null,
};

const ClientConnection = struct {
    allocator: std.mem.Allocator,
    client: *net.Server.Connection,
    logger: Logger,
    client_state: ClientState = ClientState{
        .negotiated_protocol_version = null,
    },

    fn init(allocator: std.mem.Allocator, client: *net.Server.Connection, logger: Logger) ClientConnection {
        logger.debug("client connected: {any}", .{client.address});
        return ClientConnection{
            .logger = logger,
            .allocator = allocator,
            .client = client,
        };
    }

    fn deinit(self: *ClientConnection) void {
        self.logger.debug("client disconnected: {any}", .{self.client.address});
        self.client.stream.close();
    }

    fn handleSTARTUP(self: *@This()) !void {
        self.logger.debug("handleSTARTUP...", .{});

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const req_frame = try fromBytes(
            Frame(StartupBody),
            self.client.stream.reader(),
            allocator,
            self.logger,
        );

        for (req_frame.body.value.array_list.items) |item| {
            self.logger.debug("STARTUP keyvalue {s}={s}", .{ item.key.array_list.items, item.value.array_list.items });
        }

        var bw = std.io.bufferedWriter(self.client.stream.writer());
        defer bw.flush() catch unreachable;

        var resp_frame = FrameHeader{
            .version = SupportedNativeCqlProtocolVersion | ResponseFlag,
            .flags = 0x00,
            .stream = req_frame.stream,
            .opcode = Opcode.READY,
            .length = 0,
        };

        try writeBytes(
            FrameHeader,
            &resp_frame,
            bw.writer(),
            self.logger,
        );
    }

    fn handleOPTIONS(self: *@This()) !void {
        // NOTE: I thinkg this is how the client sends the initial handshake options request:
        // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/protocol.py?L490-495
        // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/connection.py?L1312-1314
        //   - send_msg: https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/connection.py?L1059:9-1059:17
        // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/io/asyncorereactor.py?L370:14-370:35
        // class Connection https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/connection.py?L661

        self.logger.debug("handleOPTIONS...", .{});

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        const req_frame = try fromBytes(
            FrameHeader,
            self.client.stream.reader(),
            allocator,
            self.logger,
        );
        self.logger.debug("received frame: {any}", .{req_frame});

        var bw = std.io.bufferedWriter(self.client.stream.writer());
        defer bw.flush() catch unreachable;

        if (req_frame.version != SupportedNativeCqlProtocolVersion) {
            var msg_buf = std.ArrayList(u8).init(allocator);
            defer msg_buf.deinit();
            try std.fmt.format(msg_buf.writer(), "Invalid or unsupported protocol version ({d}); the lowest supported version is 5 and the greatest is 5", .{req_frame.version});

            var resp_frame = Frame(ErrorBody){
                .version = SupportedNativeCqlProtocolVersion | ResponseFlag,
                .flags = 0x00,
                .stream = req_frame.stream,
                .opcode = Opcode.ERROR,
                .body = PrefixedTypedBytes(u32, ErrorBody).fromValue(ErrorBody{
                    .code = ErrorCode.PROTOCOL_ERROR,
                    .message = try String.fromSlice(allocator, msg_buf.items[0..]),
                }),
            };

            try writeBytes(
                Frame(ErrorBody),
                &resp_frame,
                bw.writer(),
                self.logger,
            );
        } else {
            const resp_frame = Frame(StringMultimap){
                .version = SupportedNativeCqlProtocolVersion | ResponseFlag,
                .flags = 0x00,
                .stream = req_frame.stream,
                .opcode = Opcode.SUPPORTED,
                .body = PrefixedTypedBytes(u32, StringMultimap).fromValue(try StringMultimap.fromSlice(
                    allocator,
                    &[_]StringListPair{
                        .{
                            .key = try String.fromSlice(allocator, "PROTOCOL_VERSIONS"),
                            .value = try StringList.fromSlice(allocator, &[_]String{
                                // Spec: 'The body of a SUPPORTED message ... also includes "PROTOCOL_VERSIONS"':
                                try String.fromSlice(allocator, "5/v5"),
                            }),
                        },
                        .{
                            .key = try String.fromSlice(allocator, "CQL_VERSION"),
                            .value = try StringList.fromSlice(allocator, &[_]String{
                                // Spec: 'This option is mandatory and currently the only version supported is "3.0.0"'
                                try String.fromSlice(allocator, "3.0.0"),
                            }),
                        },
                        .{
                            .key = try String.fromSlice(allocator, "COMPRESSION"),
                            .value = try StringList.fromSlice(allocator, &[_]String{
                                // Spec: "As of v5 of the protocol, the only compression available is lz4"
                                try String.fromSlice(allocator, "lz4"),
                            }),
                        },
                    },
                )),
            };
            self.client_state.negotiated_protocol_version = SupportedNativeCqlProtocolVersion;
            try writeBytes(
                Frame(StringMultimap),
                &resp_frame,
                bw.writer(),
                self.logger,
            );
        }
        return;
    }
};

const CqlServer = struct {
    net_server: *net.Server,
    state_machine: StateMachine,
    logger: Logger,
    allocator: std.mem.Allocator,

    fn newServer(allocator: std.mem.Allocator, port: u16) !CqlServer {
        const logger = Logger.init(std.log.Level.debug, "CqlServer");

        // TODO: maybe receive a *std.net.Server as parameter
        const address = try net.Address.parseIp("127.0.0.1", port);
        logger.debug("Address: {}", .{address});
        const s = try allocator.create(std.net.Server);
        s.* = try address.listen(.{ .reuse_address = true });

        return .{
            .net_server = s,
            .state_machine = StateMachine{},
            .logger = logger,
            .allocator = allocator,
        };
    }

    fn deinit(
        self: *@This(),
    ) void {
        self.net_server.deinit();
        self.logger.deinit();
        self.allocator.destroy(self.net_server);
    }

    fn acceptClient(
        self: *@This(),
    ) !void {
        self.logger.debug("waiting for next client...", .{});
        var client = try self.net_server.accept();

        try self.handleClient(&client);
    }

    fn handleClient(self: *@This(), client: *net.Server.Connection) !void {
        var client_conn = ClientConnection.init(self.allocator, client, self.logger);
        defer client_conn.deinit();

        while (true) {
            // std.time.sleep(2 * 1000 * 1000 * 1000);
            if (client_conn.client_state.negotiated_protocol_version == null) {
                client_conn.handleOPTIONS() catch |err| switch (err) {
                    error.EndOfStream => {
                        self.logger.debug("client disconnected", .{});
                        return;
                    },
                    else => unreachable,
                };
            } else {
                client_conn.handleSTARTUP() catch |err| switch (err) {
                    error.EndOfStream => {
                        self.logger.debug("client disconnected", .{});
                        return;
                    },
                    else => unreachable,
                };
                self.logger.debug("TODO: We are connected", .{});

                while (true) {
                    self.logger.debug("expecting V5Frame", .{});
                    // check header of next message
                    var arena = std.heap.ArenaAllocator.init(self.allocator);
                    defer arena.deinit();
                    const allocator = arena.allocator();

                    const req_frame = try fromBytes(
                        V5Frame,
                        client.stream.reader(),
                        allocator,
                        self.logger,
                    );
                    self.logger.debug("received V5Frame: {any}", .{req_frame});
                    // TODO: depending on the opcode, parse req_frame.payload_raw_bytes
                    var stream = std.io.fixedBufferStream(req_frame.payload_raw_bytes.items);
                    switch (req_frame.payload_frame_header.opcode) {
                        Opcode.QUERY => {
                            const query_body = try fromBytes(
                                Query,
                                stream.reader(),
                                allocator,
                                self.logger,
                            );
                            self.logger.debug("received QUERY: {any}", .{query_body});

                            // respond:
                            var bw = std.io.bufferedWriter(client.stream.writer());
                            defer bw.flush() catch unreachable;

                            // TODO: parse query_body.query_string
                            const payload = std.ArrayList(u8).init(allocator); // TODO: fill this with the response payload

                            const resp_frame = V5Frame{
                                // TODO: more fields
                                .is_self_contained_flag = 1,
                                .payload_frame_header = .{
                                    .version = client_conn.client_state.negotiated_protocol_version.? | ResponseFlag,
                                    .flags = 0x00,
                                    .stream = req_frame.payload_frame_header.stream,
                                    .opcode = Opcode.RESULT,
                                    .length = @intCast(payload.items.len),
                                },
                                .payload_raw_bytes = payload,
                            };
                            try writeBytes(
                                V5Frame,
                                &resp_frame,
                                bw.writer(),
                                self.logger,
                            );
                        },
                        Opcode.REGISTER => {
                            self.logger.debug("opcode: REGISTER", .{});
                            const register_body = try fromBytes(
                                StringList,
                                stream.reader(),
                                allocator,
                                self.logger,
                            );
                            self.logger.debug("register_body: {any}", .{register_body});
                            for (register_body.array_list.items) |item| {
                                self.logger.debug("REGISTER {s}", .{item.array_list.items});
                            }

                            // respond:
                            var bw = std.io.bufferedWriter(client.stream.writer());
                            defer bw.flush() catch unreachable;

                            const resp_frame = V5Frame{
                                // TODO: more fields
                                .is_self_contained_flag = 1,
                                .payload_frame_header = .{
                                    .version = client_conn.client_state.negotiated_protocol_version.? | ResponseFlag,
                                    .flags = 0x00,
                                    .stream = req_frame.payload_frame_header.stream,
                                    .opcode = Opcode.READY,
                                    .length = 0,
                                },
                                .payload_raw_bytes = std.ArrayList(u8).init(allocator),
                            };
                            try writeBytes(
                                V5Frame,
                                &resp_frame,
                                bw.writer(),
                                self.logger,
                            );
                        },
                        else => {
                            self.logger.debug("opcode: {any}", .{req_frame.payload_frame_header.opcode});
                            unreachable;
                        },
                    }
                }

                return;
            }
        }
    }
};

pub fn main() !void {
    const trace = tracy.trace(@src());
    defer trace.end();

    // copied from https://sourcegraph.com/github.com/zigtools/zls@dd307c59bf32e2cec323235c776e07fa36efb465/-/blob/src/main.zig?L235-236
    var allocator_state = std.heap.GeneralPurposeAllocator(.{ .stack_trace_frames = 10 }){};
    var tracy_state = if (tracy.enable_allocation) tracy.tracyAllocator(allocator_state.allocator()) else void{};
    const inner_allocator: std.mem.Allocator = if (tracy.enable_allocation) tracy_state.allocator() else allocator_state.allocator();
    defer {
        const deinit_status = allocator_state.deinit();
        if (deinit_status == .leak) {
            std.log.err("There is memory leak\n", .{});
        }
    }

    var srv = try CqlServer.newServer(inner_allocator, 9042);
    defer srv.deinit();

    while (true) {
        try srv.acceptClient();
    }
}

test "let's see how struct bytes work" {
    const logger = Logger.init(std.log.Level.debug, "unit test");
    std.testing.log_level = std.log.Level.info;
    const frame1 = FrameHeader{
        .version = 1,
        .flags = 2,
        .stream = 3,
        .opcode = Opcode.READY,
        .length = 5,
    };
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try writeBytes(
        FrameHeader,
        &frame1,
        buf.writer(),
        logger,
    );
    var buf_reader = std.io.fixedBufferStream(buf.items);
    logger.debug("buf.items.len = {d}", .{buf.items.len});
    logger.debug("buf.items     = {x}", .{buf.items});
    try std.testing.expectEqual(sizeOfExcludingPadding(FrameHeader), buf.items.len);

    prettyBytes(buf.items[0..], std.log, "frame1");
    const want = [9]u8{ 1, 2, 0, 3, @intFromEnum(Opcode.READY), 0, 0, 0, 5 };
    try std.testing.expect(std.mem.eql(u8, want[0..], buf.items));

    const frame2 = try fromBytes(
        FrameHeader,
        buf_reader.reader(),
        std.testing.allocator,
        logger,
    );
    logger.debug("frame2: {any}", .{frame2});
    try std.testing.expectEqual(frame1, frame2);

    // error body
    var message = try String.fromSlice(std.testing.allocator, "error message here");
    const error_body1 = ErrorBody{
        .code = ErrorCode.PROTOCOL_ERROR,
        .message = message,
    };
    defer message.deinit();

    var error_body_buf = std.ArrayList(u8).init(std.testing.allocator);
    defer error_body_buf.deinit();

    try writeBytes(
        ErrorBody,
        &error_body1,
        error_body_buf.writer(),
        logger,
    );
    var error_body_buf_reader = std.io.fixedBufferStream(error_body_buf.items);
    logger.debug("error_body_buf = {x}", .{error_body_buf.items});
    var error_body2 = try fromBytes(
        ErrorBody,
        error_body_buf_reader.reader(),
        std.testing.allocator,
        logger,
    );
    defer error_body2.message.deinit();
    logger.debug("error_body2: {any}", .{error_body2});
    logger.debug("error_body1: {any}", .{error_body1});
    try std.testing.expectEqual(error_body1.code, error_body2.code);
    try std.testing.expect(std.mem.eql(u8, error_body1.message.array_list.items, error_body2.message.array_list.items));
}

const Logger = struct {
    const Self = @This();
    const underlying_writer = std.io.getStdErr().writer();

    // fields:
    level: std.log.Level,
    prefix: []const u8 = undefined,

    fn init(comptime level: std.log.Level, comptime prefix: []const u8) Self {
        return Self{
            .level = level,
            .prefix = prefix,
        };
    }
    fn deinit(_: Self) void {
        // noop
    }

    // fn writerFn(self: Self) std.io.Writer {
    //     return std.io.Writer(self, error{}, self.debug);
    // }

    fn debug(self: Self, comptime format: []const u8, args: anytype) void {
        if (self.level != .debug) {
            return;
        }
        var bw = std.io.bufferedWriter(underlying_writer);
        const writer = bw.writer();
        std.fmt.format(writer, "[{s}] ", .{self.prefix}) catch return;
        std.fmt.format(writer, format ++ "\n", args) catch return;
        bw.flush() catch return;
    }
};

test "test initial cql handshake" {
    const TestCqlClient = struct {
        fn send(server_address: net.Address) !void {
            const logger = Logger.init(std.log.Level.debug, "TestCqlClient");
            defer logger.deinit();

            logger.debug("debug message before", .{});
            const socket = try net.tcpConnectToAddress(server_address);
            defer socket.close();

            const request_fram = FrameHeader{
                .version = 0x66,
                .flags = 0,
                .stream = 0,
                .opcode = Opcode.OPTIONS,
                .length = 0,
            };
            try writeBytes(
                FrameHeader,
                &request_fram,
                socket.writer(),
                logger,
            );

            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const allocator = arena.allocator();

            logger.debug("reading response 1", .{});
            const response = try fromBytes(
                Frame(ErrorBody),
                socket.reader(),
                allocator,
                logger,
            );
            try std.testing.expect(std.mem.eql(u8, "Invalid or unsupported protocol version (102); the lowest supported version is 5 and the greatest is 5", response.body.value.message.array_list.items));
        }
    };

    var srv = try CqlServer.newServer(std.testing.allocator, 9042);
    defer srv.deinit();

    const t = try std.Thread.spawn(.{}, TestCqlClient.send, .{srv.net_server.listen_address});
    defer t.join();

    try srv.acceptClient();
}

const TokenType = enum {
    ILLEGAL,
    EOF,
    NEWLINE,

    literal_beg,
    IDENTIFIER, // table1
    INTEGER, // 12345
    FLOAT, // 123.45
    STRING, // 'abc'
    literal_end,

    operator_beg,
    EQ, // =
    NEQ, // !=
    LEQ, // <=
    GEQ, // >=
    LSS, // <
    GTR, // >
    STAR, // *
    DOT, // .
    operator_end,

    keyword_beg,
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    FROM,
    WHERE,
    AND,
    OR,
    IN,
    NOT,
    keyword_end,
};

const EndOfInput = 0;

const Token = struct {
    typ: TokenType,
    pos: u64,
    lit: []const u8,
};

const Scanner = struct {
    const Self = @This();
    input: []const u8,

    currentPosition: u64 = 0,
    nextPosition: u64 = 0,

    currentChar: u8 = 0,
    nextChar: u8 = 0,

    currentToken: Token = Token{
        .typ = TokenType.ILLEGAL,
        .pos = 0,
        .lit = "",
    },

    fn init(self: *Self) void {
        self.readChar();
    }

    fn isNumericalStart(current: u8) bool {
        return current >= '1' and current <= '9';
    }

    fn isNumericalMiddle(current: u8) bool {
        return current >= '0' and current <= '9';
    }

    fn isIdentifierStart(current: u8) bool {
        return (current >= 'a' and current <= 'z') or (current >= 'A' and current <= 'Z') or current == '_';
    }

    fn isIdentifierMiddle(current: u8) bool {
        return isIdentifierStart(current) or (current >= '0' and current <= '9');
    }

    fn readChar(self: *Self) void {
        if (self.nextPosition >= self.input.len) {
            self.currentChar = EndOfInput;
        } else {
            self.currentChar = self.input[self.nextPosition];
        }

        self.currentPosition = self.nextPosition;
        self.nextPosition += 1;

        if (self.nextPosition >= self.input.len) {
            self.nextChar = EndOfInput;
        } else {
            self.nextChar = self.input[self.nextPosition];
        }
    }

    fn scanIdentifier(self: *Self) !Token {
        const start = self.currentPosition;
        while (true) {
            self.readChar();
            const current = self.input[self.currentPosition];
            if (!Scanner.isIdentifierMiddle(current)) {
                break;
            }
        }

        const lit = self.input[start..self.currentPosition];

        if (std.mem.eql(u8, "SELECT", lit)) {
            return Token{
                .typ = TokenType.SELECT,
                .pos = start,
                .lit = lit,
            };
        } else if (std.mem.eql(u8, "FROM", lit)) {
            return Token{
                .typ = TokenType.FROM,
                .pos = start,
                .lit = lit,
            };
        } else if (std.mem.eql(u8, "WHERE", lit)) {
            return Token{
                .typ = TokenType.WHERE,
                .pos = start,
                .lit = lit,
            };
        } else {
            return Token{
                .typ = TokenType.IDENTIFIER,
                .pos = start,
                .lit = lit,
            };
        }
    }

    fn scan(self: *Self) !Token {
        switch (self.currentChar) {
            ' ' => {
                while (self.currentChar == ' ') {
                    self.readChar();
                }
                return self.scan();
            },
            '*' => {
                const token = Token{
                    .typ = TokenType.STAR,
                    .pos = self.currentPosition,
                    .lit = "*",
                };
                self.readChar();
                return token;
            },
            '.' => {
                const token = Token{
                    .typ = TokenType.DOT,
                    .pos = self.currentPosition,
                    .lit = ".",
                };
                self.readChar();
                return token;
            },
            '=' => {
                const token = Token{
                    .typ = TokenType.EQ,
                    .pos = self.currentPosition,
                    .lit = "=",
                };
                self.readChar();
                return token;
            },
            else => {
                if (Scanner.isIdentifierStart(self.currentChar)) {
                    return self.scanIdentifier();
                } else if (Scanner.isNumericalStart(self.currentChar)) {
                    const start = self.currentPosition;
                    while (Scanner.isNumericalMiddle(self.currentChar)) {
                        self.readChar();
                    }
                    const lit = self.input[start..self.currentPosition];
                    return Token{
                        .typ = TokenType.INTEGER,
                        .pos = start,
                        .lit = lit,
                    };
                }
            },
        }

        return Token{
            .typ = TokenType.EOF,
            .pos = self.currentPosition,
            .lit = "",
        };
    }
};

test "test scanning a SELECT statement" {
    var scanner = Scanner{
        .input = "SELECT * FROM ks.users WHERE id = 12",
    };
    scanner.init();

    var token = try scanner.scan();
    try std.testing.expectEqual(TokenType.SELECT, token.typ);
    assert(std.mem.eql(u8, "SELECT", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.STAR, token.typ);
    assert(std.mem.eql(u8, "*", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.FROM, token.typ);
    assert(std.mem.eql(u8, "FROM", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.IDENTIFIER, token.typ);
    assert(std.mem.eql(u8, "ks", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.DOT, token.typ);
    assert(std.mem.eql(u8, ".", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.IDENTIFIER, token.typ);
    assert(std.mem.eql(u8, "users", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.WHERE, token.typ);
    assert(std.mem.eql(u8, "WHERE", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.IDENTIFIER, token.typ);
    assert(std.mem.eql(u8, "id", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.EQ, token.typ);
    assert(std.mem.eql(u8, "=", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.INTEGER, token.typ);
    assert(std.mem.eql(u8, "12", token.lit));

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.EOF, token.typ);

    token = try scanner.scan();
    try std.testing.expectEqual(TokenType.EOF, token.typ);
}

const Parser = struct {
    const Self = @This();

    scanner: *Scanner,
    logger: Logger = Logger.init(std.log.Level.debug, "Parser"),
    allocator: std.mem.Allocator,

    fn init(self: *Self) void {
        self.scanner.init();
    }
    fn parseSelectClause(self: *Self) !std.ArrayList(SelectClause) {
        const token = try self.scanner.scan();
        try std.testing.expectEqual(TokenType.STAR, token.typ);
        // return token;
        unreachable;
    }

    fn parse(self: *Self) !ParserResult {
        const token = try self.scanner.scan();
        switch (token.typ) {
            TokenType.SELECT => {
                // const selectClauseToken = try self.parseSelectClause();
                const selectClauseToken = try self.scanner.scan();
                assert(std.mem.eql(u8, "*", selectClauseToken.lit));

                const fromToken = try self.scanner.scan();
                assert(TokenType.FROM == fromToken.typ);

                const keyspaceToken = try self.scanner.scan();
                assert(TokenType.IDENTIFIER == keyspaceToken.typ);

                var keyspaceLit = std.ArrayList(u8).init(self.allocator);
                try keyspaceLit.writer().writeAll(keyspaceToken.lit);

                const periodToken = try self.scanner.scan();
                assert(TokenType.DOT == periodToken.typ);

                const tableToken = try self.scanner.scan();
                assert(TokenType.IDENTIFIER == tableToken.typ);

                var tableLit = std.ArrayList(u8).init(self.allocator);
                try tableLit.writer().writeAll(tableToken.lit);

                return ParserResult{
                    .SelectQuery = SelectQuery{
                        // .select_clause = selectClauseToken,
                        .keyspace = keyspaceLit,
                        .table = tableLit,
                    },
                };
            },
            else => unreachable,
        }
    }
};

const SelectClause = struct {
    const Self = @This();

    column: std.ArrayList(u8),

    fn deinit(self: *Self) void {
        self.column.deinit();
    }
};

const ParserResult = union {
    SelectQuery: SelectQuery,
    InsertQuery: InsertQuery,
};

const SelectQuery = struct {
    const Self = @This();

    // select_clause: std.ArrayList(SelectClause),
    keyspace: std.ArrayList(u8),
    table: std.ArrayList(u8),

    fn deinit(self: *Self) void {
        self.keyspace.deinit();
        self.table.deinit();
    }
};

const InsertQuery = struct {
    const Self = @This();

    select_clause: std.ArrayList(SelectClause),
    keyspace: std.ArrayList(u8),
    table: std.ArrayList(u8),

    fn deinit(self: *Self) void {
        self.keyspace.deinit();
        self.table.deinit();
    }
};

test "test parsing a SELECT statement" {
    var scanner = Scanner{
        .input = "SELECT * FROM ks.users WHERE id = 12",
    };
    var parser = Parser{
        .scanner = &scanner,
        .allocator = std.testing.allocator,
    };
    parser.init();

    var got = try parser.parse();
    defer got.SelectQuery.deinit();

    std.debug.print("got={any}\n", .{got.SelectQuery});
    assert(std.mem.eql(u8, "ks", got.SelectQuery.keyspace.items));
    assert(std.mem.eql(u8, "users", got.SelectQuery.table.items));
}
