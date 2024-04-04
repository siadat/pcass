const std = @import("std");
const net = std.net;
const tracy = @import("tracy.zig");
const builtin = @import("builtin");

const ResponseFlag = 0x80; // == 0b10000000
const SupportedNativeCqlProtocolVersion = 0x05;

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

const V5Frame = struct {
    const Self = @This();

    // payload_length: u17,
    is_self_contained_flag: u1,
    // payload: T,
    raw_bytes: std.ArrayList(u8),
    frame_header: FrameHeader,

    pub fn writeStructBytes(
        // NOTE: this is different from other writeStructBytes, this diverged because
        // when parsing with fromStructBytes, I don't know the type of the payload,
        // but I do know it when writing with writeStructBytes.
        comptime T: type,
        payload: T,
        self: *const Self,
        writer: anytype,
        logger: Logger,
    ) !void {
        const payload_length: u17 = @truncate(getByteCount(payload));
        const header_padding: u6 = 0;
        const header_crc24: u24 = 0; // TODO
        const payload_crc24: u24 = 0; // TODO

        try writeBytes(u17, &payload_length, writer, logger);
        try writeBytes(u1, &self.is_self_contained_flag, writer, logger);
        try writeBytes(u6, &header_padding, writer, logger);
        try writeBytes(u24, &header_crc24, writer, logger);
        try writeBytes(T, &payload, writer, logger);
        try writeBytes(u32, &payload_crc24, writer, logger);
    }

    fn deinit(self: *const Self) void {
        self.raw_bytes.deinit();
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
            logger.debug("first_three_bytes: 0x{x:0>2} 0b{b:0>8}", .{ c, c });
        }

        // const T = packed struct(u) {}
        // std.mem.bytesAsValue(T, buf[0..]).*;
        const payload_length = std.mem.readPackedInt(u17, &first_three_bytes, 0, std.builtin.Endian.little);
        const is_self_contained_flag = std.mem.readPackedInt(u1, &first_three_bytes, @bitSizeOf(@TypeOf(payload_length)), std.builtin.Endian.little);
        // NTOE: we discard header_padding

        logger.debug("length: {d}", .{payload_length});
        logger.debug("is_self_contained_flag: {d}", .{is_self_contained_flag});

        const header_crc24 = try fromBytes(u24, reader, allocator, logger);
        logger.debug("header_crc24: {x}", .{header_crc24});

        const frame_header = try fromBytes(FrameHeader, reader, allocator, logger);
        logger.debug("frame_header: {any}", .{frame_header});

        var raw_bytes = std.ArrayList(u8).init(allocator);
        try raw_bytes.resize(frame_header.length);

        const raw_payload_n = try reader.readAll(raw_bytes.items);
        if (raw_payload_n < frame_header.length) {
            return error.EndOfStream;
        }
        prettyBufBytes(u8, raw_bytes.items, logger, "raw_bytes");

        const payload_crc24 = try fromBytes(u32, reader, allocator, logger);
        _ = payload_crc24;
        return .{
            .is_self_contained_flag = is_self_contained_flag,
            .raw_bytes = raw_bytes,
            .frame_header = frame_header,
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
const Option = unreachable; // PrefixedSlice(Short, Byte);
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
            for (len) |_| {
                const item = try fromBytes(T, reader, allocator, logger);
                try array_list.append(item);
            }
            return .{
                .allocator = allocator,
                .array_list = array_list,
            };
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
            logger.debug("Pointer", .{});
            // TODO: If it is a slice:
            // TODO:   First write the length of the slice
            // TODO:   Then write the elements of the slice
            unreachable;
        },
        .Struct => {
            logger.debug("Struct", .{});
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
            var bytes = std.mem.toBytes(self.*);
            std.mem.reverse(u8, &bytes);
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
                // TODO: frame messages with client_conn.client_state.negotiated_protocol_version
                self.logger.debug("TODO: We are connected", .{});

                {
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
                    // TODO: depending on the opcode, parse req_frame.raw_bytes
                    var stream = std.io.fixedBufferStream(req_frame.raw_bytes.items);
                    switch (req_frame.frame_header.opcode) {
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
                        },
                        else => unreachable,
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
