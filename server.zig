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

fn prettyStructBytes(
    comptime T: type,
    self: *const T,
    logger: Logger,
    prefix: []const u8,
) void {
    // writer.print("BEGIN\n", .{}) catch unreachable;
    // defer writer.print("END\n", .{}) catch unreachable;
    const buf = std.mem.sliceAsBytes(@as(*const [1]T, self)[0..1]);
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

// TODO: just found this https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/protocol.py?L127-129
const ErrorBody = packed struct {
    code: ErrorCode,
    length: u16,
    // TONDO: message: []const u8,
};

fn fromBytes(
    comptime T: type,
    buf: []u8,
    self: *T,
    logger: Logger,
) void {
    inline for (std.meta.fields(T)) |f| {
        const s = buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)]; // TODO: if another struct is nested, @sizeOf includes padding, so we need to use sizeOfExcludingPadding
        std.mem.reverse(u8, s);
        @field(self, f.name) = std.mem.bytesAsValue(f.type, s).*;
    }
    prettyStructBytes(T, self, logger, "fromBytes");
}

fn sizeOfExcludingPadding(comptime T: type) @TypeOf(@sizeOf(T)) {
    // We are adding 7 bits, in case the result is not a multiple of 8
    return (@bitSizeOf(T) + 7) / 8;
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
const StringPair = struct { key: String, value: String };
const BytePair = struct { key: String, value: String };
const StringMap = PrefixedSlice(Short, StringPair);
const StringMultimap = PrefixedSlice(Short, StringList);
const BytesMap = PrefixedSlice(Short, BytePair);

fn PrefixedSlice(comptime S: type, comptime T: type) type {
    return struct {
        const NewType = @This();
        value: []const T,
        fn new(items: []const T) NewType {
            return .{
                .value = items,
            };
        }
        pub fn writeStructBytes(
            self: *const NewType,
            writer: anytype,
            logger: Logger,
        ) !void {
            const len = @as(S, @truncate(self.value.len));
            try writeBytes(S, &len, writer, logger);
            for (self.value) |item| {
                try writeBytes(T, &item, writer, logger);
            }
        }
    };
}

test "test PrefixedSlice" {
    const s = String.new("hello");
    try std.testing.expectEqual("hello", s.value);
    const logger = Logger.init(std.log.Level.debug, "unit test");
    logger.debug("s = {any}", .{s});

    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();

    try writeBytes(String, &s, buf.writer(), logger);
    logger.debug("buf.items.len = {d}", .{buf.items.len});
    logger.debug("buf.items     = {x}", .{buf.items});
    try std.testing.expectEqual(@sizeOf(Short) + s.value.len, buf.items.len);

    const want = [7]u8{ 0, 5, 'h', 'e', 'l', 'l', 'o' };
    try std.testing.expect(std.mem.eql(u8, want[0..], buf.items));
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
                logger.debug("writeStructBytes", .{});
                return try self.writeStructBytes(writer, logger);
            }
            logger.debug("no writeStructBytes", .{});
            prettyStructBytes(T, self, logger, "writeBytes");
            inline for (std.meta.fields(T)) |f| {
                const field = @field(self, f.name);
                try writeBytes(f.type, &field, writer, logger);
            }
        },
        else => {
            logger.debug("else", .{});
            var bytes = std.mem.toBytes(self.*);
            logger.debug("bytes new: {x}", .{bytes});
            std.mem.reverse(u8, &bytes);
            try writer.writeAll(bytes[0..]);
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

    fn handleOPTIONS(self: *@This()) !u64 {
        // NOTE: I thinkg this is how the client sends the initial handshake options request:
        // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/protocol.py?L490-495
        // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/connection.py?L1312-1314
        //   - send_msg: https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/connection.py?L1059:9-1059:17
        // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/io/asyncorereactor.py?L370:14-370:35
        // class Connection https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/connection.py?L661
        self.logger.debug("reading bytes...", .{});
        var buf: [sizeOfExcludingPadding(FrameHeader)]u8 = undefined;
        const n = try self.client.stream.reader().readAll(&buf);
        if (n == 0) return 0;
        const bytes_read: u64 = n;

        prettyBytes(buf[0..n], self.logger, "received bytes");

        var req_frame: FrameHeader = undefined;
        fromBytes(
            FrameHeader,
            buf[0..],
            &req_frame,
            self.logger,
        );
        self.logger.debug("received frame: {any}", .{req_frame});

        // const version_str: [2]u8 = undefined;
        // std.fmt.format(version_str, "received frame: {any}\n", .{req_frame.version});
        // const message = "Invalid or unsupported protocol version (" ++ version_str ++ "); the lowest supported version is 5 and the greatest is 5"; // TODO: replace ? with req_frame.version

        if (req_frame.version != SupportedNativeCqlProtocolVersion) {
            const message = "Invalid or unsupported protocol version (66); the lowest supported version is 5 and the greatest is 5"; // TODO: replace ? with req_frame.version
            const body_len = sizeOfExcludingPadding(ErrorBody) + message.len;
            const resp_frame = FrameHeader{
                .version = SupportedNativeCqlProtocolVersion | ResponseFlag,
                .flags = 0x00,
                .stream = req_frame.stream,
                .opcode = Opcode.ERROR,
                .length = body_len,
            };
            try writeBytes(
                FrameHeader,
                &resp_frame,
                self.client.stream.writer(),
                self.logger,
            );

            const error_body = ErrorBody{
                .code = ErrorCode.PROTOCOL_ERROR,
                .length = message.len,
                // .message = message,
            };

            try writeBytes(
                ErrorBody,
                &error_body,
                self.client.stream.writer(),
                self.logger,
            );
            try self.client.stream.writer().writeAll(message);
        } else {
            const message = "TODO";
            const body_len = message.len;
            const resp_frame = FrameHeader{
                .version = SupportedNativeCqlProtocolVersion | ResponseFlag,
                .flags = 0x00,
                .stream = req_frame.stream,
                .opcode = Opcode.SUPPORTED,
                .length = body_len,
            };
            self.client_state.negotiated_protocol_version = SupportedNativeCqlProtocolVersion;
            try writeBytes(
                FrameHeader,
                &resp_frame,
                self.client.stream.writer(),
                self.logger,
            );
            try self.client.stream.writer().writeAll(message);
        }
        return bytes_read;
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
                if (try client_conn.handleOPTIONS() == 0) {
                    break;
                }
            } else {
                // TODO: frame messages with client_conn.client_state.negotiated_protocol_version
                self.logger.debug("TODO: We are connected", .{});
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
    logger.debug("buf.items.len = {d}", .{buf.items.len});
    logger.debug("buf.items     = {x}", .{buf.items});
    try std.testing.expectEqual(sizeOfExcludingPadding(FrameHeader), buf.items.len);

    prettyBytes(buf.items[0..], std.log, "frame1");
    const want = [9]u8{ 1, 2, 0, 3, @intFromEnum(Opcode.READY), 0, 0, 0, 5 };
    try std.testing.expect(std.mem.eql(u8, want[0..], buf.items));

    var frame2 = FrameHeader{
        .version = 0,
        .flags = 0,
        .stream = 0,
        .opcode = Opcode.ERROR,
        .length = 0,
    };
    fromBytes(
        FrameHeader,
        buf.items[0..],
        &frame2,
        logger,
    );
    logger.debug("frame2: {any}", .{frame2});
    try std.testing.expectEqual(frame1, frame2);

    // error body
    const error_body1 = ErrorBody{
        .code = ErrorCode.PROTOCOL_ERROR,
        .length = 12345,
    };
    var error_body2: ErrorBody = undefined;

    var error_body_buf = std.ArrayList(u8).init(std.testing.allocator);
    defer error_body_buf.deinit();

    try writeBytes(
        ErrorBody,
        &error_body1,
        &error_body_buf.writer(),
        logger,
    );
    fromBytes(
        ErrorBody,
        error_body_buf.items[0..],
        &error_body2,
        logger,
    );
    try std.testing.expectEqual(error_body1, error_body2);
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

            logger.debug("reading response 1", .{});
            var buf: [sizeOfExcludingPadding(FrameHeader)]u8 = undefined;
            const nread = try socket.reader().readAll(&buf);
            logger.debug("nread={}", .{nread});
            var response_frame: FrameHeader = undefined;
            fromBytes(
                FrameHeader,
                buf[0..],
                &response_frame,
                logger,
            );

            logger.debug("reading response 2", .{});
            var buf2: [sizeOfExcludingPadding(ErrorBody)]u8 = undefined;
            _ = try socket.reader().readAll(&buf2);
            var error_body: ErrorBody = undefined;
            fromBytes(
                ErrorBody,
                buf2[0..],
                &error_body,
                logger,
            );
            logger.debug("error_body={}", .{error_body});
            logger.debug("reading response 3", .{});

            var message: [512]u8 = undefined;
            if (error_body.length > message.len) {
                logger.debug("Error message has length {d}, truncating to {d} and discard the remaining bytes", .{ error_body.length, message.len });
            }
            const n = try socket.reader().readAll(message[0..error_body.length]);
            const message_str = message[0..error_body.length];
            logger.debug("got3 ({d} bytes): {s}", .{ n, message_str });

            // discard the remaining bytes
            logger.debug("error_body.length={d}, message_str.len={d}", .{ error_body.length, message_str.len });
            if (error_body.length > message_str.len) {
                for (0..error_body.length - message_str.len) |_| {
                    // TODO: print the discarded bytes
                    logger.debug("reading byte", .{});
                    _ = try socket.reader().readByte();
                }
            }
        }
    };

    var srv = try CqlServer.newServer(std.testing.allocator, 0);
    defer srv.deinit();

    const t = try std.Thread.spawn(.{}, TestCqlClient.send, .{srv.net_server.listen_address});
    defer t.join();

    try srv.acceptClient();
}
