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
    comptime target_endian: std.builtin.Endian,
    buf: []u8,
    self: *T,
    logger: Logger,
) void {
    _ = target_endian;
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

// fn asBytes(ptr: anytype) std.mem.AsBytesReturnType(@TypeOf(ptr)) {
//     return std.mem.asBytes(ptr);
// }

fn writeBytes(
    comptime T: type,
    comptime struct_endian: std.builtin.Endian,
    self: *const T,
    writer: anytype,
    logger: Logger,
) !void {
    _ = struct_endian;

    // In CQL, frame is big-endian (network byte order) https://github.com/apache/cassandra/blob/5d4bcc797af/doc/native_protocol_v5.spec#L232
    // So, we need to convert it to little-endian on little-endian machines

    return switch (@typeInfo(T)) {
        .Pointer => {
            // TODO: If it is a slice:
            // TODO:   First write the length of the slice
            // TODO:   Then write the elements of the slice
            unreachable;
        },
        .Struct => {
            prettyStructBytes(T, self, logger, "writeBytes");
            inline for (std.meta.fields(T)) |f| {
                var bytes = std.mem.toBytes(@field(self, f.name));
                // std.log.info("bytes before: {x}", .{bytes});
                std.mem.reverse(u8, &bytes);
                // std.log.info("bytes after : {x}", .{bytes});
                try writer.writeAll(bytes[0..]);
            }
            // prettyBytesWithAnnotatedStruct(T, buf, std.log, "writeBytes");
            // prettyBytesWithAnnotatedStruct(T, std.mem.writeBytes(self), std.log, "toBytesDEBUG");
        },
        else => unreachable,
    };
}

pub fn copyReverse(comptime T: type, dest: []T, source: []const T) void {
    // forked from std.mem.copyBackwards
    @setRuntimeSafety(false);
    std.debug.assert(dest.len >= source.len);
    var i = source.len;
    while (i > 0) {
        i -= 1;
        dest[i] = source[source.len - i - 1];
    }
}

const ClientState = struct {
    negotiated_protocol_version: ?u8 = null,
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
        self.logger.debug("got a client", .{});
        const multi_threaded = false;

        if (multi_threaded) {
            _ = try std.Thread.spawn(
                .{},
                @This().handleClient,
                .{ self, &client },
            );
        } else {
            try self.handleClient(&client);
        }
    }

    // fn handleOPTIONS(self: *@This(), allocator: std.mem.Allocator, client: net.Server.Connection) !void {
    // }

    // fn read(_: *@This(), client: net.Server.Connection, buf: []u8) !usize {
    //     return client.stream.reader().read(buf);
    // }
    //

    fn handleClient(self: *@This(), client: *net.Server.Connection) !void {
        self.logger.debug("client connected: {any}", .{client.address});
        defer self.logger.debug("client disconnected: {any}", .{client.address});
        defer client.stream.close();

        var total_bytes_count: usize = 0;
        defer self.logger.debug("total bytes: {d}", .{total_bytes_count});

        var client_state = ClientState{
            .negotiated_protocol_version = null,
        };

        while (true) {
            self.logger.debug("reading bytes...", .{});

            if (client_state.negotiated_protocol_version == null) {
                // NOTE: I thinkg this is how the client sends the initial handshake options request:
                // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/protocol.py?L490-495
                // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/connection.py?L1312-1314
                //   - send_msg: https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/connection.py?L1059:9-1059:17
                // https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86/-/blob/cassandra/io/asyncorereactor.py?L370:14-370:35
                // class Connection https://sourcegraph.com/github.com/datastax/python-driver@7e0923a86e6b8d55f5a88698f4c1e6ded65a348b/-/blob/cassandra/connection.py?L661
                var buf: [sizeOfExcludingPadding(FrameHeader)]u8 = undefined;
                const n = try client.stream.reader().readAll(&buf);
                if (n == 0) return;
                defer total_bytes_count += n;

                prettyBytes(buf[0..n], self.logger, "received bytes");

                var req_frame: FrameHeader = undefined;
                fromBytes(
                    FrameHeader,
                    std.builtin.Endian.big,
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
                        std.builtin.Endian.big,
                        &resp_frame,
                        client.stream.writer(),
                        self.logger,
                    );

                    const error_body = ErrorBody{
                        .code = ErrorCode.PROTOCOL_ERROR,
                        .length = message.len,
                        // .message = message,
                    };

                    try writeBytes(
                        ErrorBody,
                        std.builtin.Endian.big,
                        &error_body,
                        client.stream.writer(),
                        self.logger,
                    );
                    try client.stream.writer().writeAll(message);
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
                    client_state.negotiated_protocol_version = SupportedNativeCqlProtocolVersion;
                    try writeBytes(
                        FrameHeader,
                        std.builtin.Endian.big,
                        &resp_frame,
                        client.stream.writer(),
                        self.logger,
                    );
                    try client.stream.writer().writeAll(message);
                }
            } else {
                // TODO: frame messages with client_state.negotiated_protocol_version
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
        std.builtin.Endian.big,
        &frame1,
        buf.writer(),
        logger,
    );
    logger.debug("buf.items.len = {d}", .{buf.items.len});
    logger.debug("buf.items     = {x}", .{buf.items});
    std.debug.assert(buf.items.len == sizeOfExcludingPadding(FrameHeader));
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
        std.builtin.Endian.big,
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
        std.builtin.Endian.big,
        &error_body1,
        &error_body_buf.writer(),
        logger,
    );
    fromBytes(
        ErrorBody,
        std.builtin.Endian.big,
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
                std.builtin.Endian.big,
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
                std.builtin.Endian.big,
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
                std.builtin.Endian.big,
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
