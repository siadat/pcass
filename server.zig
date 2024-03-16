const std = @import("std");
const net = std.net;
const tracy = @import("tracy.zig");
const builtin = @import("builtin");

const ResponseFlag = 0x80; // == 0b10000000
const SupportedCqlVersion = 0x05;

// enum for version 4 and 5:
const Opcode = enum(u8) {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
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

fn prettyBytesWithAnnotatedStruct(comptime T: type, buf: [sizeOfNoPadding(T)]u8, logger: anytype, prefix: []const u8) void {
    const last_field = std.meta.fields(T)[std.meta.fields(T).len - 1];
    comptime var i = 0;
    inline for (std.meta.fields(T)) |f| {
        inline for (1.., buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)]) |fi, c| {
            i += 1;
            logger.info("{s} {d: >2}/{d}: 0x{x:0>2} {d: >3} {s} {s}.{s} {d}/{d}", .{
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

fn prettyStructBytes(comptime T: type, self: *const T, logger: anytype, prefix: []const u8) void {
    // writer.print("BEGIN\n", .{}) catch unreachable;
    // defer writer.print("END\n", .{}) catch unreachable;
    const buf = std.mem.sliceAsBytes(@as(*const [1]T, self)[0..1]);
    const last_field = std.meta.fields(T)[std.meta.fields(T).len - 1];
    comptime var i = 0;
    inline for (std.meta.fields(T)) |f| {
        inline for (1.., buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)]) |fi, c| {
            i += 1;
            logger.info("{s} {d: >2}/{d}: 0x{x:0>2} {d: >3} {s} {s}.{s} {d}/{d}", .{
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
    opcode: u8, // TODO: Opcode,
    length: u32,
};

const ErrorBody = packed struct {
    code: ErrorCode,
    length: u16,
    // TONDO: message: []const u8,
};

// TODO: consier using io.Reader.readStructEndian?
fn fromBytes(
    comptime T: type,
    comptime target_endian: std.builtin.Endian,
    buf: []const u8,
    self: *T,
) void {
    switch (comptime builtin.target.cpu.arch.endian()) {
        target_endian => self.* = std.mem.bytesAsValue(T, buf).*,
        else => {
            inline for (std.meta.fields(T)) |f| {
                // set each field
                @field(self, f.name) = @byteSwap(
                    std.mem.bytesAsValue(
                        f.type,
                        buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)], // TODO: if another struct is nested, @sizeOf includes padding, so we need to calculate it manually
                    ).*,
                );
            }
            prettyStructBytes(T, self, std.log, "fromBytes");
        },
    }
}

fn sizeOfNoPadding(comptime T: type) @TypeOf(@sizeOf(T)) {
    var size: usize = 0;
    inline for (std.meta.fields(T)) |f| {
        size += @sizeOf(f.type);
    }
    return size;
}

fn toBytes(
    comptime T: type,
    comptime struct_endian: std.builtin.Endian,
    self: *const T,
) [sizeOfNoPadding(T)]u8 {
    // In CQL, frame is big-endian (network byte order) https://github.com/apache/cassandra/blob/5d4bcc797af/doc/native_protocol_v5.spec#L232
    // So, we need to convert it to little-endian on little-endian machines

    // The comptime switch is used to avoid the runtime overhead of checking the endianness of the machine
    // You can verify that the other branch is not analysed by adding a @compileError
    switch (comptime builtin.target.cpu.arch.endian()) {
        // Note that this is toBytes, not asBytes, because we want to return an array
        // TODO: we should not return the padding bytes
        struct_endian => return std.mem.toBytes(self)[0..sizeOfNoPadding(T)].*,
        else => {
            // prettyStructBytes(T, self, std.log, "toBytes");
            var buf: [sizeOfNoPadding(T)]u8 = undefined;
            inline for (std.meta.fields(T)) |f| {
                copyReverse(
                    u8,
                    buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)],
                    std.mem.asBytes(&@field(self, f.name)),
                );
            }
            prettyBytesWithAnnotatedStruct(T, buf, std.log, "toBytes");
            // prettyBytesWithAnnotatedStruct(T, std.mem.toBytes(self), std.log, "toBytesDEBUG");
            return buf;
        },
    }
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

const CqlServer = struct {
    net_server: net.Server,
    state_machine: StateMachine,

    fn newServer(port: u16) !CqlServer {
        const address = try net.Address.parseIp("127.0.0.1", port);
        std.log.info("Address: {}", .{address});
        const s = try address.listen(.{ .reuse_address = true });

        return .{
            .net_server = s,
            .state_machine = StateMachine{},
        };
    }

    fn deinit(self: *@This()) void {
        self.net_server.deinit();
    }

    fn acceptClient(self: *@This()) !void {
        var client = try self.net_server.accept();
        defer client.stream.close();

        std.log.info("client connected: {any}", .{client.address});
        defer std.log.info("client disconnected: {any}", .{client.address});

        var total_bytes_count: usize = 0;
        defer std.log.info("total bytes: {d}", .{total_bytes_count});

        var buf: [@sizeOf(FrameHeader)]u8 = undefined;
        while (true) {
            const n = try client.stream.reader().read(&buf);
            if (n == 0) return;
            defer total_bytes_count += n;

            // TODO: maybe we should use readStructEndian instead of fromBytes?
            // const req_frame = try client.stream.reader().readStructEndian(FrameHeader, std.builtin.Endian.big);

            var req_frame: FrameHeader = undefined;
            fromBytes(
                FrameHeader,
                std.builtin.Endian.big,
                buf[0..@sizeOf(FrameHeader)],
                &req_frame,
            );
            std.log.info("received frame: {any}", .{req_frame});

            // for (1.., buf[0..n]) |i, c| {
            //     std.log.info("read byte {d: >2}/{d}: 0x{x:0>2} {d: >3} {s}", .{ i, buf.len, c, c, prettyByte(c) });
            // }

            const message = "Invalid or unsupported protocol version (?); the lowest supported version is 3 and the greatest is 4"; // TODO: replace ? with req_frame.version

            const body_len = @sizeOf(ErrorBody) + message.len; // TODO: sizeOf includes padding, so we need to calculate it manually
            const resp_frame = FrameHeader{
                .version = SupportedCqlVersion | ResponseFlag,
                .flags = 0x00,
                .stream = 0,
                .opcode = 0x00, // Opcode.Error,
                .length = body_len,
            };
            const error_body = ErrorBody{
                .code = ErrorCode.PROTOCOL_ERROR,
                .length = message.len,
                // .message = message,
            };

            try client.stream.writer().writeAll(
                toBytes(
                    FrameHeader,
                    std.builtin.Endian.big,
                    &resp_frame,
                )[0..],
            );
            try client.stream.writer().writeAll(
                toBytes(
                    ErrorBody,
                    std.builtin.Endian.big,
                    &error_body,
                )[0..],
            );
            try client.stream.writer().writeAll(message);
            // 84000000000000006b0000000a0065496e76616c6964206f7220756e737570706f727465642070726f746f636f6c2076657273696f6e20283636293b20746865206c6f7765737420737570706f727465642076657273696f6e206973203320616e64207468652067726561746573742069732034
        }
    }
};

pub fn main() !void {
    const trace = tracy.trace(@src());
    defer trace.end();

    var srv = try CqlServer.newServer(9042);
    defer srv.deinit();

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

    var buf = std.ArrayList(u8).init(inner_allocator);
    defer buf.deinit();

    try srv.acceptClient();
}

test "let's see how struct bytes work" {
    std.testing.log_level = std.log.Level.info;
    const frame1 = FrameHeader{
        .version = 1,
        .flags = 2,
        .stream = 3,
        .opcode = 4, // TODO: Opcode.AuthSuccess,
        .length = 5,
    };
    const buf = toBytes(
        FrameHeader,
        std.builtin.Endian.big,
        &frame1,
    );
    for (1.., buf) |i, c| {
        std.log.info("frame1 byte {d: >2}/{d}: 0x{x:0>2} {d: >3} {s}", .{ i, buf.len, c, c, prettyByte(c) });
    }

    try std.testing.expectEqual(1, buf[0]);
    try std.testing.expectEqual(2, buf[1]);
    try std.testing.expectEqual(0, buf[2]);
    try std.testing.expectEqual(3, buf[3]);
    try std.testing.expectEqual(4, buf[4]); // 0x10, buf[4]);
    try std.testing.expectEqual(0, buf[5]);
    try std.testing.expectEqual(0, buf[6]);
    try std.testing.expectEqual(0, buf[7]);
    try std.testing.expectEqual(5, buf[8]);

    var frame2 = FrameHeader{
        .version = 0,
        .flags = 0,
        .stream = 0,
        .opcode = 4, // TODO: Opcode.Error,
        .length = 0,
    };
    fromBytes(
        FrameHeader,
        std.builtin.Endian.big,
        buf[0..],
        &frame2,
    );
    std.log.info("frame2: {any}", .{frame2});
    try std.testing.expectEqual(frame1, frame2);
}

test "test initial cql handshake" {
    std.testing.log_level = std.log.Level.info;

    var srv = try CqlServer.newServer(0);
    defer srv.deinit();

    const TestCqlClient = struct {
        fn send(server_address: net.Address) !void {
            const socket = try net.tcpConnectToAddress(server_address);
            defer socket.close();

            const frame1 = FrameHeader{
                .version = 0x42,
                .flags = 0,
                .stream = 0,
                .opcode = 0x05,
                .length = 0,
            };
            _ = try socket.writer().writeAll(
                toBytes(
                    FrameHeader,
                    std.builtin.Endian.big,
                    &frame1,
                )[0..],
            );
            std.log.info("reading resonse 1", .{});
            const got = try socket.reader().readStructEndian(FrameHeader, std.builtin.Endian.big);
            std.log.info("got: {any}", .{got});

            std.log.info("reading resonse 2", .{});
            const got2 = try socket.reader().readStructEndian(ErrorBody, std.builtin.Endian.big);
            std.log.info("got2: {any}", .{got2});

            std.log.info("reading resonse 3", .{});
            var message: [100]u8 = undefined;
            const n = try socket.reader().read(&message);
            std.log.info("got3: {s}", .{message[0..n]});
        }
    };

    const t = try std.Thread.spawn(.{}, TestCqlClient.send, .{srv.net_server.listen_address});
    defer t.join();

    try srv.acceptClient();
}
