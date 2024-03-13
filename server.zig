const std = @import("std");
const net = std.net;
const tracy = @import("tracy.zig");
const builtin = @import("builtin");

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
            0,
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
    opcode: u8,
    length: u32,
    // comptime {
    //     @compileLog(@sizeOf(FrameHeader));
    //     // std.debug.assert(@sizeOf(FrameHeader) == 12);
    // }
};

fn fromBytes(
    comptime T: type,
    comptime target_endian: std.builtin.Endian,
    self: *T,
    buf: [@sizeOf(T)]u8,
) void {
    switch (builtin.target.cpu.arch.endian()) {
        target_endian => self.* = std.mem.bytesAsValue(T, &buf).*,
        else => {
            inline for (std.meta.fields(T)) |f| {
                // set each field
                @field(self, f.name) = @byteSwap(
                    std.mem.bytesAsValue(
                        f.type,
                        buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)],
                    ).*,
                );
            }
        },
    }
}

fn asBytes(
    comptime T: type,
    self: T,
) [@sizeOf(T)]u8 {
    // In CQL, frame is big-endian (network byte order) https://github.com/apache/cassandra/blob/5d4bcc797af/doc/native_protocol_v5.spec#L232
    // So, we need to convert it to little-endian on little-endian machines
    switch (builtin.target.cpu.arch.endian()) { // TODO: this is known at compile time, so we can use comptime
        .big => return std.mem.toBytes(self),
        .little => {
            var buf: [@sizeOf(T)]u8 = undefined;
            inline for (std.meta.fields(T)) |f| {
                std.mem.copyForwards(
                    u8,
                    buf[@offsetOf(T, f.name) .. @offsetOf(T, f.name) + @sizeOf(f.type)],
                    std.mem.asBytes(&@byteSwap(@field(self, f.name))),
                );
            }
            return buf;
        },
    }
}

const Server = struct {
    net_server: net.Server,
    state_machine: StateMachine,

    fn newServer(port: u16) !Server {
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

    fn accept(self: *@This()) !usize {
        var client = try self.net_server.accept();
        defer client.stream.close();

        std.log.info("client connected: {any}", .{client.address});
        defer std.log.info("client disconnected: {any}", .{client.address});

        var total_bytes_count: usize = 0;
        defer std.log.info("total bytes: {d}", .{total_bytes_count});

        var buf: [16]u8 = undefined;
        while (true) {
            const n = try client.stream.reader().read(&buf);
            if (n == 0) return total_bytes_count;
            defer total_bytes_count += n;

            const frame = std.mem.bytesAsValue(FrameHeader, buf[0..@sizeOf(FrameHeader)]);
            std.log.info("received byte: {any}", .{frame});

            for (1.., buf[0..n]) |i, c| {
                std.log.info("read byte {d: >2}/{d}: 0x{x:0>2} {d: >3} {s}", .{ i, buf.len, c, c, prettyByte(c) });
            }
        }
        return total_bytes_count;
    }
};

pub fn main() !void {
    const trace = tracy.trace(@src());
    defer trace.end();

    var srv = try Server.newServer(9042);
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

    _ = try srv.accept();
}

test "let's see how struct bytes work" {
    std.testing.log_level = std.log.Level.info;
    const frame1 = FrameHeader{
        .version = 1,
        .flags = 2,
        .stream = 3,
        .opcode = 4,
        .length = 5,
    };
    const buf = asBytes(FrameHeader, frame1);
    for (1.., buf) |i, c| {
        std.log.info("frame1 byte {d: >2}/{d}: 0x{x:0>2} {d: >3} {s}", .{ i, buf.len, c, c, prettyByte(c) });
    }

    try std.testing.expectEqual(1, buf[0]);
    try std.testing.expectEqual(2, buf[1]);
    try std.testing.expectEqual(0, buf[2]);
    try std.testing.expectEqual(3, buf[3]);
    try std.testing.expectEqual(4, buf[4]);
    try std.testing.expectEqual(0, buf[5]);
    try std.testing.expectEqual(0, buf[6]);
    try std.testing.expectEqual(0, buf[7]);
    try std.testing.expectEqual(5, buf[8]);

    var frame2 = FrameHeader{
        .version = 0,
        .flags = 0,
        .stream = 0,
        .opcode = 0,
        .length = 0,
    };
    fromBytes(FrameHeader, std.builtin.Endian.big, &frame2, buf);
    std.log.info("frame2: {any}", .{frame2});
    try std.testing.expectEqual(frame1, frame2);
}

test "test server" {
    std.testing.log_level = std.log.Level.info;

    var srv = try Server.newServer(0);
    defer srv.deinit();
    const want = "Hello world!";

    const TestClient = struct {
        fn send(server_address: net.Address) !void {
            const socket = try net.tcpConnectToAddress(server_address);
            defer socket.close();
            _ = try socket.writer().writeAll(want);
        }
    };

    const t = try std.Thread.spawn(.{}, TestClient.send, .{srv.net_server.listen_address});
    defer t.join();

    const got = try srv.accept();
    try std.testing.expectEqual(@as(usize, want.len), got);
}
