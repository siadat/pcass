const std = @import("std");
const net = std.net;
const tracy = @import("tracy.zig");

// TODO: use a writer as in stringEscape in escaper.zig,
// because the escaped string is only used for loggingin
pub fn escapeString(input: []u8, ret: *std.ArrayList(u8)) !void {
    try ret.append('"');
    for (input) |c| {
        switch (c) {
            '"', '\\' => {
                try ret.append('\\');
                try ret.append(c);
            },
            '\n' => {
                try ret.append('\\');
                try ret.append('n');
            },
            else => try ret.append(c),
        }
    }
    try ret.append('"'); // TODO: I want to run this in a defer, however, 'try' is not allowed in a defer
}

pub fn prettyByte(
    byte: u8,
) [4]u8 {
    switch (byte) {
        '\n' => return [_]u8{ '\'', '\\', 'n', '\'' },
        '\r' => return [_]u8{ '\'', '\\', 'r', '\'' },
        '\t' => return [_]u8{ '\'', '\\', 't', '\'' },
        '\'' => return [_]u8{ '\'', '\\', '\'', '\'' },
        // 39 is the ASCII code for the single quote, which is already covered above
        32...38, 40...126 => return [_]u8{
            '\'',
            byte,
            '\'',
            0,
        },
        else => return [_]u8{ '-', '-', '-', '-' },
    }
}

const Server = struct {
    net_server: net.Server,

    fn newServer(port: u16) !Server {
        const address = try net.Address.parseIp("127.0.0.1", port);
        std.log.info("Address: {}", .{address});
        const s = try address.listen(.{ .reuse_address = true });

        return .{
            .net_server = s,
        };
    }

    fn deinit(self: *@This()) void {
        self.net_server.deinit();
    }

    fn accept(self: *@This(), ret: *std.ArrayList(u8)) !void {
        var client = try self.net_server.accept();
        defer client.stream.close();

        std.log.info("client connected: {any}", .{client.address});
        defer std.log.info("client disconnected: {any}", .{client.address});

        var buf: [16]u8 = undefined;
        while (true) {
            const n = try client.stream.reader().read(&buf);
            if (n == 0) return;
            for (buf[0..n]) |c| {
                std.log.info("read byte: 0x{x:0>2} {d: >3} {s}", .{ c, c, prettyByte(c) });
            }
            try ret.appendSlice(buf[0..n]);
        }
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
    defer std.log.info("total bytes: {d}", .{buf.items.len});

    try srv.accept(&buf);

    var escaped_buf = std.ArrayList(u8).init(inner_allocator);
    defer escaped_buf.deinit();

    _ = try escapeString(buf.items, &escaped_buf);
    std.log.info("Client sent: {s}", .{escaped_buf.items});
}

test "test server" {
    const allocator = std.testing.allocator;
    std.testing.log_level = std.log.Level.info;

    var srv = try Server.newServer(0);
    defer srv.deinit();

    const TestClient = struct {
        fn send(server_address: net.Address) !void {
            const socket = try net.tcpConnectToAddress(server_address);
            defer socket.close();
            _ = try socket.writer().writeAll("Hello world!");
        }
    };

    const t = try std.Thread.spawn(.{}, TestClient.send, .{srv.net_server.listen_address});
    defer t.join();

    var ret = std.ArrayList(u8).init(allocator);
    defer ret.deinit();

    try srv.accept(&ret); // this needs to be a pointer, because append
    const want = "Hello world!";
    try std.testing.expectEqual(@as(usize, want.len), ret.items.len);
    try std.testing.expectEqualSlices(u8, want, ret.items);
}
