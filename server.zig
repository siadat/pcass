const std = @import("std");
const net = std.net;
const tracy = @import("tracy.zig");

pub fn escape(input: []u8, ret: *std.ArrayList(u8)) !void {
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
}

const Server = struct {
    net_server: net.Server = undefined,

    fn newServer() !Server {
        const port = 0;
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
        var client = try self.net_server.accept(); // TODO: why does this work even though accept requires a pointer and net_server is not a pointer, only server (parent) is a pointer
        defer client.stream.close();

        var buf: [5]u8 = undefined;
        while (true) {
            const n = try client.stream.reader().read(&buf);
            if (n == 0) return;
            std.log.info("read {d} bytes: \"{s}\"", .{ n, buf[0..n] });
            try ret.appendSlice(buf[0..n]);
        }
    }
};

pub fn main() !void {
    const trace = tracy.trace(@src());
    defer trace.end();

    var srv = try Server.newServer();
    defer srv.deinit();

    var client = try srv.net_server.accept();
    defer client.stream.close();
    std.log.info("client connected: {any}", .{client.address});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) {
            std.log.err("There is memory leak\n", .{});
        }
    }

    // copied from https://sourcegraph.com/github.com/zigtools/zls@dd307c59bf32e2cec323235c776e07fa36efb465/-/blob/src/main.zig?L235-236
    var allocator_state = std.heap.GeneralPurposeAllocator(.{ .stack_trace_frames = 10 }){};
    var tracy_state = if (tracy.enable_allocation) tracy.tracyAllocator(allocator_state.allocator()) else void{};
    const inner_allocator: std.mem.Allocator = if (tracy.enable_allocation) tracy_state.allocator() else allocator_state.allocator();

    var byte_count: u64 = 0;
    defer std.log.info("total bytes: {d}", .{byte_count});
    while (true) {
        var buf: [3]u8 = undefined;
        const n = try client.stream.reader().read(&buf);
        byte_count += n;
        if (n == 0) {
            break;
        }
        var s = std.ArrayList(u8).init(inner_allocator);
        defer s.deinit();
        try escape(buf[0..n], &s);
        std.log.info("read {d} bytes: \"{s}\"", .{ n, s.items });
    }
    std.log.info("client disconnected: {any}", .{client.address});
}

test "test server" {
    const allocator = std.testing.allocator;
    std.testing.log_level = std.log.Level.info;

    const srv = try allocator.create(Server); // create in heap and return pointer
    defer allocator.destroy(srv); // removing this will leak memory

    srv.* = try Server.newServer();
    defer srv.deinit(); // removing this does not memory leak, because this is just setting the field values to 'undefined'

    const S = struct {
        fn clientFn(server_address: net.Address) !void {
            const socket = try net.tcpConnectToAddress(server_address);
            defer socket.close();

            _ = try socket.writer().writeAll("Hello world!");
        }
    };

    const t = try std.Thread.spawn(.{}, S.clientFn, .{srv.net_server.listen_address});
    defer t.join(); // TODO: is there any way to assert whether that thread is completed before the main process exits? I mean, if I forget to free allocated memory it will be detectable as a memory leak, but this is not detectable.

    var ret = std.ArrayList(u8).init(allocator);
    defer ret.deinit();

    try srv.accept(&ret); // this needs to be a pointer, because append
    const want = "Hello world!";
    try std.testing.expectEqual(@as(usize, want.len), ret.items.len);
    try std.testing.expectEqualSlices(u8, want, ret.items);
}
