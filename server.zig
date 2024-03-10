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

pub fn main() !void {
    const trace = tracy.trace(@src());
    defer trace.end();

    const address = try net.Address.parseIp("127.0.0.1", 8080);
    std.log.info("Address: {}", .{address});

    var server = try address.listen(.{ .reuse_address = true });

    var client = try server.accept();
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
