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
    const astgen_frame = tracy.namedFrame("astgen");
    defer astgen_frame.end();

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

    while (true) {
        var buf: [3]u8 = undefined;
        const n = try client.stream.reader().read(&buf);
        if (n == 0) {
            break;
        }
        var s = std.ArrayList(u8).init(gpa.allocator());
        defer s.deinit();
        try escape(buf[0..n], &s);
        std.log.info("read {d} bytes: \"{s}\"", .{ n, s.items });
    }
    std.log.info("client disconnected: {any}", .{client.address});
}
