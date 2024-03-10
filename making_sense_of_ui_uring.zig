const std = @import("std");
const mem = std.mem;
const net = std.net;
const os = std.os;
const linux = os.linux;
const assert = std.debug.assert;

pub fn main() !void {
    const entries: u12 = 16; // TODO: u12 or u13?
    const flags: u32 = 0;
    var ring = linux.IO_Uring.init(entries, flags) catch |err| switch (err) {
        error.SystemOutdated => return error.SkipZigTest,
        error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
    defer ring.deinit();

    // std.os.linux.io_uring_prep_recv(sqe, op.socket, op.buffer, os.MSG.NOSIGNAL);
    // std.os.linux.recv(sqe, op.socket, op.buffer, os.MSG.NOSIGNAL);
    // var buffer_recv: []u8 = undefined;
    // _ = try ring.recv(0xffffffff, socket_test_harness.server, .{ .buffer = buffer_recv }, 0);

    const port = 5000; // 0
    var address = try net.Address.parseIp4("127.0.0.1", port);
    const kernel_backlog = 1;
    const listener_socket = try os.socket(
        address.any.family,
        os.SOCK.STREAM | os.SOCK.CLOEXEC,
        0,
    );
    errdefer os.closeSocket(listener_socket);

    try os.setsockopt(
        listener_socket,
        os.SOL.SOCKET,
        os.SO.REUSEADDR,
        &mem.toBytes(@as(c_int, 1)),
    );
    try os.bind(listener_socket, &address.any, address.getOsSockLen());
    try os.listen(listener_socket, kernel_backlog);

    // set address to the OS-chosen IP/port.
    var slen: os.socklen_t = address.getOsSockLen();
    try os.getsockname(listener_socket, &address.any, &slen);

    // Submit 1 accept
    var accept_addr: os.sockaddr = undefined;
    var accept_addr_len: os.socklen_t = @sizeOf(@TypeOf(accept_addr));
    _ = try ring.accept(
        0xaaaaaaaa,
        listener_socket,
        &accept_addr,
        &accept_addr_len,
        0,
    );

    defer os.closeSocket(listener_socket);

    var buffer_recv = [_]u8{ 0, 1, 0, 1, 0 };

    const sqe_recv = try ring.recv(0xffffffff, listener_socket, .{
        .buffer = buffer_recv[0..],
    }, 0);
    sqe_recv.flags |= linux.IOSQE_IO_LINK;

    // const ts = os.linux.kernel_timespec{ .tv_sec = 0, .tv_nsec = 1000000 };
    // _ = try ring.link_timeout(0x22222222, &ts, 0);

    const nr_wait = try ring.submit();
    assert(nr_wait == 2);
    // try testing.expectEqual(@as(u32, 2), nr_wait);

    var i: usize = 0;
    while (i < nr_wait) : (i += 1) {
        const cqe = try ring.copy_cqe();
        switch (cqe.user_data) {
            0xffffffff, 0xaaaaaaaa => {
                if (cqe.res != -@as(i32, @intFromEnum(linux.E.INTR)) and
                    cqe.res != -@as(i32, @intFromEnum(linux.E.CANCELED)))
                {
                    std.debug.print("Req 0x{x} got {d}\n", .{
                        cqe.user_data,
                        cqe.res,
                    });
                    // try testing.expect(false);
                } else {
                    std.log.info("Got expected", .{});
                }
            },
            // 0x22222222 => {
            //     if (cqe.res != -@as(i32, @intFromEnum(linux.E.ALREADY)) and
            //         cqe.res != -@as(i32, @intFromEnum(linux.E.TIME)))
            //     {
            //         std.debug.print("Req 0x{x} got {d}\n", .{ cqe.user_data, cqe.res });
            //         // try testing.expect(false);
            //     }
            // },
            else => {
                if (cqe.res != -@as(i32, @intFromEnum(linux.E.INTR)) and
                    cqe.res != -@as(i32, @intFromEnum(linux.E.CANCELED)))
                {
                    std.debug.print("--Req 0x{x} got {d}\n", .{ cqe.user_data, cqe.res });
                    // try testing.expect(false);
                } else {
                    std.log.info("--Got expected", .{});
                }
            },
        }
    }

    std.log.info("Done", .{});
}
