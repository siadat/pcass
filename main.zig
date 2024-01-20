const std = @import("std");
const escaper = @import("escaper.zig");

pub fn open() !std.fs.File {
    const file = try std.fs.cwd().openFile("test.lisp", .{});
    return file;
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    const file = try open();
    defer file.close();

    var buffer: [1]u8 = undefined;

    while (true) {
        const bytesRead = try file.reader().read(buffer[0..]);
        if (bytesRead == 0) break; // End of file reached

        try stdout.print("0x{x:0>2} ", .{buffer[0]});
        try stdout.print("{d: >3} ", .{buffer[0]});
        try stdout.print("'", .{});
        try escaper.stringEscape(buffer[0..], "'", .{}, stdout);
        try stdout.print("'", .{});
        try stdout.print("\n", .{});
    }
}
