const std = @import("std");
const escaper = @import("escaper.zig");

pub fn open() !std.fs.File {
    const file = try std.fs.cwd().openFile("test.lisp", .{});
    return file;
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // Open the file for reading
    const file = try open();
    defer file.close();

    // Create a buffer for a single byte
    var buffer: [1]u8 = undefined;

    // Read from the file, one byte at a time
    while (true) {
        const bytesRead = try file.reader().read(buffer[0..]);
        if (bytesRead == 0) break; // End of file reached

        // Process or print the read byte
        try stdout.print("0x{x:0>2} ", .{buffer[0]});
        try stdout.print("{d: >3} ", .{buffer[0]});
        try stdout.print("'", .{});
        try escaper.stringEscape(buffer[0..], "'", .{}, stdout);
        try stdout.print("'", .{});
        try stdout.print("\n", .{});
    }
}
