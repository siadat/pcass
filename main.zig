const std = @import("std");

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    // Open the file for reading
    const file = try std.fs.cwd().openFile("test.lisp", .{});
    defer file.close();

    // Create a buffer for reading data
    var buffer: [4096]u8 = undefined;

    // Read from the file
    while (true) {
        const bytesRead = try file.reader().read(buffer[0..]);
        if (bytesRead == 0) break; // End of file reached

        // Process or print the read bytes
        try stdout.writeAll(buffer[0..bytesRead]);
    }
}
