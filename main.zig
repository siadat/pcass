const std = @import("std");

/// Sina: copied from https://github.com/ziglang/zig/blob/804cee3b93cb7084c16ee61d3bcb57f7d3c9f0bc/lib/std/zig/fmt.zig#L50-L50
/// Print the string as escaped contents of a double quoted or single-quoted string.
/// Format `{}` treats contents as a double-quoted string.
/// Format `{'}` treats contents as a single-quoted string.
pub fn stringEscape(
    bytes: []const u8,
    comptime fmt: []const u8,
    options: std.fmt.FormatOptions,
    writer: anytype,
) !void {
    _ = options;
    for (bytes) |byte| switch (byte) {
        '\n' => try writer.writeAll("\\n"),
        '\r' => try writer.writeAll("\\r"),
        '\t' => try writer.writeAll("\\t"),
        '\\' => try writer.writeAll("\\\\"),
        '"' => {
            if (fmt.len == 1 and fmt[0] == '\'') {
                try writer.writeByte('"');
            } else if (fmt.len == 0) {
                try writer.writeAll("\\\"");
            } else {
                @compileError("expected {} or {'}, found {" ++ fmt ++ "}");
            }
        },
        '\'' => {
            if (fmt.len == 1 and fmt[0] == '\'') {
                try writer.writeAll("\\'");
            } else if (fmt.len == 0) {
                try writer.writeByte('\'');
            } else {
                @compileError("expected {} or {'}, found {" ++ fmt ++ "}");
            }
        },
        ' ', '!', '#'...'&', '('...'[', ']'...'~' => try writer.writeByte(byte),
        // Use hex escapes for rest any unprintable characters.
        else => {
            try writer.writeAll("\\x");
            try std.fmt.formatInt(byte, 16, .lower, .{ .width = 2, .fill = '0' }, writer);
        },
    };
}

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
        try stringEscape(buffer[0..], "'", .{}, stdout);
        try stdout.print("'", .{});
        try stdout.print("\n", .{});
    }
}
