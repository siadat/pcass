const std = @import("std");

/// Copied from https://github.com/ziglang/zig/blob/804cee3b93/lib/std/zig/fmt.zig#L50-L50
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
