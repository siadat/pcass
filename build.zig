const std = @import("std");

pub fn build(b: *std.Build) void {
    const exe = b.addExecutable(.{
        .name = "server",
        .root_source_file = .{ .path = "server.zig" },
        .target = b.host,
    });

    b.installArtifact(exe);
}
