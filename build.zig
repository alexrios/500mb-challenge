const std = @import("std");

pub fn build(b: *std.Build) void {
    // Default to native; override with -Dtarget=aarch64-linux-musl for the Pi image.
    const target = b.standardTargetOptions(.{});

    // Default to ReleaseFast: this is a throughput/latency benchmark. The custom
    // tasks pin this explicitly; left as an option for `zig build -Doptimize=...`.
    const optimize = b.standardOptimizeOption(.{ .preferred_optimize_mode = .ReleaseFast });

    const exe = b.addExecutable(.{
        .name = "telemetry",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
            // No libc: we go straight to the Linux syscall layer. Smallest RSS,
            // fully static, no dynamic loader.
            .single_threaded = true,
            .strip = true,
        }),
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run the server");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
