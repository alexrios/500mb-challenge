//! Thin wrappers over raw Linux syscalls. Zig 0.16 removed socket I/O from
//! std.posix (the Io migration), so we go straight to std.os.linux. No libc.

const std = @import("std");
const linux = std.os.linux;
const errno = linux.errno;

pub const epoll_event = linux.epoll_event;
pub const EPOLL = linux.EPOLL;
pub const SIG = linux.SIG;
pub const Sigaction = linux.Sigaction;

pub const IoRes = union(enum) { n: usize, again, closed };
pub const AcceptRes = union(enum) { fd: i32, again, err };
pub const ConnRes = enum { ok, inprogress, err };

pub fn tcpSocket() !i32 {
    const rc = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, 0);
    return switch (errno(rc)) {
        .SUCCESS => @intCast(rc),
        else => error.Socket,
    };
}

fn setInt(fd: i32, level: i32, optname: u32, val: i32) void {
    var v = val;
    _ = linux.setsockopt(fd, level, optname, std.mem.asBytes(&v), @sizeOf(i32));
}

pub fn setNoDelay(fd: i32) void {
    setInt(fd, linux.IPPROTO.TCP, linux.TCP.NODELAY, 1);
}

pub fn setReuseAddr(fd: i32) void {
    setInt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, 1);
}

pub const Addr = linux.sockaddr.in;

/// Build an IPv4 sockaddr. The kernel's `addr`/`port` fields are network byte
/// order; @bitCast of the octet array preserves the a.b.c.d memory layout.
pub fn ipv4(octets: [4]u8, port: u16) Addr {
    return .{
        .port = std.mem.nativeToBig(u16, port),
        .addr = @bitCast(octets),
    };
}

pub fn parseIp4(s: []const u8, port: u16) ?Addr {
    var octs: [4]u8 = undefined;
    var it = std.mem.splitScalar(u8, s, '.');
    var i: usize = 0;
    while (it.next()) |part| {
        if (i >= 4) return null;
        octs[i] = std.fmt.parseInt(u8, part, 10) catch return null;
        i += 1;
    }
    if (i != 4) return null;
    return ipv4(octs, port);
}

pub fn bind(fd: i32, addr: *const Addr) !void {
    if (errno(linux.bind(fd, @ptrCast(addr), @sizeOf(Addr))) != .SUCCESS) return error.Bind;
}

pub fn listen(fd: i32, backlog: u31) !void {
    if (errno(linux.listen(fd, backlog)) != .SUCCESS) return error.Listen;
}

pub fn accept(fd: i32) AcceptRes {
    const rc = linux.accept4(fd, null, null, linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC);
    return switch (errno(rc)) {
        .SUCCESS => .{ .fd = @intCast(rc) },
        .AGAIN, .INTR, .CONNABORTED => .again,
        else => .err,
    };
}

pub fn connect(fd: i32, addr: *const Addr) ConnRes {
    const rc = linux.connect(fd, @ptrCast(addr), @sizeOf(Addr));
    return switch (errno(rc)) {
        .SUCCESS => .ok,
        .INPROGRESS, .INTR => .inprogress,
        else => .err,
    };
}

pub fn recv(fd: i32, buf: []u8) IoRes {
    const rc = linux.recvfrom(fd, buf.ptr, buf.len, 0, null, null);
    return switch (errno(rc)) {
        .SUCCESS => if (rc == 0) .closed else .{ .n = rc },
        .AGAIN, .INTR => .again,
        else => .closed,
    };
}

pub fn send(fd: i32, buf: []const u8) IoRes {
    const MSG_NOSIGNAL: u32 = 0x4000;
    const rc = linux.sendto(fd, buf.ptr, buf.len, MSG_NOSIGNAL, null, 0);
    return switch (errno(rc)) {
        .SUCCESS => .{ .n = rc },
        .AGAIN, .INTR => .again,
        else => .closed,
    };
}

pub fn close(fd: i32) void {
    _ = linux.close(fd);
}

pub fn epollCreate() !i32 {
    const rc = linux.epoll_create1(linux.EPOLL.CLOEXEC);
    return switch (errno(rc)) {
        .SUCCESS => @intCast(rc),
        else => error.Epoll,
    };
}

pub fn epollAdd(epfd: i32, fd: i32, events: u32, tag: u64) void {
    var ev = epoll_event{ .events = events, .data = .{ .u64 = tag } };
    _ = linux.epoll_ctl(epfd, linux.EPOLL.CTL_ADD, fd, &ev);
}
pub fn epollMod(epfd: i32, fd: i32, events: u32, tag: u64) void {
    var ev = epoll_event{ .events = events, .data = .{ .u64 = tag } };
    _ = linux.epoll_ctl(epfd, linux.EPOLL.CTL_MOD, fd, &ev);
}
pub fn epollDel(epfd: i32, fd: i32) void {
    _ = linux.epoll_ctl(epfd, linux.EPOLL.CTL_DEL, fd, null);
}

pub fn epollWait(epfd: i32, events: []epoll_event, timeout_ms: i32) usize {
    const rc = linux.epoll_wait(epfd, events.ptr, @intCast(events.len), timeout_ms);
    return switch (errno(rc)) {
        .SUCCESS => rc,
        else => 0, // EINTR etc -> treat as no events; caller loops
    };
}

pub fn sigaction(sig: SIG, act: *const Sigaction) void {
    _ = linux.sigaction(sig, act, null);
}

pub fn emptySigset() linux.sigset_t {
    return linux.sigemptyset();
}

pub fn nowMs() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(linux.CLOCK.MONOTONIC, &ts);
    return @as(i64, ts.sec) * 1000 + @divTrunc(@as(i64, ts.nsec), 1_000_000);
}

// ---------------------------------------------------------------------------
// Environment (read once from /proc/self/environ; avoids std env churn / libc).
// ---------------------------------------------------------------------------

var env_buf: [32 * 1024]u8 = undefined;
var env_len: usize = 0;

pub fn loadEnv() void {
    const path: [*:0]const u8 = "/proc/self/environ";
    const fd_rc = linux.openat(linux.AT.FDCWD, path, .{ .ACCMODE = .RDONLY }, 0);
    if (errno(fd_rc) != .SUCCESS) return;
    const fd: i32 = @intCast(fd_rc);
    defer _ = linux.close(fd);
    while (env_len < env_buf.len) {
        const rc = linux.read(fd, env_buf[env_len..].ptr, env_buf.len - env_len);
        switch (errno(rc)) {
            .SUCCESS => {
                if (rc == 0) break;
                env_len += rc;
            },
            .INTR => continue,
            else => break,
        }
    }
}

pub fn getenv(key: []const u8) ?[]const u8 {
    var it = std.mem.splitScalar(u8, env_buf[0..env_len], 0);
    while (it.next()) |entry| {
        const eq = std.mem.indexOfScalar(u8, entry, '=') orelse continue;
        if (std.mem.eql(u8, entry[0..eq], key)) return entry[eq + 1 ..];
    }
    return null;
}
