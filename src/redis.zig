//! Minimal RESP2 client helpers: low-level command builders that append to a
//! `Buf` (so commands pipeline naturally) and reply scanners for the shapes we
//! issue.
//!
//! Storage model: one sorted set per device, keyed `z:<id>`, scored by the
//! telemetry timestamp. Members are the packed record followed by a unique
//! 8-byte sequence (so identical samples never collapse). Unlike streams, a
//! ZSET tolerates out-of-order timestamps — telemetry is not monotonic.

const std = @import("std");
const Buf = @import("util.zig").Buf;

// ---------------------------------------------------------------------------
// Low-level RESP builders (public so handlers compose commands directly)
// ---------------------------------------------------------------------------

pub fn arrayHeader(out: *Buf, n: usize) !void {
    try out.appendByte('*');
    try out.appendInt(@intCast(n));
    try out.appendSlice("\r\n");
}

pub fn bulk(out: *Buf, s: []const u8) !void {
    try out.appendByte('$');
    try out.appendInt(@intCast(s.len));
    try out.appendSlice("\r\n");
    try out.appendSlice(s);
    try out.appendSlice("\r\n");
}

pub fn bulkInt(out: *Buf, v: i64) !void {
    var tmp: [24]u8 = undefined;
    const s = std.fmt.bufPrint(&tmp, "{d}", .{v}) catch unreachable;
    try bulk(out, s);
}

pub fn ping(out: *Buf) !void {
    try arrayHeader(out, 1);
    try bulk(out, "PING");
}

// ---------------------------------------------------------------------------
// Reply scanning
// ---------------------------------------------------------------------------

fn findCrlf(buf: []const u8, start: usize) ?usize {
    var i = start;
    while (i + 1 < buf.len) : (i += 1) {
        if (buf[i] == '\r' and buf[i + 1] == '\n') return i;
    }
    return null;
}

fn parseSignedLine(buf: []const u8, start: usize) ?struct { val: i64, next: usize } {
    const crlf = findCrlf(buf, start) orelse return null;
    const v = std.fmt.parseInt(i64, buf[start..crlf], 10) catch return null;
    return .{ .val = v, .next = crlf + 2 };
}

/// Total byte length of one complete reply at `buf[0]`, or null if incomplete.
pub fn replyLen(buf: []const u8) ?usize {
    return scan(buf, 0);
}

fn scan(buf: []const u8, at: usize) ?usize {
    if (at >= buf.len) return null;
    switch (buf[at]) {
        '+', '-', ':' => {
            const crlf = findCrlf(buf, at + 1) orelse return null;
            return crlf + 2 - at;
        },
        '$' => {
            const hdr = parseSignedLine(buf, at + 1) orelse return null;
            if (hdr.val < 0) return hdr.next - at;
            const total = (hdr.next - at) + @as(usize, @intCast(hdr.val)) + 2;
            if (at + total > buf.len) return null;
            return total;
        },
        '*' => {
            const hdr = parseSignedLine(buf, at + 1) orelse return null;
            var pos = hdr.next;
            if (hdr.val < 0) return pos - at;
            var k: i64 = 0;
            while (k < hdr.val) : (k += 1) {
                const sub = scan(buf, pos) orelse return null;
                pos += sub;
            }
            return pos - at;
        },
        else => return null,
    }
}

pub const ReplyKind = enum { ok, err };
pub fn ackKind(buf: []const u8) ReplyKind {
    if (buf.len == 0) return .err;
    return if (buf[0] == '-') .err else .ok;
}

/// Parse a `:<int>\r\n` integer reply (e.g. ZADD count). 0 on anything else.
pub fn parseInteger(buf: []const u8) i64 {
    if (buf.len == 0 or buf[0] != ':') return 0;
    const hdr = parseSignedLine(buf, 1) orelse return 0;
    return hdr.val;
}

/// Iterates the bulk-string elements of a flat array reply (ZRANGEBYSCORE /
/// ZREVRANGE without scores). Yields each member's raw bytes.
pub const Members = struct {
    buf: []const u8,
    pos: usize,
    remaining: i64,

    pub fn init(reply: []const u8) Members {
        if (reply.len == 0 or reply[0] != '*') {
            return .{ .buf = reply, .pos = reply.len, .remaining = 0 };
        }
        const hdr = parseSignedLine(reply, 1) orelse return .{ .buf = reply, .pos = reply.len, .remaining = 0 };
        const n = if (hdr.val < 0) 0 else hdr.val;
        return .{ .buf = reply, .pos = hdr.next, .remaining = n };
    }

    pub fn count(self: *const Members) usize {
        return @intCast(self.remaining);
    }

    pub fn next(self: *Members) ?[]const u8 {
        if (self.remaining <= 0) return null;
        self.remaining -= 1;
        if (self.pos >= self.buf.len or self.buf[self.pos] != '$') return null;
        const hdr = parseSignedLine(self.buf, self.pos + 1) orelse return null;
        if (hdr.val < 0) {
            self.pos = hdr.next;
            return &[_]u8{};
        }
        const n: usize = @intCast(hdr.val);
        const start = hdr.next;
        const end = start + n;
        if (end + 2 > self.buf.len) return null;
        self.pos = end + 2;
        return self.buf[start..end];
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "replyLen simple and bulk" {
    try std.testing.expectEqual(@as(?usize, 5), replyLen("+OK\r\n"));
    try std.testing.expectEqual(@as(?usize, 11), replyLen("$5\r\nhello\r\n"));
    try std.testing.expectEqual(@as(?usize, null), replyLen("$5\r\nhel"));
    try std.testing.expectEqual(@as(?usize, 5), replyLen(":12\r\n"));
}

test "parseInteger" {
    try std.testing.expectEqual(@as(i64, 50), parseInteger(":50\r\n"));
    try std.testing.expectEqual(@as(i64, 0), parseInteger("+OK\r\n"));
}

test "members iterate" {
    const r = "*2\r\n$2\r\nAB\r\n$2\r\nCD\r\n";
    try std.testing.expectEqual(@as(?usize, r.len), replyLen(r));
    var it = Members.init(r);
    try std.testing.expectEqual(@as(usize, 2), it.count());
    try std.testing.expectEqualStrings("AB", it.next().?);
    try std.testing.expectEqualStrings("CD", it.next().?);
    try std.testing.expect(it.next() == null);
}

test "empty array" {
    var it = Members.init("*0\r\n");
    try std.testing.expectEqual(@as(usize, 0), it.count());
    try std.testing.expect(it.next() == null);
}
